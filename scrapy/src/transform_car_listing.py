import json
import pandas as pd
import os
import re
from datetime import datetime

RAW_FILE_PATH = os.path.join("data", "raw", "car_listing.jsonl")
TRANSFORMED_FILE_PATH = os.path.join("data", "transformed")
os.makedirs(TRANSFORMED_FILE_PATH, exist_ok=True)

def clean_text(text):
  return text.strip() if isinstance(text, str) else text

def extract_number(text):
  if text:
    numbers = re.findall(r'\d+', text.replace(',', ''))
    return int("".join(numbers)) if numbers else None
  return None

def extract_hp(text):
  if text:
    match = re.search(r'(\d+)\s*hp', text)
    return int(match.group(1)) if match else None
  return None

def convert_fuel_consumption(text):
  match = re.search(r'(\d+(\.\d+)?)', text)
  if match:
    liters_per_100km = float(match.group(1))
    return round(100 / liters_per_100km, 2) if liters_per_100km else None
  return None

def clean_seller_address(seller_address_1, seller_address_2, seller_type):
  if seller_type == "Private seller":
    if not isinstance(seller_address_1, str):
      return "", None, None  # Handle missing values

    # Match patterns like '5384Da Schaijk, NL'
    match = re.match(r"([\d\w\s-]+)\s+([A-Za-zÀ-ÿ'\-\s]+),?\s*NL", seller_address_1.strip(), re.IGNORECASE)

    if match:
      seller_zip_code = match.group(1).strip()
      seller_city = match.group(2).strip()
      return "", seller_zip_code, seller_city

    return "", None, None

  elif seller_type == "Dealer":
    if not isinstance(seller_address_1, str) or not isinstance(seller_address_2, str):
      return None, None, None  # Handle missing values

    # Extract ZIP code and city from seller_address_2 (format: "1234 AB City, NL")
    match = re.match(r"([\d\w\s-]+)\s+([A-Za-zÀ-ÿ'\-\s]+),?\s*NL", seller_address_2.strip(), re.IGNORECASE)

    if match:
      seller_zip_code = match.group(1).strip()  # Extract ZIP
      seller_city = match.group(2).strip()  # Extract city name
      return seller_address_1.strip(), seller_zip_code, seller_city

    return seller_address_1.strip(), None, None  # Default case when no match is found

  return seller_address_1, None, None  # Default case for other seller types

def extract_year_active_on_autoscout(text):
  if text:
    match = re.search(r'(\d{4})$', text)
    return int(match.group(1)) if match else None
  return None

### Apply transformations ###
def transform_data():
  with open(RAW_FILE_PATH, "r", encoding="utf-8") as file:
    data = [json.loads(line) for line in file]

  df = pd.DataFrame(data)

  # Call functions
  df["manufacturer"] = df["manufacturer"].apply(clean_text)
  df["price"] = df["price"].apply(extract_number)
  df["lease_price_per_month"] = df["lease_price_per_month"].apply(extract_number)
  df["km"] = df["km"].apply(extract_number)
  df["electric_range"] = df["electric_range"].apply(extract_number)
  df["engine_power_hp"] = df["engine_power"].apply(extract_hp)
  df["engine_size_cc"] = df["engine_size"].apply(extract_number)
  df["empty_weight_kg"] = df["empty_weight"].apply(extract_number)
  df["fuel_consumption_km_per_l"] = df["fuel_consumption"].apply(convert_fuel_consumption)
  df["co2_emission_g_per_km"] = df["co2_emission"].apply(extract_number)
  df["active_since"] = df["active_since"].apply(extract_year_active_on_autoscout)
  df["seller_address"], df["seller_zip_code"], df["seller_city"] = zip(*df.apply(lambda row: clean_seller_address(row["seller_address_1"], row["seller_address_2"], row["seller_type"]), axis=1))
  
  # Other transformations and business rules

  # Calculate car age
  # Convert 'built_in' to datetime format and calculate car age in months
  df["built_in"] = pd.to_datetime(df["built_in"], format="%m/%Y", errors="coerce")
  # Calculate car age in months
  df["car_age_in_months"] = df["built_in"].apply(lambda x: (datetime.now().year - x.year) * 12 + datetime.now().month - x.month if pd.notnull(x) else None)
  # Replace any negative car age values with 0
  df["car_age_in_months"] = df["car_age_in_months"].replace(-1, 0)
  # Filter out rows where car age is negative
  df = df[df["car_age_in_months"] >= 0]

  # Update gear type
  df["gear_type"] = df.apply(lambda row: "Automatic" if row["fuel"] == "Electric" and pd.isna(row["gear_type"]) else row["gear_type"], axis=1)
  # Drop rows where gear_type is "Semi-automatic" or is empty
  df = df[~df["gear_type"].isin(["Semi-automatic", None, ""])]

  # Update body type
  df["body_type"] = df["body_type"].replace("Off-Road/Pick-up", "SUV")

  # Drop unnecessary columns or irrelevant rows
  df.drop(columns=[
    "engine_power", "engine_size", "empty_weight", 
    "fuel_consumption", "co2_emission","seller_address_1", 
    "seller_address_2", "manufacturer_color", "non-smoker", 
    "previous_owners", "electric_range", "fuel_consumption_km_per_l"
  ], inplace=True)

  # Drop empty rows without kilometrage
  df.dropna(subset=["km"], inplace=True)

  # Assume that if "full_service_history" is empty, then "full_service_history" = "No"
  df["full_service_history"] = df["full_service_history"].fillna("No")

  # Save transformed data as CSV
  df.to_csv(os.path.join(TRANSFORMED_FILE_PATH, "transformed_car_listing.csv"), index=False)
  print("Data transformation completed!")

# Run transformation
if __name__ == "__main__":
    transform_data()
