import json
import pandas as pd
import os
import re
from datetime import datetime

# Define paths
RAW_FILE_PATH = "data/raw/car_listing.jsonl"
TRANSFORMED_FOLDER_PATH = "data/transformed"
os.makedirs(TRANSFORMED_FOLDER_PATH, exist_ok=True)

### Define transformation functions ###

# Remove leading and trailing whitespaces
def clean_text(text):
  return text.strip() if isinstance(text, str) else text

# Extract numbers from text
def extract_number(text):
  if text:
    numbers = re.findall(r'\d+', text.replace(',', ''))
    return int("".join(numbers)) if numbers else None
  return None

# Extract horsepower from text
def extract_hp(text):
  if text:
    match = re.search(r'(\d+)\s*hp', text)
    return int(match.group(1)) if match else None
  return None

# Convert fuel consumption to km per liter from text
def convert_fuel_consumption(text):
  match = re.search(r'(\d+(\.\d+)?)', text)
  if match:
    liters_per_100km = float(match.group(1))
    return round(100 / liters_per_100km, 2) if liters_per_100km else None
  return None

# Clean seller address
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

# Merge seller address data and trim spaces
def merge_seller_address(seller_address_1, seller_address_2):
  if seller_address_1 and seller_address_2:
    return f"{seller_address_1.strip()}, {seller_address_2.strip()}"
  return (seller_address_1 or seller_address_2).strip() if (seller_address_1 or seller_address_2) else None

# Extract year active on autoscout platform
def extract_year_active_on_autoscout(text):
  if text:
    match = re.search(r'(\d{4})$', text)
    return int(match.group(1)) if match else None
  return None

# Convert data types
def convert_data_types(df):
  # Convert columns to numeric
  numerical_columns = ["price", "lease_price_per_month", "km", "engine_power", "seats", "engine_size", "gears", "cylinders", "empty_weight", "fuel_consumption", "co2_emission", "electric_range", "fuel_consumption_km_per_l", "previous_owners", "co2_emission_g_per_km"]
  for col in numerical_columns:
    df[col] = pd.to_numeric(df[col], errors="coerce")
  # Convert 'built_in' to datetime
  df["built_in"] = pd.to_datetime(df["built_in"], format="%m/%Y", errors="coerce")
  # Convert "timestamp" to datetime
  df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
  return df

### Apply transformations ###
def transform_data():
  with open(RAW_FILE_PATH, "r", encoding="utf-8") as file:
    try:
      data = [json.loads(line) for line in file]
    except json.JSONDecodeError as e:
      print(f"Skipping malformed JSON line: {e}")
      return data
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
  df["seller_address_test"] = df.apply(lambda row: merge_seller_address(row["seller_address_1"], row["seller_address_2"]), axis=1)
  df = convert_data_types(df)

  ### Other transformations and business rules ###

  # Calculate car age in months
  df["car_age_in_months"] = df["built_in"].apply(lambda x: (datetime.now().year - x.year) * 12 + datetime.now().month - x.month if pd.notnull(x) else None)
  # Replace car age equal to -1 with 0
  df["car_age_in_months"] = df["car_age_in_months"].replace(-1, 0)
  # Filter out rows where car age is negative
  df = df[df["car_age_in_months"] >= 0]

  # Calculate year of registration
  df["years_active"] = df["active_since"].apply(lambda x: (datetime.now().year - x) if pd.notnull(x) else None)

  # Update gear type for eletric cars
  df["gear_type"] = df.apply(lambda row: "Automatic" if row["fuel"] == "Electric" else row["gear_type"], axis=1)

  # Update number of previous owners
  df["previous_owners"] = df.apply(lambda row: 0 if row["used_or_new"] == "New" else row["previous_owners"], axis=1)
  
  # Update body type labels for SUVs
  df["body_type"] = df["body_type"].replace("Off-Road/Pick-up", "SUV")

  # Update engine size thresholds
  df["engine_size_cc"] = df["engine_size_cc"].apply(lambda x: x if 600 <= x <= 8_000 else None)

  # Update co2 emission thresholds and co2 emission depeding on the fuel type
  df["co2_emission_g_per_km"] = df["co2_emission_g_per_km"].apply(lambda x: x if 0 <= x <= 300 else None)
  df["co2_emission_g_per_km"] = df.apply(lambda row: 0 if row["fuel"] == "Electric" else row["co2_emission_g_per_km"], axis=1)
  df["co2_emission_g_per_km"] = df.apply(lambda row: None if row["co2_emission_g_per_km"] == 0 and row["fuel"] != "Electric" else row["co2_emission_g_per_km"], axis=1)

  # Update empty weight thresholds
  df["empty_weight_kg"] = df["empty_weight_kg"].apply(lambda x: x if 1_000 <= x <= 3_000 else None)

  # Update km thresholds
  df["km"] = df["km"].apply(lambda x: x if 0 <= x <= 400_000 else None)

  # Update engine power thresholds
  df["engine_power_hp"] = df["engine_power_hp"].apply(lambda x: x if 70 <= x <= 700 else None)

  ### Drop unnecessary data according to explore_car_listing.py ###

  # Drop irrelevant columns
  df.drop(columns=[
    "engine_power", "engine_size", "empty_weight", 
    "fuel_consumption", "co2_emission","seller_address_1", 
    "seller_address_2", "manufacturer_color", "non-smoker", 
    "fuel_consumption_km_per_l", "seats"
  ], inplace=True)

  # Drop irrelevant rows
  df.dropna(subset=["km", "built_in", "price", "manufacturer", "car"], inplace=True)
  df.drop(df[df["gear_type"].isin(["Semi-automatic", None, ""])].index, inplace=True)
  df.drop(df[df["fuel"].isin(["Electric/Diesel", None, ""])].index, inplace=True)
  df.drop(df[df["emission_class"].isin(["Euro 4", "Euro 5", "Euro 6c"])].index, inplace=True)
  df.drop(df[(df["fuel"] == "Gasoline") & (df["electric_range"] > 0)].index, inplace=True)

  # Drop duplicate rows
  df.sort_values(by="timestamp", inplace=True)
  df.drop_duplicates(subset=["km", "price", "manufacturer", "car", "listing_url"], keep="last", inplace=True)


  ### Save transformed data as CSV ###
  df.to_csv(os.path.join(TRANSFORMED_FOLDER_PATH, "transformed_car_listing.csv"), index=False)
  print("Data transformation completed!")

# Run transformation
if __name__ == "__main__":
  transform_data()
