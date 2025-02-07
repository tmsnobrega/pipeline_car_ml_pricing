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
  # Convert 'built_in' and 'timestamp' to datetime
  df["built_in"] = pd.to_datetime(df["built_in"], format="%m/%Y", errors="coerce")
  df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
  return df

# # Extract unique equipment features across all cars
# def extract_equipment_features(df):
#   all_equipment = set()
#   df["equipment"].dropna().apply(lambda x: all_equipment.update(x))
#   equipment_df = df["equipment"].apply(lambda x: {feature: 1 if feature in (x or []) else 0 for feature in all_equipment})
#   equipment_df = pd.DataFrame(equipment_df.tolist())
#   df = pd.concat([df, equipment_df], axis=1)
#   return df

def extract_equipment_features(df):
  # Define relevant features to keep
  relevant_features = {
      # Performance & Driving
      "Adaptive Cruise Control", "Cruise control", "Sport package", "Sport suspension",
      "Shift paddles", "Air suspension", "Parking assist system self-steering",
      "Parking assist system camera", "360° camera", "Trailer hitch", "Hill Holder",
      # Safety & Driver Assistance
      "Blind spot monitor", "Lane departure warning system", "Emergency brake assistant",
      "Traffic sign recognition", "Night view assist", "Headlight washer system",
      "Heads-up display", "Distance warning system", "Parking assist system sensors front",
      "Parking assist system sensors rear",
      # Comfort & Interior
      "Leather seats", "Seat heating", "Seat ventilation", "Heated steering wheel",
      "Electrically adjustable seats", "Ambient lighting", "Panorama roof",
      "Sunroof", "Multi-function steering wheel", "Induction charging for smartphones",
      "WLAN / WiFi hotspot",
      # Infotainment & Connectivity
      "Android Auto", "Apple CarPlay", "Navigation system", "Voice Control",
      "Digital cockpit", "Touch screen", "Sound system", "Integrated music streaming",
      "Bluetooth", "USB"
  }
  # Convert the 'equipment' list into a dictionary of binary indicators
  equipment_df = df["equipment"].apply(
      lambda x: {feature: 1 if feature in (x or []) else 0 for feature in relevant_features}
  )
  equipment_df = pd.DataFrame(equipment_df.tolist())
  df = pd.concat([df, equipment_df], axis=1)
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
  df = extract_equipment_features(df)
  df = convert_data_types(df)


  ### Other transformations and business rules ###

  # Calculate car age in months
  df["car_age_in_months"] = df["built_in"].apply(lambda x: (datetime.now().year - x.year) * 12 + datetime.now().month - x.month if pd.notnull(x) else None)
  df["car_age_in_months"] = df["car_age_in_months"].replace(-1, 0)
  df = df[df["car_age_in_months"] >= 0]

  # Calculate years active on the platform
  df["years_active"] = df["active_since"].apply(lambda x: (datetime.now().year - x) if pd.notnull(x) else None)

  # Update gear type for eletric cars
  df["gear_type"] = df.apply(lambda row: "Automatic" if row["fuel"] == "Electric" else row["gear_type"], axis=1)

  # Update number of previous owners & used_or_new
  df["previous_owners"] = df.apply(lambda row: 0 if row["used_or_new"] == "New" else row["previous_owners"], axis=1)
  df.loc[(df["car_age_in_months"] <= 12) & (df["km"] < 1_000), ["previous_owners", "used_or_new"]] = [0, "New"]
  df["used_or_new"] = df["used_or_new"].apply(lambda x: "Used" if x != "New" else x)
  
  # Update body type labels for SUVs
  df["body_type"] = df["body_type"].replace("Off-Road/Pick-up", "SUV")

  # Update engine size thresholds
  df["engine_size_cc"] = df["engine_size_cc"].apply(lambda x: x if 600 <= x <= 8_000 else None)

  # Update co2 emission thresholds and co2 emission depeding on the fuel type
  df["co2_emission_g_per_km"] = df["co2_emission_g_per_km"].apply(lambda x: x if 0 <= x <= 300 else None)
  df["co2_emission_g_per_km"] = df.apply(lambda row: 0 if row["fuel"] == "Electric" else row["co2_emission_g_per_km"], axis=1)
  df["co2_emission_g_per_km"] = df.apply(lambda row: None if row["co2_emission_g_per_km"] == 0 and row["fuel"] != "Electric" else row["co2_emission_g_per_km"], axis=1)

  # Establish min and max thresholds
  df["empty_weight_kg"] = df["empty_weight_kg"].apply(lambda x: x if 1_000 <= x <= 3_000 else None)
  df["km"] = df["km"].apply(lambda x: x if 0 <= x <= 400_000 else None)
  df["engine_power_hp"] = df["engine_power_hp"].apply(lambda x: x if 70 <= x <= 700 else None)  
  
  ### Drop unnecessary data according to exploration/explore_car_listing.py ###

  # Drop irrelevant columns
  df.drop(columns=[
    "engine_power", "engine_size", "empty_weight", 
    "fuel_consumption", "co2_emission","seller_address_1", 
    "seller_address_2", "manufacturer_color", "non-smoker", 
    "fuel_consumption_km_per_l", "seats", "paint", 
    "equipment"
  ], inplace=True)

  # Drop irrelevant rows
  df.dropna(subset=["km", "built_in", "price", "manufacturer", "car"], inplace=True)
  df.drop(df[df["gear_type"].isin(["Semi-automatic", None, ""])].index, inplace=True)
  df.drop(df[df["fuel"].isin(["Electric/Diesel", None, ""])].index, inplace=True)
  df.drop(df[df["emission_class"].isin(["Euro 4", "Euro 5", "Euro 6c"])].index, inplace=True)
  df.drop(df[(df["fuel"] == "Gasoline") & (df["electric_range"] > 0)].index, inplace=True)
  df.drop(df[df["car"].isin(["UX 300h", "UX 300e"])].index, inplace=True)

  # Drop duplicate rows
  df.sort_values(by="timestamp", inplace=True)
  df.drop_duplicates(subset=["km", "price", "car", "listing_url"], keep="last", inplace=True)


  ### Save transformed data as CSV ###
  df.to_csv(os.path.join(TRANSFORMED_FOLDER_PATH, "transformed_car_listing.csv"), index=False)
  print("Data transformation completed!")

# Run transformation
if __name__ == "__main__":
  transform_data()
