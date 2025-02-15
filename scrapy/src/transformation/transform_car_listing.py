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

# # Extract all unique equipment features across all cars
# def extract_all_equipment_features(df):
#   all_equipment = set()
#   df["equipment"].dropna().apply(lambda x: all_equipment.update(x))
#   equipment_df = df["equipment"].apply(lambda x: {feature: 1 if feature in (x or []) else 0 for feature in all_equipment})
#   equipment_df = pd.DataFrame(equipment_df.tolist())
#   df = pd.concat([df, equipment_df], axis=1)
#   return df

def extract_equipment_features(df):
  # Define relevant features to keep based on your refined list, grouped under specific categories
  relevant_features = {
    "360° camera", 
    "Adaptive Cruise Control",  
    "Ambient lighting", 
    "Android Auto", 
    "Apple CarPlay", 
    "Armrest", 
    "Blind spot monitor", 
    "Bluetooth", 
    "Distance warning system", 
    "Electrically adjustable seats", 
    "Electrically heated windshield", 
    "Electronic parking brake", 
    "Emergency brake assistant",
    "Induction charging for smartphones", 
    "Keyless central door lock", 
    "Lane departure warning system", 
    "Leather seats", 
    "Navigation system", 
    "On-board computer", 
    "Panorama roof",  
    "Parking assist system camera", 
    "Parking assist system self-steering", 
    "Rain sensor", 
    "Rear airbag", 
    "Rear seat heating", 
    "Seat heating", 
    "Seat ventilation", 
    "Shift paddles",
    "Speed limit control system", 
    "Sport seats", 
    "Sport suspension",
    "Start-stop system", 
    "Sunroof", 
    "Touch screen",  
    "Traffic sign recognition",
    "WLAN / WiFi hotspot", 
    "Xenon headlights"
  }
  # Convert the 'equipment' list into a dictionary of binary indicators
  equipment_df = df["equipment"].apply(lambda x: {feature: 1 if feature in (x or []) else 0 for feature in relevant_features})
  # Convert the list of dictionaries into a DataFrame
  equipment_df = pd.DataFrame(equipment_df.tolist())
  # Concatenate the new features with the original DataFrame
  df = pd.concat([df, equipment_df], axis=1)
  return df


### Apply transformations ###
def transform_data():
  print("Initiating data transformation...")
  with open(RAW_FILE_PATH, "r", encoding="utf-8") as file:
    try:
      data = [json.loads(line) for line in file]
    except json.JSONDecodeError as e:
      print(f"Skipping malformed JSON line: {e}")
      return data
  df = pd.DataFrame(data)

  print(f"\tRows before any filters: {len(df)}")

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
  df["seller_address_merged"] = df.apply(lambda row: merge_seller_address(row["seller_address_1"], row["seller_address_2"]), axis=1)
  df = extract_equipment_features(df)
  df = convert_data_types(df)


  ### Other transformations and business rules ###

  # Establish min and max thresholds for numerical columns
  df["empty_weight_kg"] = df["empty_weight_kg"].apply(lambda x: x if 1_000 <= x <= 3_000 else None)
  df["km"] = df["km"].apply(lambda x: x if 0 <= x <= 400_000 else None)
  df["engine_power_hp"] = df["engine_power_hp"].apply(lambda x: x if 70 <= x <= 700 else None)  
  df["engine_size_cc"] = df["engine_size_cc"].apply(lambda x: x if 600 <= x <= 8_000 else None)
  df["co2_emission_g_per_km"] = df["co2_emission_g_per_km"].apply(lambda x: x if 0 <= x <= 300 else None)

  # Calculate car age in months
  df["car_age_in_months"] = df["built_in"].apply(lambda x: (datetime.now().year - x.year) * 12 + datetime.now().month - x.month if pd.notnull(x) else None)
  df["car_age_in_months"] = df["car_age_in_months"].replace(-1, 0)
  df = df[df["car_age_in_months"] >= 0]

  # Calculate years active on the platform
  df["years_active"] = df["active_since"].apply(lambda x: (datetime.now().year - x) if pd.notnull(x) else None)

  # Update gear type for eletric cars
  df["gear_type"] = df.apply(lambda row: "Automatic" if row["fuel"] == "Electric" else row["gear_type"], axis=1) # If a car is eletric, it is automatic

  # Update number of previous owners & used_or_new
  df["previous_owners"] = df.apply(lambda row: 0 if row["used_or_new"] == "New" else row["previous_owners"], axis=1) # If a car is new, it has 0 previous owners
  df.loc[(df["car_age_in_months"] <= 12) & (df["km"] < 1_000), ["previous_owners", "used_or_new"]] = [0, "New"] # If a car is lesnn than 12 months old and less then 1000 km, it is new and has 0 previous owners
  df["used_or_new"] = df["used_or_new"].apply(lambda x: "Used" if x != "New" else x) # If a car is not new, it is used
  
  # Update drive train
  df["drive_train"] = df.apply(lambda row: "4WD" if pd.isna(row["drive_train"]) and any(x in row["description"] for x in ["AWD", "4WD"]) else row["drive_train"], axis=1)

  # Update gear
  df.loc[df["fuel"] == "Electric", "gears"] = 1 # If a car is eletric, it has 1 gear
  df.loc[(df["gears"] == 8) & (df["manufacturer"] != "Volvo"), "gears"] = pd.NA # If a car has 8 gears and is not a Volvo, it is set to None
  df.loc[df["manufacturer"] == "Lynk & Co", "gears"] = 7 # If manufacturer is Lynk & Co, gears is set to 7
  df.loc[df["car"] == "Niro", "gears"] = 6 # If car is Niro, gears is set to 6
  df.loc[df["gears"] > 8, "gears"] = pd.NA # If a car has more than 8 gears, it is set to None
  df.loc[df["gears"].isin([2, 3, 4]), "gears"] = pd.NA # If a car has 2, 3 or 4 gears, it is set to None

  # Update labels
  df["body_type"] = df["body_type"].replace("Off-Road/Pick-up", "SUV")
  df["fuel"] = df["fuel"].replace("Electric/Gasoline", "Hybrid")

  # Update co2 emission depeding on the fuel type
  df["co2_emission_g_per_km"] = df.apply(lambda row: 0 if row["fuel"] == "Electric" else row["co2_emission_g_per_km"], axis=1) # If a car is eletric, it has 0 co2 emission
  df["co2_emission_g_per_km"] = df.apply(lambda row: None if row["co2_emission_g_per_km"] == 0 and row["fuel"] != "Electric" else row["co2_emission_g_per_km"], axis=1) # If a car is not eletric and has 0 co2 emission, it is set to None


  ### Drop unnecessary data according to exploration/explore_car_listing.py ###

  # Drop irrelevant columns
  df.drop(columns=[
    "engine_power", "engine_size", "empty_weight", 
    "fuel_consumption", "co2_emission", "manufacturer_color", 
    "non-smoker", "fuel_consumption_km_per_l", "seats", 
    "paint", "equipment", "doors",
    #"seller_address_1", "seller_address_2",
  ], inplace=True)

  # Drop irrelevant rows
  df.dropna(subset=["km", "built_in", "price", "manufacturer", "car"], inplace=True)
  df.drop(df[df["gear_type"].isin(["Semi-automatic", None, ""])].index, inplace=True)
  df.drop(df[df["fuel"].isin(["Electric/Diesel", None, ""])].index, inplace=True)
  df.drop(df[df["emission_class"].isin(["Euro 4", "Euro 5", "Euro 6c"])].index, inplace=True)
  df.drop(df[(df["fuel"] == "Gasoline") & (df["electric_range"] > 0)].index, inplace=True)
  df.drop(df[(df["fuel"] == "Electric") & (~df["manufacturer"].isin(["Kia", "Tesla", "Volvo"]))].index, inplace=True) # Only keep Tesla, Kia and Volvo electric cars as other manufacturers do not have electric cars
  df.drop(df[(df["manufacturer"] == "Lynk & Co") & (df["fuel"] != "Hybrid")].index, inplace=True) # Only keep Lynk & Co hybrid cars
  df.drop(df[(df["car"] == "Niro") & (df["fuel"] != "Hybrid")].index, inplace=True) # Only keep Kia Niro hybrid cars
  df.drop(df[(df["manufacturer"] == "Toyota") & (df["fuel"] != "Hybrid")].index, inplace=True) # Only keep Toyota hybrid cars
  df.drop(df[(df["fuel"] == "Diesel") & (df["car"] != "A3")].index, inplace=True) # Only keep Audi A3 Diesel cars as there are not enough data entries for other Diesel cars
  df.drop(df[df["car"].isin(["UX 300h", "UX 300e"])].index, inplace=True) # Not enought data entries for these Lexus models
  print(f"\tRows after applying filters: {len(df)}")

  # Drop duplicate rows
  df.sort_values(by="timestamp", inplace=True)
  df.drop_duplicates(subset=["km", "price", "car", "listing_url"], keep="last", inplace=True)
  print(f"\tRows after dropping duplicates: {len(df)}")

  ### Save transformed data as CSV ###
  df.to_csv(os.path.join(TRANSFORMED_FOLDER_PATH, "transformed_car_listing.csv"), index=False)
  print("\tData transformation completed!")

# Run transformation
if __name__ == "__main__":
  transform_data()
