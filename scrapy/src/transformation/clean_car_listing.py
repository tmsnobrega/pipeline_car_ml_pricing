import json
import pandas as pd
import os
import re

# Define paths
RAW_FILE_PATH = "data/raw/car_listing.jsonl"
TRANSFORMED_FOLDER_PATH = "data/transformed"
os.makedirs(TRANSFORMED_FOLDER_PATH, exist_ok=True)

### Define cleaning functions ###

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

# Extract year active on autoscout platform
def extract_year_active_on_autoscout(text):
  if text:
    match = re.search(r'(\d{4})$', text)
    return int(match.group(1)) if match else None
  return None

# Extract zip code from seller address
def extract_zip_code(df):

  def extract_zip(address):
    # Match complete Dutch zip code patterns (e.g., "1234AB")
    match = re.search(r'\b\d{4}\s?[A-Za-z]{2}\b', address, re.IGNORECASE)
    return match.group().replace(" ", "").upper() if match else None
  
  # Initialize the column for zip code
  df['seller_zip_code'] = None
  
  # Handle rows where seller_type is "Private seller"
  private_seller_filter = df['seller_type'] == "Private seller"
  df.loc[private_seller_filter, 'seller_zip_code'] = df.loc[private_seller_filter, 'seller_address_1'].apply(extract_zip)
  
  # Handle rows where seller_type is "Dealer"
  dealer_filter = df['seller_type'] == "Dealer"
  df.loc[dealer_filter, 'seller_zip_code'] = df.loc[dealer_filter, 'seller_address_2'].apply(extract_zip)

  return df

def extract_equipment_features(df):
  # Define relevant features to keep based on your refined list, grouped under specific categories
  relevant_features = {
    "360° camera", "Adaptive Cruise Control", "Ambient lighting", "Android Auto", "Apple CarPlay", "Armrest", "Blind spot monitor", "Bluetooth", "Distance warning system", "Electrically adjustable seats", "Electrically heated windshield", "Electronic parking brake", "Emergency brake assistant", "Induction charging for smartphones", "Keyless central door lock", "Lane departure warning system", "Leather seats", "Navigation system", "On-board computer", "Panorama roof", "Parking assist system camera", "Parking assist system self-steering", "Rain sensor", "Rear airbag", "Rear seat heating", "Seat heating", "Seat ventilation", "Shift paddles", "Speed limit control system", "Sport seats", "Sport suspension", "Start-stop system", "Sunroof", "Touch screen", "Traffic sign recognition", "WLAN / WiFi hotspot", "Xenon headlights"
  }

  # Convert the 'equipment' list into a dictionary of binary indicators
  equipment_df = df["equipment"].apply(lambda x: {feature: 1 if feature in (x or []) else 0 for feature in relevant_features})

  # Convert the list of dictionaries into a DataFrame
  equipment_df = pd.DataFrame(equipment_df.tolist())

  # Concatenate the new features with the original DataFrame
  df = pd.concat([df, equipment_df], axis=1)

  return df

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


### Apply data cleaning rules ###
def clean_data():
  print("Initiating data cleaning...")
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
  df = extract_zip_code(df)
  df = extract_equipment_features(df)
  df = convert_data_types(df)


  ### Other cleanings and business rules ###

  # Establish min and max thresholds for numerical columns
  df["empty_weight_kg"] = df["empty_weight_kg"].apply(lambda x: x if 1_000 <= x <= 3_000 else None)
  df["km"] = df["km"].apply(lambda x: x if 0 <= x <= 400_000 else None)
  df["engine_power_hp"] = df["engine_power_hp"].apply(lambda x: x if 70 <= x <= 700 else None)  
  df["engine_size_cc"] = df["engine_size_cc"].apply(lambda x: x if 600 <= x <= 8_000 else None)
  df["co2_emission_g_per_km"] = df["co2_emission_g_per_km"].apply(lambda x: x if 0 <= x <= 300 else None)

  # Drop irrelevant columns according to exploration/explore_car_listing.py
  df.drop(columns=[
    "engine_power", "engine_size", "empty_weight", 
    "fuel_consumption", "co2_emission", "manufacturer_color", 
    "non-smoker", "fuel_consumption_km_per_l", "seats", 
    "paint", "equipment", "doors",
    "seller_address_1", "seller_address_2", 
  ], inplace=True)

  # Drop irrelevant rows according to exploration/explore_car_listing.py
  df.dropna(subset=["manufacturer", "car", "price", "km", "gear_type", "built_in", "fuel", "body_type", "seller_zip_code"], inplace=True)
  df.drop(df[df["gear_type"] == "Semi-automatic"].index, inplace=True)
  df.drop(df[df["fuel"] == "Electric/Diesel"].index, inplace=True)
  df.drop(df[df["emission_class"].isin(["Euro 4", "Euro 5", "Euro 6c"])].index, inplace=True)
  print(f"\tRows after applying filters: {len(df)}")

  # Drop duplicate rows
  df.sort_values(by="timestamp", inplace=True)
  df.drop_duplicates(subset=["km", "price", "car", "listing_url"], keep="last", inplace=True)
  print(f"\tRows after dropping duplicates: {len(df)}")

  ### Save cleaned data as CSV ###
  df.to_csv(os.path.join(TRANSFORMED_FOLDER_PATH, "cleaned_car_listing.csv"), index=False)
  print("\tData cleaning completed!")

# Run cleaning function
if __name__ == "__main__":
  clean_data()
