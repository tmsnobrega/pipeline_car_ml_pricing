import pandas as pd
import os
from datetime import datetime, timedelta
import numpy as np
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()  
GEONAMES_USERNAME = os.getenv("GEONAMES_USERNAME")

# Define paths
TRANSFORMED_FOLDER_PATH = "data/transformed"

# Function to fetch Geonames data
def fetch_geonames_data(zip_code):
  url = "http://api.geonames.org/postalCodeLookupJSON"
  params = {
    "postalcode": zip_code,
    "country": "NL",
    "username": GEONAMES_USERNAME
  }

  response = requests.get(url, params=params)
  if response.status_code == 200:
    data = response.json()
    if "postalcodes" in data and len(data["postalcodes"]) > 0:
      place = data["postalcodes"][0]
      return {
        "lon": place.get("lng"),
        "lat": place.get("lat"),
        "city": place.get("placeName"),
        "province": place.get("adminName1")
      }
    else:
      return {"lon": None, "lat": None, "city": None, "province": None}
  else:
    print(f"Error: {response.status_code}")
    return {"lon": None, "lat": None, "city": None, "province": None}

# Function to add Geonames data to DataFrame
def add_geonames_data(df):
  # Initialize empty lists for new columns
  lon_list, lat_list, city_list, province_list = [], [], [], []

  # Load existing geonames data from file if it exists
  geonames_cache_file = os.path.join(TRANSFORMED_FOLDER_PATH, "geonames_cache.csv")
  if os.path.exists(geonames_cache_file):
    geonames_cache_df = pd.read_csv(geonames_cache_file)
    geonames_cache = geonames_cache_df.set_index('zip_code').T.to_dict('dict')
  else:
    geonames_cache = {}

  zip_codes = df["zip_code"].unique()

  # Fetch data for new zip codes and update the cache
  new_data = []
  for zip_code in zip_codes:
    if zip_code not in geonames_cache:
      geonames_data = fetch_geonames_data(zip_code)
      geonames_cache[zip_code] = geonames_data
      new_data.append({
        "zip_code": zip_code,
        "lon": geonames_data["lon"],
        "lat": geonames_data["lat"],
        "city": geonames_data["city"],
        "province": geonames_data["province"]
      })

  # Save the updated geonames cache to file
  if new_data:
    new_data_df = pd.DataFrame(new_data)
    if os.path.exists(geonames_cache_file):
      new_data_df.to_csv(geonames_cache_file, mode='a', header=False, index=False)
    else:
      new_data_df.to_csv(geonames_cache_file, index=False)

  # Populate lists with geonames data
  for zip_code in df["zip_code"]:
    if pd.notnull(zip_code) and zip_code in geonames_cache:
      geonames_data = geonames_cache[zip_code]
      lon_list.append(geonames_data["lon"])
      lat_list.append(geonames_data["lat"])
      city_list.append(geonames_data["city"])
      province_list.append(geonames_data["province"])
    else:
      lon_list.append(None)
      lat_list.append(None)
      city_list.append(None)
      province_list.append(None)

  # Add new columns to the DataFrame
  df["lon"] = lon_list
  df["lat"] = lat_list
  df["city"] = city_list
  df["province"] = province_list

  return df

### Apply transformations ###
def transform_data():
  print("Initiating data transformation...")
  df = pd.read_csv(os.path.join(TRANSFORMED_FOLDER_PATH, "cleaned_car_listing.csv")) # Load cleaned data

  print(f"\tRows before any transformations: {len(df)}")

  # Update labels
  df["body_type"] = df["body_type"].replace("Off-Road/Pick-up", "SUV")
  df["fuel"] = df["fuel"].replace("Electric/Gasoline", "Hybrid")

  # Update gear_type
  df["gear_type"] = df.apply(lambda row: "Automatic" if row["fuel"] == "Electric" else row["gear_type"], axis=1) # If a car is eletric, it is automatic

  # Calculate car age
  df["built_in"] = pd.to_datetime(df["built_in"], errors="coerce")
  df.drop(df[df["built_in"] > datetime.now() + timedelta(days=30)].index, inplace=True) # If built_in is more than 30 days in the future, then drop the row
  df["car_age_in_months"] = df["built_in"].apply(lambda x: max((datetime.now().year - x.year) * 12 + datetime.now().month - x.month, 0) if pd.notnull(x) else None) # Expressed in months

  # Update previous_owners & used_or_new
  df["previous_owners"] = df.apply(lambda row: 0 if row["used_or_new"] == "New" else row["previous_owners"], axis=1) # If a car is new, it has 0 previous owners
  df.loc[(df["car_age_in_months"] <= 12) & (df["km"] < 1_000), ["previous_owners", "used_or_new"]] = [0, "New"] # If a car is less than 12 months old and less then 1000 km, it is "New" and has 0 previous owners
  df["used_or_new"] = df["used_or_new"].apply(lambda x: "Used" if x != "New" else x) # If a car is not new, it is used
  
  # Update drive_train
  df["drive_train"] = np.where(df["description"].str.contains("AWD|4WD", na=False), "4WD", df["drive_train"]) # If the description contains AWD or 4WD, the drive train is set to 4WD
  df.loc[(df["fuel"] == "Electric") & (df["drive_train"].isin([np.nan, "Front"])), "drive_train"] = "Rear" # If car is "Eletric", and the drive train is either empty or "Front", then it is set to "Rear"
  df.loc[(df["fuel"] != "Electric") & (df["drive_train"].isin([np.nan, "Rear"])), "drive_train"] = "Front" # If the car is not eletric and the drive train is either empty or "Rear", it is set to "Front"

  # Update full_service_history
  df["full_service_history"] = df.apply(lambda row: 1 if pd.isnull(row["full_service_history"]) and row["used_or_new"] == "New" else 0, axis=1) # If "full_service_history" is null, and "used_or_new" is "New", then "full_service_history" is set to 1, else it is set to 0

  # Update gear
  df.loc[df["fuel"] == "Electric", "gears"] = 1 # If a car is eletric, it has 1 gear
  df.loc[(df["gears"] == 8) & (df["manufacturer"] != "Volvo"), "gears"] = pd.NA # If a car has 8 gears and is not a Volvo, it is set to None
  df.loc[(df["gears"] > 8) | (df["gears"].isin([2, 3, 4])), "gears"] = pd.NA # If a car has more than 8 gears or has 2, 3, or 4 gears, it is set to None 
  # If fuel is not Eletric, manufactuer in not Toyota or Lexus, and gears is 1, it is set to None
  df.loc[(df["fuel"] != "Electric") & (~df["manufacturer"].isin(["Toyota", "Lexus"])) & (df["gears"] == 1), "gears"] = pd.NA
  # Update gears based on car model
  df.loc[df["car"] == "1", "gears"] = 7
  df.loc[df["car"] == "3", "gears"] = 6
  df.loc[df["car"] == "CX-30", "gears"] = 6
  df.loc[df["car"] == "Formentor", "gears"] = 6
  df.loc[df["car"] == "HR-V", "gears"] = 6
  df.loc[df["car"] == "Niro", "gears"] = 6
  df.loc[df["car"] == "TUCSON", "gears"] = 6

  # Update co2_emission_g_per_km depeding on the fuel
  df["co2_emission_g_per_km"] = df.apply(lambda row: 0 if row["fuel"] == "Electric" else row["co2_emission_g_per_km"], axis=1) # If a car is eletric, it has 0 co2 emission
  df["co2_emission_g_per_km"] = df.apply(lambda row: None if row["co2_emission_g_per_km"] == 0 and row["fuel"] != "Electric" else row["co2_emission_g_per_km"], axis=1) # If a car is not eletric and has 0 co2 emission, it is set to None

  # Calculate years active on the platform
  df["years_active"] = df["active_since"].apply(lambda x: (datetime.now().year - x) if pd.notnull(x) else None)

  # Drop irrelevant rows
  df.drop(df[(df["fuel"] == "Gasoline") & (df["electric_range"] > 0)].index, inplace=True) # Drop rows where electric_range is greater than 0 and the fuel is Gasoline
  df.drop(df[(df["fuel"] == "Electric") & (~df["manufacturer"].isin(["Kia", "Tesla", "Volvo"]))].index, inplace=True) # Only keep Tesla, Kia and Volvo electric cars as other manufacturers do not have electric cars
  df.drop(df[(df["manufacturer"] == "Lynk & Co") & (df["fuel"] != "Hybrid")].index, inplace=True) # Only keep Lynk & Co hybrid cars
  df.drop(df[(df["car"] == "Niro") & (df["fuel"] != "Hybrid")].index, inplace=True) # Only keep Kia Niro hybrid cars
  df.drop(df[(df["manufacturer"] == "Toyota") & (df["fuel"] != "Hybrid")].index, inplace=True) # Only keep Toyota hybrid cars
  df.drop(df[(df["fuel"] == "Diesel") & (df["car"] != "A3")].index, inplace=True) # Only keep Audi A3 Diesel cars as there are not enough data entries for other Diesel cars
  df.drop(df[df["car"].isin(["UX 300h", "UX 300e"])].index, inplace=True) # Not enought data entries for these Lexus models
  
  # Create dataframe for zip_code
  if "zip_code" in df.columns:
    print("\tAdding Geonames data based on zip_code...")
    df = add_geonames_data(df)

  ### Save transformed data as CSV ###
  df.to_csv(os.path.join(TRANSFORMED_FOLDER_PATH, "transformed_car_listing.csv"), index=False)
  print("\tData transformation completed!")

  print(f"\tRows after applying all transformations: {len(df)}")

# Run transformation
if __name__ == "__main__":
  transform_data()
