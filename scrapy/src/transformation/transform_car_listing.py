import pandas as pd
import os
from datetime import datetime, timedelta
import numpy as np

# Define paths
TRANSFORMED_FOLDER_PATH = "data/transformed"

### Apply transformations ###
def transform_data():
  print("Initiating data transformation...")
  df = pd.read_csv(os.path.join(TRANSFORMED_FOLDER_PATH, "cleaned_car_listing.csv"))

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
  
  ### Save transformed data as CSV ###
  df.to_csv(os.path.join(TRANSFORMED_FOLDER_PATH, "transformed_car_listing.csv"), index=False)
  print("\tData transformation completed!")

  print(f"\tRows after applying all transformations: {len(df)}")

# Run transformation
if __name__ == "__main__":
  transform_data()
