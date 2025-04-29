import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database Configuration (Use environment variables for security)
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Define file path
TRANSFORMED_FILE_PATH = "data/transformed/transformed_car_listing.csv"

# Database connection
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def create_table():
	with engine.begin() as conn:
		conn.execute(text("""
			CREATE TABLE IF NOT EXISTS car_listings (
				id SERIAL PRIMARY KEY,
				manufacturer TEXT,
				car TEXT,
				price NUMERIC,
				lease_price_per_month NUMERIC,
				km NUMERIC,
				gear_type TEXT,
				built_in DATE,
				car_age_in_months INTEGER,
				fuel TEXT,
				body_type TEXT,
				seller_type TEXT,
				seller_name TEXT,
				years_active_on_platform NUMERIC,
				zip_code TEXT,
				city TEXT,
				province TEXT,
				lat NUMERIC,
				lon NUMERIC,
				listing_url TEXT UNIQUE,
				timestamp TIMESTAMP
			)
		"""))
		print("Table 'car_listings' is ready.")

def load_csv():
	try:
		df = pd.read_csv(TRANSFORMED_FILE_PATH)
		print(f"Loaded {len(df)} rows from CSV file.")
		return df
	except Exception as e:
		print(f"Failed to load CSV file: {e}")
		return None

def insert_data(df):
	if df is None or df.empty:
		print("No data to insert.")
		return
	try:
		# Bulk insert using to_sql for efficiency
		df.to_sql("car_listings", engine, if_exists="append", index=False, method="multi")
		print(f"Inserted {len(df)} rows into car_listings.")
	except Exception as e:
		print(f"Failed to insert data: {e}")

def upload_data_to_db():
	create_table()
	df = load_csv()
	insert_data(df)
	print("Upload process completed.")

if __name__ == "__main__":
	upload_data_to_db()
