import os
import subprocess
import sys

# Define paths
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SCRAPY_PATH = os.path.join(PROJECT_ROOT, "scrapy", "src")
TRANSFORMATION_PATH = os.path.join(SCRAPY_PATH, "transformation")

def run_scrapy_spider():
    print("📥 Starting Scrapy spider...")
    subprocess.run([
        "scrapy", "crawl", "scrape_car_listing",
        "-o", os.path.join(PROJECT_ROOT, "data", "raw", "car_listing.jsonl")
    ], cwd=SCRAPY_PATH, check=True)

def run_script(script_filename):
    print(f"▶️ Running script: {script_filename}")
    subprocess.run([sys.executable, os.path.join(TRANSFORMATION_PATH, script_filename)], check=True)

if __name__ == "__main__":
    try:
        #run_scrapy_spider()
        run_script("clean_car_listing.py")
        run_script("transform_car_listing.py")
        print("✅ Pipeline finished successfully.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error: {e}")
