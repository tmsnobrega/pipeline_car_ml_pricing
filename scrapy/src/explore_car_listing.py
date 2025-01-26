import os
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Define file paths
RAW_FILE_PATH = "data/raw/car_listing.jsonl"
OUTPUT_DIR = "data/exploration"

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

### Apply transformations ###
def load_data():
    with open(RAW_FILE_PATH, "r", encoding="utf-8") as file:
        data = []
        for line in file:
            try:
                data.append(json.loads(line.strip()))
            except json.JSONDecodeError as e:
                print(f"Skipping malformed JSON line: {e}")

    return pd.DataFrame(data)

df = load_data()
print(df.info())

### 1️⃣ Check Data Availability ###
def plot_missing_values(df):
  missing_percent = df.isnull().mean() * 100
  missing_percent = missing_percent[missing_percent > 0]  # Filter only missing values

  plt.figure(figsize=(12, 6))
  missing_percent.sort_values().plot(kind="barh", color="skyblue", edgecolor="black")
  plt.xlabel("Percentage of Missing Data")
  plt.ylabel("Columns")
  plt.title("Missing Data Percentage per Column")
  plt.grid(axis="x")
  plt.savefig(f"{OUTPUT_DIR}/missing_data.png", dpi=300)
  plt.show()

plot_missing_values(df)

import sys
sys.exit()

# Data Types Overview
def check_data_types(df):
  dtype_info = pd.DataFrame(df.dtypes, columns=["Data Type"])
  dtype_info["Unique Values"] = df.nunique()
  dtype_info.to_csv(f"{OUTPUT_DIR}/data_types.csv", index=True)
  print(dtype_info)

check_data_types(df)

# Summary Statistics
def generate_summary_statistics(df):
  summary = df.describe().transpose()
  summary.to_csv(f"{OUTPUT_DIR}/summary_statistics.csv")
  print(summary)

generate_summary_statistics(df)

# Unique Value Counts for Categorical Data
def check_unique_values(df):
  categorical_columns = df.select_dtypes(include=["object"]).columns
  unique_counts = {col: df[col].nunique() for col in categorical_columns}
  pd.DataFrame.from_dict(unique_counts, orient="index", columns=["Unique Values"]).to_csv(
      f"{OUTPUT_DIR}/unique_values.csv"
  )
  print(unique_counts)

check_unique_values(df)

# Histograms for Numerical Features
def plot_histograms(df):
    num_cols = df.select_dtypes(include=["int64", "float64"]).columns
    df[num_cols].hist(figsize=(12, 10), bins=30, edgecolor="black")
    plt.suptitle("Histograms of Numerical Features")
    plt.savefig(f"{OUTPUT_DIR}/histograms.png", dpi=300)
    plt.show()

plot_histograms(df)

# Bar Charts for Categorical Features
def plot_categorical_distributions(df):
  categorical_columns = ["fuel", "gear_type", "body_type", "manufacturer"]

  for col in categorical_columns:
    plt.figure(figsize=(10, 5))
    df[col].value_counts().plot(kind="bar", color="lightcoral", edgecolor="black")
    plt.title(f"Distribution of {col}")
    plt.xlabel(col)
    plt.ylabel("Count")
    plt.xticks(rotation=45)
    plt.grid(axis="y")
    plt.savefig(f"{OUTPUT_DIR}/{col}_distribution.png", dpi=300)
    plt.show()

plot_categorical_distributions(df)

# Correlation Heatmap
def plot_correlation_matrix(df):
  numeric_df = df.select_dtypes(include=["int64", "float64"])
  plt.figure(figsize=(12, 8))
  sns.heatmap(numeric_df.corr(), annot=True, cmap="coolwarm", fmt=".2f", linewidths=0.5)
  plt.title("Correlation Heatmap")
  plt.savefig(f"{OUTPUT_DIR}/correlation_heatmap.png", dpi=300)
  plt.show()

plot_correlation_matrix(df)

# Scatter Plots for Key Relationships
def plot_scatter_plots(df):
  scatter_pairs = [
    ("price", "km"),
    ("price", "car_age_in_months"),
    ("price", "engine_power"),
  ]

  for x_col, y_col in scatter_pairs:
    if x_col in df.columns and y_col in df.columns:
      plt.figure(figsize=(8, 5))
      sns.scatterplot(x=df[x_col], y=df[y_col], alpha=0.5)
      plt.xlabel(x_col)
      plt.ylabel(y_col)
      plt.title(f"{y_col} vs {x_col}")
      plt.grid()
      plt.savefig(f"{OUTPUT_DIR}/{y_col}_vs_{x_col}.png", dpi=300)
      plt.show()

plot_scatter_plots(df)

# Boxplots for Price Analysis
def plot_boxplots(df):
  plt.figure(figsize=(12, 6))
  sns.boxplot(x=df["manufacturer"], y=df["price"])
  plt.xticks(rotation=45)
  plt.title("Boxplot of Price by Manufacturer")
  plt.grid()
  plt.savefig(f"{OUTPUT_DIR}/boxplot_price.png", dpi=300)
  plt.show()

plot_boxplots(df)

# Outlier Detection (IQR Method)
def detect_outliers(df, column):
  Q1 = df[column].quantile(0.25)
  Q3 = df[column].quantile(0.75)
  IQR = Q3 - Q1
  outlier_mask = (df[column] < (Q1 - 1.5 * IQR)) | (df[column] > (Q3 + 1.5 * IQR))
  
  outliers = df[outlier_mask]
  outliers.to_csv(f"{OUTPUT_DIR}/{column}_outliers.csv", index=False)
  print(f"Found {len(outliers)} outliers in {column}")

for col in ["price", "km", "car_age_in_months"]:
  if col in df.columns:
    detect_outliers(df, col)

# Check for Duplicate Rows
def check_duplicates(df):
  duplicates = df[df.duplicated()]
  duplicates.to_csv(f"{OUTPUT_DIR}/duplicates.csv", index=False)
  print(f"Found {len(duplicates)} duplicate rows.")

check_duplicates(df)

print(f"Exploratory Analysis Completed! Results saved in '{OUTPUT_DIR}'")
