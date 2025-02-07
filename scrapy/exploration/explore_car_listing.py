import os
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def explore_data():
  # Define file paths
  RAW_FILE_PATH = "data/raw/car_listing.jsonl"
  TRANSFORMED_FILE_PATH = "data/transformed/transformed_car_listing.csv"
  OUTPUT_DIR = "data/exploration"

  # Ensure output directory exists
  os.makedirs(OUTPUT_DIR, exist_ok=True)

  # Load raw data
  def load_data():
    with open(RAW_FILE_PATH, "r", encoding="utf-8") as file:
      data = []
      for line in file:
        try:
          data.append(json.loads(line.strip()))
        except json.JSONDecodeError as e:
          print(f"Skipping malformed JSON line: {e}")

    return pd.DataFrame(data)
  df_raw = load_data()
  df_raw.name = "raw"

  df = pd.read_csv(TRANSFORMED_FILE_PATH)
  df.name = "transformed"


  # Check data availability
  def plot_missing_values(df):
    df.replace("", np.nan, inplace=True)
    complete_columns = df.notnull().all()
    print("Columns without any missing values:")
    print(complete_columns[complete_columns].index.tolist())

    missing_percent = df.isnull().mean() * 100
    missing_percent = missing_percent[missing_percent > 0]
    plt.figure(figsize=(12, 6))
    missing_percent.sort_values().plot(kind="barh", color="skyblue", edgecolor="black")
    plt.xlabel("Percentage of Missing Data")
    plt.ylabel("Columns")
    plt.title("Missing Data Percentage per Column")
    plt.grid(axis="x")
    plt.savefig(f"{OUTPUT_DIR}/missing_data_{df.name}.png", dpi=300)
    #plt.show()
  plot_missing_values(df_raw)
  plot_missing_values(df)


  # Summary Statistics with Data Types
  def generate_summary_statistics(df):
    summary = df.describe().transpose()
    summary.to_csv(f"{OUTPUT_DIR}/summary_statistics_{df.name}.csv")
    #print(summary)
  generate_summary_statistics(df_raw)
  generate_summary_statistics(df)

  
  # Histograms for Numerical Features
  def plot_histograms(df):
      num_cols = df.select_dtypes(include=["int64", "float64"]).columns
      df[num_cols].hist(figsize=(12, 6), bins=10, edgecolor="black")
      plt.suptitle("Histograms of Numerical Features")
      plt.savefig(f"{OUTPUT_DIR}/histograms.png", dpi=300)
      plt.show()
  plot_histograms(df)


  # Bar Charts for Categorical Features
  def plot_categorical_distributions(df):
    categorical_columns = ["fuel", "gear_type", "body_type", "manufacturer"]
    for col in categorical_columns:
      plt.figure(figsize=(12, 6))
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
    plt.figure(figsize=(12, 6))
    sns.heatmap(numeric_df.corr(), annot=True, cmap="coolwarm", fmt=".2f", linewidths=0.5)
    plt.title("Correlation Heatmap")
    plt.savefig(f"{OUTPUT_DIR}/correlation_heatmap.png", dpi=300)
    plt.show()
  plot_correlation_matrix(df)


  # Scatter Plots for Key Relationships
  def plot_scatter_plots(df):
    scatter_pairs = [
      ("km", "price"),
      ("car_age_in_months", "price"),
      ("engine_power", "price"),
    ]
    for x_col, y_col in scatter_pairs:
      if x_col in df.columns and y_col in df.columns:
        plt.figure(figsize=(12, 6))
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

  print(f"Exploratory Analysis Completed! Results saved in '{OUTPUT_DIR}'")

# Run exploratory analysis
if __name__ == "__main__":
  explore_data()
