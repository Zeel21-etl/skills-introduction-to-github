import requests
import pandas as pd
import sqlite3
from datetime import datetime
import os

# ------------------------------
# Configuration
# ------------------------------
API_KEY = "02cd97e6596df900af02efa2d414a2a9"  
CITIES = [
    "Berlin", "Munich", "Hamburg", "Cologne", "Frankfurt",
    "Stuttgart", "Düsseldorf", "Dortmund", "Essen", "Leipzig",
    "Bremen", "Dresden", "Hanover", "Nuremberg", "Duisburg",
    "Bochum", "Wuppertal", "Bielefeld", "Bonn", "Münster"
]
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
DATA_DIR = r"C:\Users\zeelp\PyCharmMiscProject\files"
CSV_PATH = os.path.join(DATA_DIR, "cleaned_weather.csv")
DB_PATH = os.path.join(DATA_DIR, "weather.db")

# ------------------------------
# Step 1: extract Weather Data
def fetch_weather_data():
    records = []

    for city in CITIES:
        params = {
            "q": city,
            "appid": API_KEY,
            "units": "metric"
        }
        try:
            response = requests.get(BASE_URL, params=params)
            data = response.json()

            if response.status_code == 200:
                record = {
                    "city": city,
                    "temperature": data["main"]["temp"],
                    "feels_like": data["main"]["feels_like"],
                    "temp_min": data["main"]["temp_min"],
                    "temp_max": data["main"]["temp_max"],
                    "humidity": data["main"]["humidity"],
                    "pressure": data["main"]["pressure"],
                    "wind_speed": data["wind"]["speed"],
                    "wind_deg": data["wind"].get("deg"),
                    "weather_main": data["weather"][0]["main"],
                    "weather_description": data["weather"][0]["description"],
                    "cloud_coverage": data["clouds"]["all"],
                    "timestamp": data["dt"],
                    "datetime": datetime.utcfromtimestamp(data["dt"]).strftime('%Y-%m-%d %H:%M:%S'),
                    "temp_range": data["main"]["temp_max"] - data["main"]["temp_min"]
                }
                records.append(record)
            else:
                print(f"[ERROR] Failed for {city}: {data.get('message')}")

        except Exception as e:
            print(f"[EXCEPTION] Error fetching data for {city}: {str(e)}")

    return pd.DataFrame(records)

# ------------------------------
# Step 2: Store Data
def store_data(df):
    # Save to CSV
    df.to_csv(CSV_PATH, index=False)
    print(f"✅ Data saved to CSV: {CSV_PATH}")

    # Save to SQLite DB
    conn = sqlite3.connect(DB_PATH)
    df.to_sql("weather_data", conn, if_exists="replace", index=False)
    conn.close()
    print(f"✅ Data saved to SQLite DB: {DB_PATH}")

# ------------------------------
# Main Pipeline Execution
def run_etl():
    print("🚀 Starting ETL pipeline...")
    df = fetch_weather_data()

    if df.empty:
        print("⚠️ No data fetched. Skipping storage.")
    else:
        store_data(df)
    print("✅ ETL pipeline completed.")

# ------------------------------
# Trigger when script runs
# ------------------------------
if __name__ == "__main__":
    run_etl()
