import requests
from dotenv import load_dotenv
import os

load_dotenv()

API_TOKEN = os.getenv("CDC_API_TOKEN")
URL = "https://data.cdc.gov/resource/ksfb-ug5d.json"

params = {"$limit": 1}
headers = {"X-App-Token": API_TOKEN}

response = requests.get(URL, params=params, headers=headers)
data = response.json()

columns = list(data[0].keys())

print("EXPECTED_COLUMNS = [")
for col in columns:
    print(f'    "{col}",')
print("]")

print("\n\nCREATE TABLE:")
print("CREATE TABLE IF NOT EXISTS covid_pipeline.RAW.VACCINATION_COVERAGE (")
for i, col in enumerate(columns):
    comma = "," if i < len(columns) - 1 else ""
    print(f"    {col} VARCHAR{comma}")
print(");")