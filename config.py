import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = int(os.getenv("DB_PORT", 3306))
DB_USER     = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME     = os.getenv("DB_NAME", "python_data_warehouse_1")

DATABASE_URL = (
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
)

 
LOCATIONS = [
    {"city": "Nairobi",    "country": "Kenya",         "lat": -1.2921,  "lon": 36.8219, "tz": "Africa/Nairobi"},
    {"city": "London",     "country": "United Kingdom","lat": 51.5074,  "lon": -0.1278, "tz": "Europe/London"},
    {"city": "New York",   "country": "United States", "lat": 40.7128,  "lon": -74.0060,"tz": "America/New_York"},
    {"city": "Tokyo",      "country": "Japan",         "lat": 35.6762,  "lon": 139.6503,"tz": "Asia/Tokyo"},
]

EXTRACT_START_DATE = "2024-01-01"
EXTRACT_END_DATE   = "2024-03-31"  