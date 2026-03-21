import requests
from typing import Optional
from src.utils.logger import get_logger

logger = get_logger("extract.api_client")

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

# The exact fields we want from the API
HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
    "surface_pressure",
    "cloud_cover",
    "weather_code",
]

def fetch_weather(
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
    timezone: str,
    city: str = ""
) -> Optional[dict]:
    """
    Calls the Open-Meteo Historical Weather API and returns raw JSON.

    Args:
        latitude:   Location latitude
        longitude:  Location longitude
        start_date: 'YYYY-MM-DD' format
        end_date:   'YYYY-MM-DD' format
        timezone:   IANA timezone string e.g. 'Africa/Nairobi'
        city:       For logging only

    Returns:
        Raw API response dict, or None on failure.
    """
    params = {
        "latitude":  latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date":   end_date,
        "hourly":     ",".join(HOURLY_VARIABLES),
        "timezone":   timezone,
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
    }

    logger.info(f"Fetching data for {city} ({start_date} → {end_date})")

    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()   # Raises HTTPError for 4xx/5xx
        data = response.json()
        logger.info(f"  ✓ {city}: received {len(data.get('hourly', {}).get('time', []))} hourly records")
        return data

    except requests.exceptions.Timeout:
        logger.error(f"  ✗ {city}: request timed out")
    except requests.exceptions.HTTPError as e:
        logger.error(f"  ✗ {city}: HTTP error {e.response.status_code} — {e}")
    except requests.exceptions.RequestException as e:
        logger.error(f"  ✗ {city}: network error — {e}")

    return None