import pandas as pd
from datetime import datetime
from typing import Optional
from src.utils.logger import get_logger

logger = get_logger("transform.transformer")


def build_dim_date_records(start_date: str, end_date: str) -> list[dict]:
    """
    Pre-generates all calendar day rows between two dates.
    This is standard practice — load the date dimension once,
    then all fact rows just reference it by integer ID.
    """
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")
    records = []

    day_names   = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    month_names = ["","January","February","March","April","May","June",
                   "July","August","September","October","November","December"]

    for d in date_range:
        records.append({
            "date_id":      int(d.strftime("%Y%m%d")),
            "full_date":    d.date(),
            "year":         d.year,
            "quarter":      d.quarter,
            "month":        d.month,
            "month_name":   month_names[d.month],
            "week":         int(d.isocalendar().week),
            "day_of_month": d.day,
            "day_of_week":  d.dayofweek,
            "day_name":     day_names[d.dayofweek],
            "is_weekend":   d.dayofweek >= 5,
        })

    logger.info(f"dim_date: generated {len(records)} date records")
    return records


def build_dim_time_records() -> list[dict]:
    """
    Generates 24 rows — one per hour of the day.
    """
    records = []
    for hour in range(24):
        if hour < 6:
            part = "Night"
        elif hour < 12:
            part = "Morning"
        elif hour < 18:
            part = "Afternoon"
        else:
            part = "Evening"

        records.append({
            "time_id":    hour,
            "hour":       hour,
            "time_label": f"{hour:02d}:00",
            "part_of_day": part,
        })

    logger.info(f"dim_time: generated {len(records)} time records")
    return records


def transform_weather_data(
    raw_data: dict,
    location_id: int,
    city: str
) -> Optional[list[dict]]:
    """
    Converts raw Open-Meteo API response into a list of fact table rows.

    Each row in the API's hourly arrays maps to one fact row.
    We extract the date_id and time_id from each timestamp.

    Args:
        raw_data:    Raw JSON from the API
        location_id: The location_id from dim_location
        city:        For logging

    Returns:
        List of dicts, each representing one row in fact_weather_observations
    """
    try:
        hourly = raw_data.get("hourly", {})
        times  = hourly.get("time", [])

        if not times:
            logger.warning(f"{city}: no hourly data in API response")
            return None

        records = []
        skipped = 0

        for i, time_str in enumerate(times):
            # time_str looks like "2024-01-15T14:00"
            dt = datetime.fromisoformat(time_str)

            # Get each measure, using .get() so missing fields don't crash us
            temp     = hourly.get("temperature_2m", [])[i]
            humidity = hourly.get("relative_humidity_2m", [])[i]
            precip   = hourly.get("precipitation", [])[i]
            wind_spd = hourly.get("wind_speed_10m", [])[i]
            wind_dir = hourly.get("wind_direction_10m", [])[i]
            pressure = hourly.get("surface_pressure", [])[i]
            cloud    = hourly.get("cloud_cover", [])[i]
            w_code   = hourly.get("weather_code", [])[i]

            # Skip rows where ALL measures are None (missing data from API)
            if all(v is None for v in [temp, humidity, precip, wind_spd]):
                skipped += 1
                continue

            records.append({
                "location_id":          location_id,
                "date_id":              int(dt.strftime("%Y%m%d")),
                "time_id":              dt.hour,
                "temperature_2m":       temp,
                "relative_humidity_2m": humidity,
                "precipitation":        precip,
                "wind_speed_10m":       wind_spd,
                "wind_direction_10m":   wind_dir,
                "surface_pressure":     pressure,
                "cloud_cover":          cloud,
                "weather_code":         w_code,
                "source":               "open-meteo",
            })

        logger.info(f"  {city}: transformed {len(records)} records (skipped {skipped} empty rows)")
        return records

    except Exception as e:
        logger.error(f"{city}: transform failed — {e}")
        return None