"""
python_data_warehouse_1 — ETL Pipeline Orchestrator

"""

import sys
from config import LOCATIONS, EXTRACT_START_DATE, EXTRACT_END_DATE
from src.utils.db import test_connection
from src.utils.logger import get_logger
from src.extract.api_client import fetch_weather
from src.transform.transformer import (
    build_dim_date_records,
    build_dim_time_records,
    transform_weather_data,
)
from src.load.loader import (
    upsert_location,
    load_dim_date,
    load_dim_time,
    load_fact_weather,
)

logger = get_logger("main")


def run_pipeline():
    logger.info("=" * 60)
    logger.info("python_data_warehouse_1 — ETL Pipeline Starting")
    logger.info(f"Date range: {EXTRACT_START_DATE} → {EXTRACT_END_DATE}")
    logger.info(f"Locations:  {[loc['city'] for loc in LOCATIONS]}")
    logger.info("=" * 60)

    # Step 1: Verify DB connection 
    if not test_connection():
        logger.error("Cannot connect to database. Aborting.")
        sys.exit(1)

    # Step 2: Load dimension tables (run once) 
    logger.info("\n[PHASE 1] Loading dimension tables...")

    date_records = build_dim_date_records(EXTRACT_START_DATE, EXTRACT_END_DATE)
    load_dim_date(date_records)

    time_records = build_dim_time_records()
    load_dim_time(time_records)

    # Step 3: ETL per location 
    logger.info("\n[PHASE 2] Running ETL per location...")

    total_facts = 0
    failed_locations = []

    for loc in LOCATIONS:
        city = loc["city"]
        logger.info(f"\n── Processing: {city} ──")

        # 3a. Upsert location into dim_location, get its ID
        location_record = {
            "city_name":  city,
            "country":    loc["country"],
            "latitude":   loc["lat"],
            "longitude":  loc["lon"],
            "timezone":   loc["tz"],
        }
        location_id = upsert_location(location_record)
        logger.info(f"  location_id = {location_id}")

        # 3b. Extract from API
        raw_data = fetch_weather(
            latitude=loc["lat"],
            longitude=loc["lon"],
            start_date=EXTRACT_START_DATE,
            end_date=EXTRACT_END_DATE,
            timezone=loc["tz"],
            city=city,
        )

        if raw_data is None:
            logger.warning(f"  Skipping {city} — API extract failed.")
            failed_locations.append(city)
            continue   # Don't crash the whole pipeline for one bad location

        # 3c. Transform
        fact_records = transform_weather_data(raw_data, location_id, city)

        if fact_records is None:
            logger.warning(f"  Skipping {city} — transform failed.")
            failed_locations.append(city)
            continue

        # 3d. Load
        count = load_fact_weather(fact_records, city)
        total_facts += count

    # Step 4: Summary 
    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info(f"  Total fact rows loaded : {total_facts:,}")
    logger.info(f"  Successful locations   : {len(LOCATIONS) - len(failed_locations)}/{len(LOCATIONS)}")
    if failed_locations:
        logger.warning(f"  Failed locations       : {failed_locations}")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()