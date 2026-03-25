from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy import Table, MetaData
from src.utils.db import get_session, engine
from src.utils.logger import get_logger

logger = get_logger("load.loader")

# Reflect the existing tables from MySQL so SQLAlchemy knows their structure
metadata = MetaData()
metadata.reflect(bind=engine)


def _get_table(name: str) -> Table:
    return metadata.tables[name]


def upsert_location(location: dict) -> int:
    tbl  = _get_table("dim_location")
    stmt = (
        mysql_insert(tbl)
        .values(**location)
        .on_duplicate_key_update(city_name=location["city_name"])
    )

    with get_session() as session:
        result = session.execute(stmt)
        session.commit()

        # If it was a new insert, lastrowid gives us the new ID
        if result.lastrowid:
            return result.lastrowid

        # Otherwise query for the existing ID
        row = session.execute(
            tbl.select().where(
                tbl.c.latitude  == location["latitude"],
                tbl.c.longitude == location["longitude"]
            )
        ).fetchone()
        return row.location_id


def load_dim_date(records: list[dict]) -> int:
    
    if not records:
        return 0

    tbl  = _get_table("dim_date")
    stmt = (
        mysql_insert(tbl)
        .values(records)
        .on_duplicate_key_update(full_date=tbl.c.full_date)  # no-op update = ignore dup
    )

    with get_session() as session:
        session.execute(stmt)
        session.commit()

    logger.info(f"dim_date: loaded {len(records)} records")
    return len(records)


def load_dim_time(records: list[dict]) -> int:
   
    if not records:
        return 0

    tbl  = _get_table("dim_time")
    stmt = (
        mysql_insert(tbl)
        .values(records)
        .on_duplicate_key_update(hour=tbl.c.hour)
    )

    with get_session() as session:
        session.execute(stmt)
        session.commit()

    logger.info(f"dim_time: loaded {len(records)} records")
    return len(records)


def load_fact_weather(records: list[dict], city: str, batch_size: int = 500) -> int:
    if not records:
        return 0

    tbl = _get_table("fact_weather_observations")
    total = 0

    with get_session() as session:
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            stmt = (
              mysql_insert(tbl)
              .values(batch)
              .on_duplicate_key_update(
               temperature_2m=tbl.c.temperature_2m,
               relative_humidity_2m=tbl.c.relative_humidity_2m,
              precipitation=tbl.c.precipitation,
               wind_speed_10m=tbl.c.wind_speed_10m,
                 )
              )
                
            
            session.execute(stmt)
            total += len(batch)

        session.commit()

    logger.info(f"  {city}: loaded {total} fact rows")
    return total