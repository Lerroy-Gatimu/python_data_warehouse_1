# python_data_warehouse_1

A production-grade **ETL pipeline and data warehouse** built with Python, MySQL, and the Open-Meteo REST API. Ingests 3 months of hourly weather observations for 4 global cities, transforms them into a dimensional star schema, and loads them into MySQL with fully idempotent upserts.

---

## Overview

| Item | Detail |
|---|---|
| **Pipeline type** | Extract → Transform → Load (ETL) |
| **Data source** | [Open-Meteo Historical Weather API](https://open-meteo.com/) (free, no key required) |
| **Warehouse model** | Star schema (1 fact table, 3 dimension tables) |
| **Rows loaded** | ~8,736 hourly fact rows per full run |
| **Cities tracked** | Nairobi · London · New York · Tokyo |
| **Date range** | Jan 2024 – Mar 2024 (configurable) |

---

## Tech Stack

- **Python 3.x** — core pipeline language
- **MySQL 8** — data warehouse storage
- **SQLAlchemy 2.0** — ORM, connection pooling, upserts
- **PyMySQL** — MySQL driver
- **Pandas** — data transformation
- **Requests** — HTTP API client
- **python-dotenv** — environment-based config

---

## Project Structure

```
python_data_warehouse_1/
├── src/
│   ├── extract/
│   │   └── api_client.py       # Calls Open-Meteo API
│   ├── transform/
│   │   └── transformer.py      # Cleans & reshapes raw data
│   ├── load/
│   │   └── loader.py           # Writes to MySQL (batched upserts)
│   └── utils/
│       ├── db.py               # SQLAlchemy engine & session
│       └── logger.py           # Structured logging
├── sql/
│   └── schema.sql              # DDL — all table definitions
├── logs/                       # Timestamped daily log files
├── main.py                     # Orchestrator — runs the full pipeline
├── config.py                   # Reads .env and exposes settings
├── .env                        # Credentials (never committed)
├── .env.example                # Template for new contributors
└── requirements.txt
```

---

## Data Warehouse Schema

Star schema with a central fact table and three surrounding dimensions:

```
dim_location ──┐
dim_date     ──┼──► fact_weather_observations
dim_time     ──┘
```

### dim_location
One row per city — city name, country, latitude, longitude, timezone.

### dim_date
One row per calendar day with derived attributes: year, quarter, month, week number, day name, is_weekend flag.

### dim_time
24 rows (one per hour) with time label and part-of-day (Morning / Afternoon / Evening / Night).

### fact_weather_observations
One row per hourly reading at one location:

| Column | Type | Description |
|---|---|---|
| `temperature_2m` | DECIMAL | °C at 2m height |
| `relative_humidity_2m` | TINYINT | % |
| `precipitation` | DECIMAL | mm |
| `wind_speed_10m` | DECIMAL | km/h |
| `wind_direction_10m` | SMALLINT | degrees |
| `surface_pressure` | DECIMAL | hPa |
| `cloud_cover` | TINYINT | % |
| `weather_code` | SMALLINT | WMO code |

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/lerrize/python_data_warehouse_1.git
cd python_data_warehouse_1
```

### 2. Create a virtual environment

```bash
python -m venv venv
source venv/bin/activate        # Linux / macOS
venv\Scripts\activate           # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment

```bash
cp .env.example .env
```

Edit `.env` with your MySQL credentials:

```env
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_password_here
DB_NAME=python_data_warehouse_1
```

### 5. Create the MySQL database and tables

```bash
mysql -u root -p < sql/schema.sql
```

### 6. Run the pipeline

```bash
python main.py
```

---

## Configuration

Edit `config.py` to change locations or the date range:

```python
LOCATIONS = [
    {"city": "Nairobi", "country": "Kenya", "lat": -1.2921, "lon": 36.8219, "tz": "Africa/Nairobi"},
    # Add more cities here
]

EXTRACT_START_DATE = "2024-01-01"
EXTRACT_END_DATE   = "2024-03-31"
```

---

## Sample Analytics Queries

```sql
-- Average monthly temperature per city
SELECT l.city_name, d.month_name,
       ROUND(AVG(f.temperature_2m), 1) AS avg_temp_c
FROM fact_weather_observations f
JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_date     d ON f.date_id     = d.date_id
GROUP BY l.city_name, d.month, d.month_name
ORDER BY l.city_name, d.month;

-- Total rainfall per city per month
SELECT l.city_name, d.month_name,
       ROUND(SUM(f.precipitation), 1) AS total_rainfall_mm
FROM fact_weather_observations f
JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_date     d ON f.date_id     = d.date_id
GROUP BY l.city_name, d.month, d.month_name
ORDER BY l.city_name, d.month;
```

---

## Engineering Highlights

- **Idempotent loads** — `ON DUPLICATE KEY UPDATE` prevents duplicate rows on repeated runs
- **Connection pooling** — SQLAlchemy engine with `pool_size`, `pool_recycle`, and `pool_pre_ping`
- **Batched inserts** — fact rows written in batches of 500 to avoid MySQL packet limits
- **Graceful failure handling** — a failed API call for one city is logged and skipped; the pipeline continues
- **Modular ETL layers** — extract, transform, and load are fully decoupled; easy to extend or swap components
- **Structured logging** — timestamped output to console and daily log files in `logs/`

---

## Environment Template

Create a `.env.example` file (safe to commit — no real credentials):

```env
DB_HOST=localhost
DB_PORT=3306
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_NAME=python_data_warehouse_1
```

---

## License

MIT