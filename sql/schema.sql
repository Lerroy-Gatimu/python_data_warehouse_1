CREATE DATABASE IF NOT EXISTS python_data_warehouse_1 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE python_data_warehouse_1;

CREATE TABLE IF NOT EXISTS dim_location (
  location_id INT NOT NULL PRIMARY KEY,
  city_name VARCHAR(255) NOT NULL,
  country VARCHAR(40) NOT NULL,
  latitude DECIMAL (9,6) NOT NULL,
  longitude DECIMAL (9,6) NOT NULL,
  timezone VARCHAR(60) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_location (latitude, longitude)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id       INT PRIMARY KEY,     -- format: YYYYMMDD (e.g. 20240315)
    full_date     DATE           NOT NULL,
    year          SMALLINT       NOT NULL,
    quarter       TINYINT        NOT NULL,
    month         TINYINT        NOT NULL,
    month_name    VARCHAR(20)    NOT NULL,
    week          TINYINT        NOT NULL,
    day_of_month  TINYINT        NOT NULL,
    day_of_week   TINYINT        NOT NULL,   -- 0=Mon, 6=Sun
    day_name      VARCHAR(20)    NOT NULL,
    is_weekend    BOOLEAN        NOT NULL,
    UNIQUE KEY uq_date (full_date)
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_id       INT PRIMARY KEY,    -- format: HH (0–23)
    hour          TINYINT        NOT NULL,
    time_label    VARCHAR(10)    NOT NULL,   -- e.g. "14:00"
    part_of_day   VARCHAR(20)    NOT NULL    -- Morning/Afternoon/Evening/Night
);

CREATE TABLE IF NOT EXISTS fact_weather_observations (
    observation_id          BIGINT AUTO_INCREMENT PRIMARY KEY,

    -- Foreign keys (dimensions)
    location_id             INT            NOT NULL,
    date_id                 INT            NOT NULL,
    time_id                 INT            NOT NULL,

    -- Measures (the actual numbers we analyse)
    temperature_2m          DECIMAL(5, 2),      -- °C at 2m height
    relative_humidity_2m    TINYINT UNSIGNED,   -- %
    precipitation           DECIMAL(6, 2),      -- mm
    wind_speed_10m          DECIMAL(6, 2),      -- km/h
    wind_direction_10m      SMALLINT UNSIGNED,  -- degrees
    surface_pressure        DECIMAL(7, 2),      -- hPa
    cloud_cover             TINYINT UNSIGNED,   -- %
    weather_code            SMALLINT,           -- WMO code

    -- Audit columns
    loaded_at               TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    source                  VARCHAR(50)    DEFAULT 'open-meteo',

    -- Prevent duplicate loads (idempotency)
    UNIQUE KEY uq_observation (location_id, date_id, time_id),

    CONSTRAINT fk_obs_location FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    CONSTRAINT fk_obs_date     FOREIGN KEY (date_id)     REFERENCES dim_date(date_id),
    CONSTRAINT fk_obs_time     FOREIGN KEY (time_id)     REFERENCES dim_time(time_id)
);