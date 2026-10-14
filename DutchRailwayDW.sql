CREATE DATABASE IF NOT EXISTS railway_dw;

USE railway_dw;

-- Dimension Tables

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day_number TINYINT NOT NULL,
    month_number TINYINT NOT NULL,
    month_name VARCHAR(15) NOT NULL,
    quarter_number TINYINT NOT NULL,
    year_number SMALLINT NOT NULL
);

CREATE TABLE dim_station (
    station_key INT AUTO_INCREMENT PRIMARY KEY,
    source_station_id VARCHAR(100),
    station_code VARCHAR(50) UNIQUE,
    station_name VARCHAR(255)
);

CREATE TABLE dim_service (
    service_key INT AUTO_INCREMENT PRIMARY KEY,
    source_service_id VARCHAR(100) UNIQUE,
    service_type VARCHAR(100),
    company VARCHAR(150),
    train_number VARCHAR(50)
);

-- Fact Table

CREATE TABLE fact_station_stop (
    fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_stop_id BIGINT UNIQUE NOT NULL,
    
    date_key INT NOT NULL,
    station_key INT NOT NULL,
    service_key INT NOT NULL,

    arrival_delay INT,
    departure_delay INT,

    arrival_cancelled TINYINT,
    departure_cancelled TINYINT,

    stop_count TINYINT NOT NULL DEFAULT 1,

    FOREIGN KEY (date_key)
        REFERENCES dim_date(date_key),

    FOREIGN KEY (station_key)
        REFERENCES dim_station(station_key),

    FOREIGN KEY (service_key)
        REFERENCES dim_service(service_key)
);

SHOW TABLES;


