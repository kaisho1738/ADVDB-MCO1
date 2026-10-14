# Dutch Railway Data Warehouse ETL

Loads the 2023 Dutch railway services dataset (about 21.2 million stops) into a MySQL star-schema warehouse.

**Pipeline:** raw `services` table → `etl.py` → `dim_date`, `dim_station`, `dim_service`, `fact_station_stop`

## Files

| File | Purpose |
|---|---|
| `etl.py` | The ETL script (extract, transform, load) |
| `warehouse.sql` (the CREATE TABLE script) | Creates the `railway_dw` database and the four warehouse tables |
| `services.sql` (zipped found in the attached gdrive link) | Dump of the raw `services` table (about 3 GB unzipped) |
| Use drive link to download: | services.sql https://drive.google.com/file/d/13dxlelSzBu-MXMng0MiW4azh4V1uFark/view?usp=sharing |

## Requirements

- Python
- MySQL Server, running
- Packages:
  ```
  pip install pandas sqlalchemy pymysql cryptography
  ```

## Setup and run

### 1. Create the warehouse tables

Run the CREATE TABLE script in MySQL Workbench. It creates the `railway_dw` database and the four empty warehouse tables.

### 2. Import the raw data

Unzip `services.sql`, then run this in **Command Prompt** (not PowerShell, which doesn't support `<`):

```
mysql -u root -p railway_dw < services.sql
```

If `mysql` isn't recognized, use the full path, for example:

```
"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe" -u root -p railway_dw < services.sql
```

It prints nothing while it works. It is finished when the prompt returns (a few minutes).

Check it loaded:

```sql
SELECT COUNT(*) FROM railway_dw.services;   -- expect 21,239,393
```

### 3. Run the ETL

In **PowerShell**, from the folder containing `etl.py`:

```
go to directory ->
$env:MYSQL_PASSWORD="your_password"
python etl.py
```
- It prints a progress line every 100k rows and ends with `ETL run finished.`

### 4. Verify

Run these in Workbench:

```sql
USE railway_dw;

SELECT COUNT(*) FROM fact_station_stop;   -- expect 21,239,393
SELECT COUNT(*) FROM dim_date;            -- expect 365
SELECT COUNT(*) FROM dim_station;
SELECT COUNT(*) FROM dim_service;

-- Broken references (expect 0)
SELECT COUNT(*) AS orphaned_facts
FROM fact_station_stop f
LEFT JOIN dim_date d    ON f.date_key = d.date_key
LEFT JOIN dim_station s ON f.station_key = s.station_key
LEFT JOIN dim_service v ON f.service_key = v.service_key
WHERE d.date_key IS NULL OR s.station_key IS NULL OR v.service_key IS NULL;
```
## What the ETL does

- **Extract:** reads the 2023 rows from `services` in 100k-row chunks.
- **Transform:** trims whitespace, blanks become NULL, station codes are uppercased, dates are parsed and the date attributes derived, delays become numbers (bad values become NULL), and cancel flags become 1/0/NULL.
- **Load:** upserts `dim_date`, `dim_station` and `dim_service`, looks up their surrogate keys, then upserts `fact_station_stop`. Each chunk is one transaction, so a failed chunk rolls back.
- **Rejects:** rows missing the service date, service ID, stop ID, or station code are written to `rejected_2023.csv`. In my run, 0 rows were rejected.
- **Duplicates:** repeated `source_stop_id` values within a chunk are dropped, and the unique `source_stop_id` prevents duplicates across reruns.
