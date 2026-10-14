
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.engine import URL


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

CSV_PATH = "services-2023.csv"

YEAR = 2023
CHUNK_SIZE = 100_000
INSERT_BATCH_SIZE = 2_000
REJECTED_FILE = Path(f"rejected_{YEAR}.csv")


DB_URL = URL.create(
    drivername="mysql+pymysql",
    username=os.getenv("MYSQL_USER", "root"),
    password=os.getenv("MYSQL_PASSWORD", ""),
    host=os.getenv("MYSQL_HOST", "localhost"),
    port=int(os.getenv("MYSQL_PORT", "3306")),
    database="railway_dw",
)

engine = create_engine(DB_URL, pool_pre_ping=True)


# --------------------------------------------------
# EXTRACT
# --------------------------------------------------

SOURCE_QUERY = text("""
    SELECT
        CAST(`Service:RDT-ID` AS CHAR) AS source_service_id,
        `Service:Date` AS service_date,
        `Service:Type` AS service_type,
        `Service:Company` AS company,
        CAST(`Service:Train number` AS CHAR) AS train_number,

        CAST(`Stop:RDT-ID` AS CHAR) AS source_stop_id,
        `Stop:Station code` AS station_code,
        `Stop:Station name` AS station_name,

        `Stop:Arrival delay` AS arrival_delay,
        `Stop:Departure delay` AS departure_delay,
        `Stop:Arrival cancelled` AS arrival_cancelled,
        `Stop:Departure cancelled` AS departure_cancelled

    FROM services
    WHERE `Service:Date` >= :start_date
      AND `Service:Date` < :end_date
""")


def extract_chunks():
    """Stream the selected year from the raw staging table."""
    with engine.connect().execution_options(stream_results=True) as conn:
        chunks = pd.read_sql_query(
            SOURCE_QUERY,
            conn,
            params={
                "start_date": f"{YEAR}-01-01",
                "end_date": f"{YEAR + 1}-01-01",
            },
            chunksize=CHUNK_SIZE,
        )

        for chunk in chunks:
            yield chunk


# --------------------------------------------------
# TRANSFORM
# --------------------------------------------------

def normalize_flag(value):
    """Convert known Boolean representations to 1, 0, or NULL."""
    if pd.isna(value):
        return None

    if isinstance(value, str):
        value = value.strip().lower()

        if value in {"true", "1", "yes"}:
            return 1
        if value in {"false", "0", "no"}:
            return 0

        return None

    if value is True or value == 1:
        return 1

    if value is False or value == 0:
        return 0

    return None


def first_non_null(series):
    """Select the first available value in a group."""
    for value in series:
        if not pd.isna(value):
            return value

    return None


def transform_chunk(raw):
    """Clean records and derive the date-dimension attributes."""
    df = raw.copy()

    # Standardize strings and preserve missing values.
    string_columns = [
        "source_service_id",
        "source_stop_id",
        "train_number",
        "service_type",
        "company",
        "station_code",
        "station_name",
    ]

    for col in string_columns:
        df[col] = df[col].astype("string").str.strip()
        df[col] = df[col].replace("", pd.NA)

    # Station codes are identifiers, so standardize their case.
    df["station_code"] = df["station_code"].str.upper()

    # Collapse repeated whitespace in descriptive text.
    for col in ["service_type", "company", "station_name"]:
        df[col] = df[col].str.replace(r"\s+", " ", regex=True)

    # Parse service dates and derive date attributes.
    parsed_dates = pd.to_datetime(
        df["service_date"], errors="coerce"
    )

    df["service_date"] = parsed_dates.dt.date
    df["day_number"] = parsed_dates.dt.day
    df["month_number"] = parsed_dates.dt.month
    df["month_name"] = parsed_dates.dt.month_name()
    df["quarter_number"] = parsed_dates.dt.quarter
    df["year_number"] = parsed_dates.dt.year

    # Invalid delay values become NULL, not zero.
    for col in ["arrival_delay", "departure_delay"]:
        df[col] = pd.to_numeric(
            df[col], errors="coerce"
        ).astype("Int64")

    for col in ["arrival_cancelled", "departure_cancelled"]:
        df[col] = df[col].map(normalize_flag)

    # Reject records without essential identifiers.
    valid = (
        df["service_date"].notna()
        & df["source_service_id"].notna()
        & df["source_stop_id"].notna()
        & df["station_code"].notna()
    )

    rejected = df.loc[~valid].copy()
    clean = df.loc[valid].copy()

    # Remove repeated source-stop IDs within this chunk.
    clean = clean.drop_duplicates(
        subset=["source_stop_id"],
        keep="first",
    )

    return clean, rejected


# --------------------------------------------------
# HELPERS FOR LOADING DATA
# --------------------------------------------------

def execute_many(conn, statement, records):
    """Execute batched parameterized inserts."""
    for start in range(0, len(records), INSERT_BATCH_SIZE):
        batch = records[start:start + INSERT_BATCH_SIZE]
        if batch:
            conn.execute(statement, batch)


def fetch_key_map(conn, table, natural_key, surrogate_key, keys):
    """Retrieve natural-key -> surrogate-key mappings in batches."""
    keys = list(dict.fromkeys(keys))
    result = {}

    if not keys:
        return result

    # Table and column names passed here are fixed internal constants.
    statement = text(
        f"SELECT `{natural_key}`, `{surrogate_key}` "
        f"FROM `{table}` WHERE `{natural_key}` IN :keys"
    ).bindparams(bindparam("keys", expanding=True))

    for start in range(0, len(keys), 1_000):
        batch = keys[start:start + 1_000]

        rows = conn.execute(
            statement, {"keys": batch}
        ).all()

        result.update({key: value for key, value in rows})

    return result


def to_python_value(value):
    """Convert pandas missing values and scalars for MySQL."""
    if pd.isna(value):
        return None

    if hasattr(value, "item"):
        return value.item()

    return value


# --------------------------------------------------
# LOAD DIM_DATE
# --------------------------------------------------

DATE_UPSERT = text("""
    INSERT INTO dim_date (
        date_key, full_date, day_number, month_number,
        month_name, quarter_number, year_number
    )
    VALUES (
        :date_key, :full_date, :day_number, :month_number,
        :month_name, :quarter_number, :year_number
    )
    ON DUPLICATE KEY UPDATE
        day_number = VALUES(day_number),
        month_number = VALUES(month_number),
        month_name = VALUES(month_name),
        quarter_number = VALUES(quarter_number),
        year_number = VALUES(year_number)
""")


def load_dates(conn, clean):
    dates = clean["service_date"].drop_duplicates().tolist()
    records = []

    for d in dates:
        records.append({
            # Deterministic date key: YYYYMMDD.
            "date_key": int(d.strftime("%Y%m%d")),
            "full_date": d,
            "day_number": d.day,
            "month_number": d.month,
            "month_name": d.strftime("%B"),
            "quarter_number": (d.month - 1) // 3 + 1,
            "year_number": d.year,
        })

    execute_many(conn, DATE_UPSERT, records)


# --------------------------------------------------
# LOAD DIM_STATION
# --------------------------------------------------

STATION_UPSERT = text("""
    INSERT INTO dim_station (station_code, station_name)
    VALUES (:station_code, :station_name)
    ON DUPLICATE KEY UPDATE
        station_name = COALESCE(
            VALUES(station_name), dim_station.station_name
        )
""")


def load_stations(conn, clean):
    distinct = clean[
        ["station_code", "station_name"]
    ].drop_duplicates()

    # Choose the first non-missing name per station code.
    grouped = distinct.groupby("station_code", sort=False)

    records = []
    for code, group in grouped:
        records.append({
            "station_code": code,
            "station_name": first_non_null(group["station_name"]),
        })

    execute_many(conn, STATION_UPSERT, records)

    return fetch_key_map(
        conn,
        "dim_station",
        "station_code",
        "station_key",
        [r["station_code"] for r in records],
    )


# --------------------------------------------------
# LOAD DIM_SERVICE
# --------------------------------------------------

SERVICE_UPSERT = text("""
    INSERT INTO dim_service (
        source_service_id, service_type, company, train_number
    )
    VALUES (
        :source_service_id, :service_type, :company, :train_number
    )
    ON DUPLICATE KEY UPDATE
        service_type = COALESCE(
            dim_service.service_type, VALUES(service_type)
        ),
        company = COALESCE(
            dim_service.company, VALUES(company)
        ),
        train_number = COALESCE(
            dim_service.train_number, VALUES(train_number)
        )
""")


def load_services(conn, clean):
    records = []

    for service_id, group in clean.groupby(
        "source_service_id", sort=False
    ):
        records.append({
            "source_service_id": service_id,
            "service_type": first_non_null(group["service_type"]),
            "company": first_non_null(group["company"]),
            "train_number": first_non_null(group["train_number"]),
        })

    execute_many(conn, SERVICE_UPSERT, records)

    return fetch_key_map(
        conn,
        "dim_service",
        "source_service_id",
        "service_key",
        [r["source_service_id"] for r in records],
    )


# --------------------------------------------------
# LOAD FACT_STATION_STOP
# --------------------------------------------------

FACT_UPSERT = text("""
    INSERT INTO fact_station_stop (
        source_stop_id,
        date_key,
        station_key,
        service_key,
        arrival_delay,
        departure_delay,
        arrival_cancelled,
        departure_cancelled,
        stop_count
    )
    VALUES (
        :source_stop_id,
        :date_key,
        :station_key,
        :service_key,
        :arrival_delay,
        :departure_delay,
        :arrival_cancelled,
        :departure_cancelled,
        1
    )
    ON DUPLICATE KEY UPDATE
        date_key = VALUES(date_key),
        station_key = VALUES(station_key),
        service_key = VALUES(service_key),
        arrival_delay = VALUES(arrival_delay),
        departure_delay = VALUES(departure_delay),
        arrival_cancelled = VALUES(arrival_cancelled),
        departure_cancelled = VALUES(departure_cancelled),
        stop_count = 1
""")


def load_facts(conn, clean, station_map, service_map):
    records = []

    for row in clean.itertuples(index=False):
        station_key = station_map.get(row.station_code)
        service_key = service_map.get(row.source_service_id)

        if station_key is None or service_key is None:
            raise ValueError(
                "Missing dimension key while loading fact for "
                f"stop {row.source_stop_id}"
            )

        records.append({
            "source_stop_id": int(row.source_stop_id),
            "date_key": int(row.service_date.strftime("%Y%m%d")),
            "station_key": station_key,
            "service_key": service_key,
            "arrival_delay": to_python_value(row.arrival_delay),
            "departure_delay": to_python_value(row.departure_delay),
            "arrival_cancelled": to_python_value(
                row.arrival_cancelled
            ),
            "departure_cancelled": to_python_value(
                row.departure_cancelled
            ),
        })

    execute_many(conn, FACT_UPSERT, records)


# --------------------------------------------------
# PIPELINE ORCHESTRATION
# --------------------------------------------------

def run_etl():
    rows_read = 0
    rows_processed = 0
    rows_rejected = 0

    write_reject_header = not REJECTED_FILE.exists()

    for raw_chunk in extract_chunks():
        rows_read += len(raw_chunk)

        clean, rejected = transform_chunk(raw_chunk)
        rows_rejected += len(rejected)

        # Retain rejected rows for inspection and correction.
        if not rejected.empty:
            rejected.to_csv(
                REJECTED_FILE,
                mode="a",
                index=False,
                header=write_reject_header,
            )
            write_reject_header = False

        if not clean.empty:
            # Dimensions and facts for a chunk are committed together.
            # A failed load rolls back that chunk.
            with engine.begin() as conn:
                load_dates(conn, clean)
                station_map = load_stations(conn, clean)
                service_map = load_services(conn, clean)
                load_facts(
                    conn, clean, station_map, service_map
                )

            rows_processed += len(clean)

        print(
            f"Read: {rows_read:,} | "
            f"Processed: {rows_processed:,} | "
            f"Rejected: {rows_rejected:,}"
        )

    print("\nETL run finished.")
    print(f"Rows read:      {rows_read:,}")
    print(f"Rows processed: {rows_processed:,}")
    print(f"Rows rejected:  {rows_rejected:,}")


if __name__ == "__main__":
    try:
        run_etl()
    finally:
        engine.dispose()
