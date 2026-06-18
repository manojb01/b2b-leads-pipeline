import logging

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Suppress Snowflake connector's verbose SQL statement logging
# Still captures WARNING and ERROR — errors will still surface
logging.getLogger('snowflake').setLevel(logging.WARNING)

# --- Snowflake Config ---
SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_RAW_SCHEMA = "RAW"
SNOWFLAKE_RAW_TABLE = "PLACES"
SNOWFLAKE_STAGING_TABLE = "PLACES_STAGING"

# Snowflake external stage — points to s3://swacchh-leads-pipeline/
# Uses IAM role via storage integration — no credentials in SQL
SNOWFLAKE_S3_STAGE = "SWACCHH_LEADS.RAW.swacchh_s3_stage"


def _create_raw_schema_and_tables():
    """
    Creates the RAW schema and both tables if they don't exist.

    PLACES_STAGING  — temporary landing zone, truncated each run
    PLACES          — permanent store, one row per place_id (latest data)
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    hook.run(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_RAW_SCHEMA}")

    # Staging — truncated each run, holds current API response
    hook.run(f"""
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_RAW_SCHEMA}.{SNOWFLAKE_STAGING_TABLE} (
            raw_data    VARIANT,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Main — permanent store, one row per place_id
    hook.run(f"""
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_RAW_SCHEMA}.{SNOWFLAKE_RAW_TABLE} (
            raw_data    VARIANT,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    print("✅ Schema and tables ready")


def _load_to_snowflake(s3_keys: list[str]):
    """
    Load data from S3 into Snowflake using COPY INTO — bulk load pattern.

    S3 is the source of truth (bronze layer). Snowflake reads directly
    from each S3 file via IAM role — no credentials in code or SQL.

    Flow:
        S3 files (one per hexagon+business_type) → COPY INTO staging → MERGE into main table

    Step 1 — Truncate staging
    Step 2 — COPY INTO staging from each new S3 file (IAM role auth, no credentials)
    Step 3 — MERGE staging into main table on place_id:
             - place exists → UPDATE
             - place is new → INSERT

    This means:
    - Only new hexagon files are loaded — no reprocessing
    - No credentials in SQL or logs — IAM role handles auth
    - Idempotent — reruns safe, MERGE handles duplicates

    Args:
        s3_keys: List of S3 keys to load (output of fetch_and_save task)
    """
    if not s3_keys:
        print("No new S3 files to load. All hexagons already processed.")
        return

    _create_raw_schema_and_tables()

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    # Step 1 — Truncate staging
    hook.run(f"TRUNCATE TABLE {SNOWFLAKE_RAW_SCHEMA}.{SNOWFLAKE_STAGING_TABLE}")
    print("✅ Staging table truncated")

    # Step 2 — COPY INTO staging from all new S3 files in one statement
    # FILES = (...) lets Snowflake load all files in parallel internally —
    # one round-trip instead of N sequential COPY INTO calls.
    # STRIP_OUTER_ARRAY = TRUE — JSON file is an array [...], each element becomes one row
    files_list = ", ".join(f"'{key}'" for key in s3_keys)
    hook.run(f"""
        COPY INTO {SNOWFLAKE_RAW_SCHEMA}.{SNOWFLAKE_STAGING_TABLE} (raw_data)
        FROM @{SNOWFLAKE_S3_STAGE}
        FILES = ({files_list})
        FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = TRUE)
    """)
    print(f"✅ Loaded {len(s3_keys)} files into staging")

    # Step 3 — Merge staging into main table on place_id
    # Staging may have duplicate place_ids from overlapping hexagons
    # QUALIFY deduplicates source before merge to avoid Snowflake MERGE error
    hook.run(f"""
        MERGE INTO {SNOWFLAKE_RAW_SCHEMA}.{SNOWFLAKE_RAW_TABLE} AS target
        USING (
            SELECT raw_data
            FROM {SNOWFLAKE_RAW_SCHEMA}.{SNOWFLAKE_STAGING_TABLE}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY raw_data:id::varchar
                ORDER BY raw_data:id::varchar
            ) = 1
        ) AS source
        ON target.raw_data:id::varchar = source.raw_data:id::varchar
        WHEN MATCHED THEN
            UPDATE SET
                raw_data    = source.raw_data,
                ingested_at = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (raw_data, ingested_at)
            VALUES (source.raw_data, CURRENT_TIMESTAMP)
    """)
    print(f"✅ Merged staging into {SNOWFLAKE_RAW_SCHEMA}.{SNOWFLAKE_RAW_TABLE}")
