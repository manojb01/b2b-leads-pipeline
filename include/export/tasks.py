import csv
import io
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

SNOWFLAKE_CONN_ID = "snowflake_default"
S3_BUCKET = "swacchh-leads-pipeline"


def _export_leads_to_csv(execution_date: str) -> str:
    """
    Export leads from Snowflake PROD.LEADS to S3 as CSV.

    Sorted by rating desc, review_count desc — highest quality leads first.
    This is the daily working list for the marketing/sales team.

    Args:
        execution_date: Airflow execution date string (YYYY-MM-DD)

    Returns:
        S3 key of the uploaded CSV file
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    rows = hook.get_records("""
        SELECT
            name,
            area,
            city,
            business_type,
            phone_clean     AS phone,
            rating,
            review_count,
            priority_tier,
            website,
            google_maps_url
        FROM SWACCHH_LEADS.PROD.LEADS
        WHERE has_phone = TRUE
        ORDER BY
            CASE priority_tier WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
            rating DESC,
            review_count DESC
    """)

    columns = [
        'name', 'area', 'city', 'business_type',
        'phone', 'rating', 'review_count', 'priority_tier',
        'website', 'google_maps_url'
    ]

    # Write to in-memory CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(columns)
    writer.writerows(rows)

    s3_key = f"exports/{execution_date}/leads.csv"

    s3_hook = S3Hook(aws_conn_id="aws_s3")
    s3_hook.load_string(
        string_data=output.getvalue(),
        key=s3_key,
        bucket_name=S3_BUCKET,
        replace=True,
    )

    print(f"✅ Exported {len(rows)} leads to s3://{S3_BUCKET}/{s3_key}")
    return s3_key
