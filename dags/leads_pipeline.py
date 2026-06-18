from datetime import datetime, date

from airflow.sdk import dag, task
from airflow.sdk.bases.sensor import PokeReturnValue
from cosmos import DbtTaskGroup, ExecutionConfig, RenderConfig
from cosmos import ExecutionMode, LoadMode

from include.places.tasks import (
    _is_api_available,
    _get_unprocessed_coordinates,
    _fetch_and_save_hexagon,
)
from include.coordinates.generator import get_ameerpet_and_neighbors
from include.snowflake.tasks import _load_to_snowflake
from include.dbt.swacchh.cosmos_config import DBT_PROJECT_CONFIG, DBT_PROFILE_CONFIG
from include.soda.helpers import check
from include.export.tasks import _export_leads_to_csv

DBT_EXECUTION_CONFIG = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
)

DBT_STAGING_RENDER_CONFIG = RenderConfig(
    load_method=LoadMode.DBT_LS,
    select=['path:models/staging'],
)

DBT_PROD_RENDER_CONFIG = RenderConfig(
    load_method=LoadMode.DBT_LS,
    select=['path:models/prod'],
)


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2},
    tags=["swacchh", "leads", "places_api"],
)
def leads_pipeline():

    @task.sensor(poke_interval=30, timeout=300, mode="poke")
    def is_api_available() -> PokeReturnValue:
        return _is_api_available()

    @task()
    def get_unprocessed_coords() -> list[dict]:
        """
        Get coordinates for hexagons not yet processed.
        This drives dynamic task mapping — one task per unprocessed hexagon.
        Returns empty list if all hexagons are done.
        """
        coordinates = get_ameerpet_and_neighbors(rings=1)
        return _get_unprocessed_coordinates(coordinates)

    @task()
    def fetch_and_save(coord: dict) -> list[str]:
        """
        Dynamically mapped task — one instance per unprocessed hexagon.
        Fetches all business types for the hexagon, saves each to S3.
        Runs in parallel across all hexagons.
        """
        return _fetch_and_save_hexagon(coord)

    @task()
    def flatten_s3_keys(s3_keys_per_hexagon: list[list[str]]) -> list[str]:
        """
        Flatten the list of lists from mapped fetch_and_save tasks
        into a single list of S3 keys for load_to_snowflake.
        """
        flat = [key for keys in s3_keys_per_hexagon for key in keys]
        print(f"Total S3 files to load: {len(flat)}")
        return flat

    @task()
    def load_to_snowflake(s3_keys: list[str]) -> None:
        _load_to_snowflake(s3_keys)

    @task()
    def soda_check_raw() -> None:
        check('PLACES', 'raw_places.yml', schema='RAW')

    @task()
    def soda_check_staging() -> None:
        check('STG_PLACES', 'stg_places.yml', schema='STAGING')

    @task()
    def soda_check_leads() -> None:
        check('LEADS', 'leads.yml', schema='PROD')

    @task()
    def export_leads_csv() -> str:
        from airflow.sdk import get_current_context
        ctx = get_current_context()
        execution_date = ctx.get("ds") or date.today().isoformat()
        return _export_leads_to_csv(execution_date)

    dbt_staging = DbtTaskGroup(
        group_id='dbt_staging',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_PROFILE_CONFIG,
        execution_config=DBT_EXECUTION_CONFIG,
        render_config=DBT_STAGING_RENDER_CONFIG,
    )

    dbt_prod = DbtTaskGroup(
        group_id='dbt_prod',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_PROFILE_CONFIG,
        execution_config=DBT_EXECUTION_CONFIG,
        render_config=DBT_PROD_RENDER_CONFIG,
    )

    # Get unprocessed hexagon coordinates
    coords = get_unprocessed_coords()

    # Dynamically map fetch_and_save — one task per hexagon, all parallel
    s3_keys_per_hexagon = fetch_and_save.expand(coord=coords)

    # Flatten results from all hexagon tasks into single list
    s3_keys = flatten_s3_keys(s3_keys_per_hexagon)

    load = load_to_snowflake(s3_keys)

    (
        is_api_available()
        >> coords
        >> s3_keys_per_hexagon
        >> s3_keys
        >> load
        >> soda_check_raw()
        >> dbt_staging
        >> soda_check_staging()
        >> dbt_prod
        >> soda_check_leads()
        >> export_leads_csv()
    )


leads_pipeline()
