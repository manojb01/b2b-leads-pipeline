from pathlib import Path
from cosmos import ProfileConfig
from cosmos.config import ProjectConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path=Path('/usr/local/airflow/include/dbt/swacchh'),
)

DBT_PROFILE_CONFIG = ProfileConfig(
    profile_name='swacchh',
    target_name='dev',
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id='snowflake_default',
        profile_args={
            'database': 'SWACCHH_LEADS',
            'schema': 'raw',
        }
    )
)
