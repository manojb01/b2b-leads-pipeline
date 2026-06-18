from soda.scan import Scan
import os


def check(table_name: str, checks_subpath: str, schema: str) -> None:
    """
    Run Soda checks for a given table.

    Args:
        table_name:     The table being checked (used as scan name in logs)
        checks_subpath: Path to the checks file relative to include/soda/checks/
        schema:         Snowflake schema where the table lives (e.g. 'RAW', 'PROD')
    """
    scan = Scan()
    scan.set_scan_definition_name(table_name)
    scan.set_data_source_name('snowflake')

    scan.add_configuration_yaml_str(f"""
        data_source snowflake:
            type: snowflake
            username: {os.environ['SNOWFLAKE_USER']}
            password: {os.environ['SNOWFLAKE_PASSWORD']}
            account:  {os.environ['SNOWFLAKE_ACCOUNT']}
            database: {os.environ['SNOWFLAKE_DATABASE']}
            warehouse: {os.environ['SNOWFLAKE_WAREHOUSE']}
            role:     {os.environ['SNOWFLAKE_ROLE']}
            schema:   {schema}
    """)

    scan.add_sodacl_yaml_file(f'/usr/local/airflow/include/soda/checks/{checks_subpath}')
    scan.execute()

    print(scan.get_logs_text())

    if scan.has_error_logs():
        raise ValueError(f'Soda scan encountered errors for {table_name}')

    if scan.has_check_fails():
        raise ValueError(f'Soda checks FAILED for {table_name} — pipeline stopped')

    print(f'✅ All Soda checks passed for {table_name}')
