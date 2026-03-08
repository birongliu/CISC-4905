from pathlib import Path

from airflow.exceptions import AirflowSkipException
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.sdk import TaskGroup, task

from databricks_reporter import WorkbookReporter


DEFAULT_DATABRICKS_CONN_ID = "databricks_conn"
DEFAULT_DATABRICKS_FETCH_LIMIT = 500
DATABRICKS_SOURCE_TABLE = "workspace.flights.ingest_flights"
GROUP_DATABRICKS_OUTPUT_PATHS = {
    "current_week_workbook": "/opt/airflow/data/flight_data_weekday.csv",
    "next_week_workbook": "/opt/airflow/data/flight_data_weekday.csv",
    "next2_week_workbook": "/opt/airflow/data/flight_data_weekday.csv",
    "current_weekend_workbook": "/opt/airflow/data/flight_data_weekend.csv",
    "next_weekend_workbook": "/opt/airflow/data/flight_data_weekend.csv",
}
WEEKEND_GROUP_IDS = {
    "current_weekend_workbook",
    "next_weekend_workbook",
}


def _build_databricks_query(group_id, fetch_limit):
    safe_limit = max(fetch_limit, 1)
    day_filter = "dayofweek(time_position) IN (1, 7)" if group_id in WEEKEND_GROUP_IDS else "dayofweek(time_position) BETWEEN 2 AND 6"

    return f"""
SELECT
    time_ingest,
    icao24,
    callsign,
    origin_country,
    time_position,
    last_contact,
    longitude,
    latitude,
    geo_altitude,
    on_ground,
    velocity,
    category
FROM {DATABRICKS_SOURCE_TABLE}
WHERE {day_filter}
ORDER BY time_position DESC
LIMIT {safe_limit}
""".strip()

class WorkbookGroup(TaskGroup):

    def __init__(self, metadata, **kwargs):
        super().__init__(**kwargs)
        self.metadata = metadata

        if self:

            @task(task_group=self)
            def should_run(metadata, group_id):
                run_groups = metadata.get("run_groups", [])
                if group_id not in run_groups:
                    print(f"Skipping {group_id} for weekday {metadata.get('weekday')}")
                    raise AirflowSkipException("WorkbookGroup not scheduled to run")
                print(f"Running {group_id} with metadata {metadata}")
                return True

            @task(task_group=self)
            def generate_data_from_databricks(metadata, group_id):
                try:
                    fetch_limit = int(metadata.get("databricks_fetch_limit", DEFAULT_DATABRICKS_FETCH_LIMIT))
                except (TypeError, ValueError):
                    fetch_limit = DEFAULT_DATABRICKS_FETCH_LIMIT

                databricks_conn_id = metadata.get("databricks_conn_id", DEFAULT_DATABRICKS_CONN_ID)

                output_path = (
                    metadata.get("databricks_output_path")
                    or GROUP_DATABRICKS_OUTPUT_PATHS.get(group_id)
                    or f"/opt/airflow/data/{group_id}_databricks.csv"
                )
                query = _build_databricks_query(group_id=group_id, fetch_limit=fetch_limit)

                hook = DatabricksSqlHook(
                    databricks_conn_id=databricks_conn_id,
                    http_path=metadata.get("databricks_http_path"),
                    catalog=metadata.get("databricks_catalog"),
                    schema=metadata.get("databricks_schema"),
                )
                dataframe = hook.get_df(sql=query, df_type="pandas")

                if dataframe.empty:
                    raise ValueError(f"Databricks query returned no rows for {group_id}")

                output_file = Path(output_path)
                output_file.parent.mkdir(parents=True, exist_ok=True)
                dataframe.to_csv(output_file, index=False)

                print(
                    f"Fetched {len(dataframe)} rows from Databricks for {group_id} "
                    f"using connection {databricks_conn_id} into {output_file}"
                )
                return str(output_file)

            @task(task_group=self)
            def generate_workbook(metadata, group_id, source_path):
                reporter = WorkbookReporter()
                workbook_path = reporter.generate_workbook(
                    source_path=source_path,
                    group_id=group_id,
                    metadata=metadata,
                )
                print(f"Generated workbook for {group_id}: {workbook_path}")
                return workbook_path

            @task(task_group=self)
            def upload_to_sharepoint(metadata, group_id, workbook_path):
                print(f"Uploading workbook for {group_id} from {workbook_path} with metadata {metadata}")
                return "uploaded"

            # ---------------------------
            # Failure route
            # ---------------------------
            @task(task_group=self, trigger_rule="one_failed")
            def send_email_no_workbook(group_id):
                print(f"Task failed for {group_id}, sending email alert...")

            # ---------------------------
            # Normal flow
            # ---------------------------
            gate = should_run(metadata=self.metadata, group_id=self.group_id)
            data_source_path = generate_data_from_databricks(metadata=self.metadata, group_id=self.group_id)
            workbook = generate_workbook(
                metadata=self.metadata,
                group_id=self.group_id,
                source_path=data_source_path,
            )
            upload = upload_to_sharepoint(
                metadata=self.metadata,
                group_id=self.group_id,
                workbook_path=workbook,
            )

            gate.set_downstream(data_source_path)
            data_source_path.set_downstream(workbook)
            workbook.set_downstream(upload)

            # ---------------------------
            # Failure handling
            # ---------------------------
            # This task runs if ANY of the upstream tasks fail
            send_email_no_workbook(group_id=self.group_id).set_upstream([data_source_path, workbook, upload])
