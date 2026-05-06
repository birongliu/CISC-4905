from airflow.sdk import dag, task, get_current_context
from datetime import datetime
import pandas as pd

from workbook_group import WorkbookGroup

import os
CALENDAR_CSV_PATH = "/opt/airflow/data/calendar_2018_2030.csv"
ROLLUP_ENABLED = True

@dag(
    dag_id="flight_dag",
    start_date=datetime(2026, 2, 7),
    schedule="@daily",
    catchup=True,
)
def calendar_metadata_dag():

    # Step 1: Calendar
    @task
    def generate_calendar():
        # Create date range
        calendar_df = pd.date_range(start="2018-01-01", end="2030-12-31", freq="D").to_frame(index=False, name="date")

        # Ensure 'date' column is datetime
        calendar_df["date"] = pd.to_datetime(calendar_df["date"])

        # Add extra columns
        calendar_df["year"] = calendar_df["date"].dt.year
        calendar_df["month"] = calendar_df["date"].dt.month
        calendar_df["day"] = calendar_df["date"].dt.day
        calendar_df["weekday_num"] = calendar_df["date"].dt.weekday 
        calendar_df["weekday"] = calendar_df["date"].dt.day_name()
        calendar_df["is_weekend"] = calendar_df["weekday_num"].isin([5, 6])
        calendar_df["week"] = calendar_df["date"].dt.isocalendar().week

        # Add US holidays
        import holidays
        us_holidays = holidays.country_holidays("US", years=range(2018, 2031))
        calendar_df["is_holiday"] = calendar_df["date"].isin(us_holidays)

        # Save CSV
        os.makedirs(os.path.dirname(CALENDAR_CSV_PATH), exist_ok=True)
        calendar_df.to_csv(CALENDAR_CSV_PATH, index=False)
        print(f"Saved {len(calendar_df)} dates to {CALENDAR_CSV_PATH}")

        return CALENDAR_CSV_PATH
    

    @task
    def get_metadata():
        # Load the calendar CSV
        df = pd.read_csv(CALENDAR_CSV_PATH, parse_dates=["date"])

        context = get_current_context()
        logical_date = context.get("logical_date") or context.get("data_interval_start")
        if logical_date is None:
            raise ValueError("No execution date found in task context")
        execution_date = pd.Timestamp(logical_date).date()

        # Find the current week based on execution date
        current_row = df[df["date"].dt.date == execution_date]
        if current_row.empty:
            raise ValueError(f"No calendar entry for execution date {execution_date}")

        current_week = int(current_row["week"].iloc[0])
        weekday_num = int(current_row["weekday_num"].iloc[0])

        # Compute metadata
        all_groups = [
            "current_week_workbook",
            "next_week_workbook",
            "next2_week_workbook",
            "current_weekend_workbook",
            "next_weekend_workbook",
        ]

        if ROLLUP_ENABLED:
            run_groups = all_groups
        elif weekday_num in (0, 1, 2):
            run_groups = ["current_week_workbook"]
        elif weekday_num in (3, 4):
            run_groups = all_groups
        else:
            run_groups = []

        metadata = {
            "execution_date": execution_date.isoformat(),
            "weekday": weekday_num,
            "prev_week": current_week - 1,
            "current_week": current_week,
            "next_week": current_week + 1,
            "next2_week": current_week + 2,
            "rollup": ROLLUP_ENABLED,
            "databricks_conn_id": os.getenv("DATABRICKS_CONN_ID", "databricks_conn"),
            "run_groups": run_groups,
        }

        # Allow Databricks SQL connection details to come from container env vars.
        for env_var, metadata_key in (
            ("DATABRICKS_HTTP_PATH", "databricks_http_path"),
            ("DATABRICKS_SQL_ENDPOINT_NAME", "databricks_sql_endpoint_name"),
            ("DATABRICKS_CATALOG", "databricks_catalog"),
            ("DATABRICKS_SCHEMA", "databricks_schema"),
            ("SHAREPOINT_SITE_URL", "sharepoint_site_url"),
            ("SHAREPOINT_FOLDER_URL", "sharepoint_folder_url"),
            ("SHAREPOINT_USERNAME", "sharepoint_username"),
            ("SHAREPOINT_BASE_URL", "sharepoint_base_url"),
            ("SHAREPOINT_PASSWORD", "sharepoint_password"),
            ("SHAREPOINT_CLIENT_ID", "sharepoint_client_id"),
            ("SHAREPOINT_CLIENT_SECRET", "sharepoint_client_secret"),
            ("SHAREPOINT_UPLOAD_CHUNK_SIZE", "sharepoint_upload_chunk_size"),
        ):
            value = os.getenv(env_var)
            print(f"Environment variable {env_var} = {value}")
            if value:
                metadata[metadata_key] = value
        print(f"Generated metadata for execution date {execution_date}: {metadata}")
        return metadata

    calendar_path = generate_calendar()
    metadata = get_metadata()

    current_week_workbook = WorkbookGroup(group_id="current_week_workbook", metadata=metadata)
    next_week_workbook = WorkbookGroup(group_id="next_week_workbook", metadata=metadata)
    calendar_path.set_downstream(metadata)
    metadata.set_downstream(current_week_workbook)
    metadata.set_downstream(next_week_workbook)
   
calendar_metadata_dag()
