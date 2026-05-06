import logging
import os
from pathlib import Path
import tempfile

from airflow.exceptions import AirflowException
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.sdk import TaskGroup, task
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from cryptography.fernet import Fernet
from databricks_reporter import WorkbookReporter
log = logging.getLogger(__name__)


DEFAULT_DATABRICKS_CONN_ID = "databricks_conn"
DEFAULT_DATABRICKS_FETCH_LIMIT = 500
DATABRICKS_SOURCE_TABLE = "workspace.flights.ingest_flights"
TEMP_WORK_DIR = Path("/opt/airflow/data/tmp")
DEFAULT_SHAREPOINT_UPLOAD_CHUNK_SIZE = 1024 * 1024
WEEKEND_GROUP_IDS = {
    "current_weekend_workbook",
    "next_weekend_workbook",
}


def _build_work_dir(metadata, group_id: str) -> Path:
    execution_date = metadata.get("execution_date", "run")
    TEMP_WORK_DIR.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix=f"{group_id}_{execution_date}_", dir=TEMP_WORK_DIR))


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


_REQUIRED_SHAREPOINT_KEYS = ("sharepoint_site_url", "sharepoint_username", "sharepoint_password")


def _build_sharepoint_context(metadata: dict) -> ClientContext:
    """Build and return an authenticated SharePoint ClientContext.

    Args:
        metadata: DAG run metadata containing SharePoint credentials.

    Raises:
        AirflowException: If any required SharePoint configuration key is missing.
    """
    site_url = metadata.get("sharepoint_site_url") or os.getenv("SHAREPOINT_SITE_URL")
    username = metadata.get("sharepoint_username") or os.getenv("SHAREPOINT_USERNAME")
    password = metadata.get("sharepoint_password", "") or os.getenv("SHAREPOINT_PASSWORD", "")
    base_url = os.getenv("SHAREPOINT_BASE_URL") 
    SECRET_KEY = os.getenv("SECRET_KEY")

    if not SECRET_KEY:
        raise AirflowException("Missing 'SECRET_KEY' environment variable for encrypting SharePoint password.")
    
    if not base_url:
       raise AirflowException("Missing 'SHAREPOINT_BASE_URL' environment variable for SharePoint connection.")
    
    if password is None:
        raise AirflowException("Missing SharePoint password. Set 'sharepoint_password' in metadata or 'SHAREPOINT_PASSWORD' in environment variables.")
    
    resolved_values = {
        "sharepoint_site_url": site_url,
        "sharepoint_username": username,
        "sharepoint_password": password,
        "sharepoint_base_url": base_url,
        "secret_key": SECRET_KEY
    }
    missing = [key for key in _REQUIRED_SHAREPOINT_KEYS if not resolved_values.get(key)]
    if missing:
        raise AirflowException(
            f"Missing SharePoint configuration key(s): {', '.join(missing)}. "
            "Expected values from metadata or environment variables: "
            "SHAREPOINT_SITE_URL, SHAREPOINT_USERNAME, SHAREPOINT_PASSWORD."
        )
    print("Secret", SECRET_KEY)
    log.info("Authenticating to SharePoint at %s as %s", base_url, username)
    encrypted = Fernet(SECRET_KEY).decrypt(password)
    ctx = ClientContext(base_url).with_credentials(UserCredential(username, encrypted)).execute_query()

    try:
        ctx.load(ctx.web).execute_query()
    except Exception as e:
        print(f"Failed to authenticate to SharePoint: {e}")

    log.info("Successfully authenticated to SharePoint")
    return ctx


def _upload_workbook_to_sharepoint(metadata, workbook_path):
    def authenticate_sharepoint():
        try:
            return _build_sharepoint_context(metadata)
        except AirflowException as e:
            print(f"SharePoint authentication failed: {e}")
            raise

    def upload_file(context: ClientContext, folder_url: str, file_path: str):
        try:
            target_folder = context.web.get_folder_by_server_relative_path(folder_url).execute_query()
            print(target_folder.serverRelativeUrl)
            print(f"Uploading {file_path} to SharePoint folder {folder_url}...")
            with open(file_path, "rb") as content_file:
                # print(f"Read file {file_path} ({os.path.getsize(file_path)} bytes)")
                # target_folder.upload_file("hello", 'HELLO').execute_query()
                # target_folder.upload_file(Path(file_path).name, content_file.read()).execute_query()
                # file_content = content_file.read()
                # name = Path(file_path).name
                # upload_result = target_folder.upload_file(name, file_content).execute_query()
                # return upload_result
                return target_folder
        except Exception as e:
            print(f"Failed to upload {file_path} to SharePoint folder {folder_url}: {e}")
            raise

    context = authenticate_sharepoint()
    folder_url = metadata.get("sharepoint_folder_url")
    if not folder_url:
        raise AirflowException("Missing 'sharepoint_folder_url' in metadata for SharePoint upload.")
    return upload_file(context, folder_url, workbook_path)

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

                output_path = metadata.get("databricks_output_path")
                if not output_path:
                    work_dir = _build_work_dir(metadata=metadata, group_id=group_id)
                    output_path = work_dir / "databricks.csv"

                query = _build_databricks_query(group_id=group_id, fetch_limit=fetch_limit)

                databricks_http_path = metadata.get("databricks_http_path")
                databricks_sql_endpoint_name = metadata.get("databricks_sql_endpoint_name")
                if not databricks_http_path and not databricks_sql_endpoint_name:
                    raise AirflowException(
                        "Missing Databricks SQL endpoint configuration. Set either "
                        "'databricks_http_path' or 'databricks_sql_endpoint_name' in metadata, "
                        "or configure http_path in the Airflow connection extras."
                    )

                hook_kwargs = {
                    "databricks_conn_id": databricks_conn_id,
                    "catalog": metadata.get("databricks_catalog"),
                    "schema": metadata.get("databricks_schema"),
                }
                if databricks_http_path:
                    hook_kwargs["http_path"] = databricks_http_path
                if databricks_sql_endpoint_name:
                    hook_kwargs["sql_endpoint_name"] = databricks_sql_endpoint_name

                hook = DatabricksSqlHook(**hook_kwargs)
        
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
                reporter = WorkbookReporter(output_dir=str(Path(source_path).parent))
                workbook_path = reporter.generate_workbook(
                    source_path=source_path,
                    group_id=group_id,
                    metadata=metadata,
                )
                print(f"Generated workbook for {group_id}: {workbook_path}")
                return workbook_path

            @task(task_group=self)
            def upload_to_sharepoint(metadata, group_id, workbook_path):
                uploaded_file = _upload_workbook_to_sharepoint(metadata, workbook_path)
                print(
                    f"Uploaded workbook for {group_id} from {workbook_path} "
                    f"to {uploaded_file.serverRelativeUrl}"
                )
                return uploaded_file.serverRelativeUrl

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
