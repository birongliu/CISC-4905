
import pandas as pd
from pathlib import Path


def workbook_to_parquet(xlsx_path: str, output_dir: str = "/opt/airflow/data") -> dict[str, str]:
    """
    Parse all sheets in an Excel workbook and save each as a parquet file.

    Args:
        xlsx_path:  Path to the .xlsx file
        output_dir: Directory to write .parquet files

    Returns:
        Dict mapping sheet name -> parquet file path
    """
    src = Path(xlsx_path)
    if not src.exists():
        raise FileNotFoundError(f"Workbook not found: {xlsx_path}")

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    xl = pd.ExcelFile(src)
    output_paths = {}

    for sheet in xl.sheet_names:
        df = xl.parse(sheet)

        if df.empty:
            print(f"Skipping empty sheet: {sheet}")
            continue

        # Sanitize column names (strip whitespace)
        df.columns = [str(c).strip() for c in df.columns]

        out_path = out_dir / f"{src.stem}_{sheet}.parquet"
        df.to_parquet(out_path, index=False, engine="pyarrow")
        output_paths[sheet] = str(out_path)
        print(f"Saved sheet '{sheet}' -> {out_path}")

    return output_paths


# ── Airflow task wrapper ───────────────────────────────────────────────────────
def parse_workbook_to_parquet(**context) -> None:
    """
    Airflow PythonOperator callable.
    Expects 'xlsx_path' passed via op_kwargs or XCom.
    """
    xlsx_path = context.get("xlsx_path") or context["ti"].xcom_pull(key="xlsx_path")
    output_dir = context.get("output_dir", "/opt/airflow/data/parquet")
    workbook_to_parquet(xlsx_path, output_dir)