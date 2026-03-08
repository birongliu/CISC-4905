from pathlib import Path
from typing import Any, Mapping

import pandas as pd


class WorkbookReporter:
    def __init__(self, output_dir: str = "/opt/airflow/data"):
        self.output_dir = Path(output_dir)

    def generate_workbook(
        self,
        source_path: str,
        group_id: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> str:
        source = Path(source_path)
        if not source.exists():
            raise FileNotFoundError(f"Databricks output file not found: {source_path}")

        dataframe = pd.read_csv(source)
        if dataframe.empty:
            raise ValueError(f"No rows found in Databricks output: {source_path}")

        metadata = metadata or {}
        execution_date = str(metadata.get("execution_date", "unknown_date"))

        self.output_dir.mkdir(parents=True, exist_ok=True)
        workbook_path = self.output_dir / f"{group_id}_{execution_date}.xlsx"

        with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
            dataframe.to_excel(writer, sheet_name="flights", index=False)
            self._build_report_metadata(dataframe, metadata, group_id).to_excel(
                writer,
                sheet_name="report_meta",
                index=False,
            )
            self._build_country_summary(dataframe).to_excel(
                writer,
                sheet_name="country_summary",
                index=False,
            )

        return str(workbook_path)

    def _build_report_metadata(
        self,
        dataframe: pd.DataFrame,
        metadata: Mapping[str, Any],
        group_id: str,
    ) -> pd.DataFrame:
        unique_countries = int(dataframe["origin_country"].nunique()) if "origin_country" in dataframe.columns else 0
        max_velocity = float(dataframe["velocity"].max()) if "velocity" in dataframe.columns else 0.0
        average_velocity = float(dataframe["velocity"].mean()) if "velocity" in dataframe.columns else 0.0

        rows = [
            {"metric": "group_id", "value": group_id},
            {"metric": "execution_date", "value": metadata.get("execution_date", "")},
            {"metric": "weekday", "value": metadata.get("weekday", "")},
            {"metric": "total_rows", "value": int(len(dataframe))},
            {"metric": "unique_origin_countries", "value": unique_countries},
            {"metric": "avg_velocity", "value": round(average_velocity, 2)},
            {"metric": "max_velocity", "value": round(max_velocity, 2)},
        ]
        return pd.DataFrame(rows)

    def _build_country_summary(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        if "origin_country" not in dataframe.columns:
            return pd.DataFrame(columns=["origin_country", "flights"])

        summary = (
            dataframe.groupby("origin_country", dropna=False)
            .size()
            .reset_index(name="flights")
            .sort_values("flights", ascending=False)
        )
        return summary