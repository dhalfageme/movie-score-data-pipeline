from pathlib import Path
from datetime import datetime
from src.silver.staging import stg_utils as utils
import pandas as pd


def stage_provider3(ingestion_date: str = None, bronze_path: str = None, silver_path: str = None):
    """
    Merge Provider3 domestic, international, financials,
    """
    current_file = Path(__file__).resolve()
    project_root = current_file.parents[3]

    bronze_path = Path(bronze_path) if bronze_path else project_root / "data/bronze/provider3/"
    silver_path = Path(silver_path) if silver_path else project_root / "data/silver/staging/provider3/"

    if ingestion_date is None:
        ingestion_date =  utils.get_last_ingestion_date(bronze_path)

    partition_path = bronze_path / f"ingestion_date={ingestion_date}"

    domestic = pd.read_csv(partition_path / "provider3_domestic.csv")
    international = pd.read_csv(partition_path / "provider3_international.csv")
    financials = pd.read_csv(partition_path / "provider3_financials.csv")

    domestic = utils.cast_columns(domestic, {"domestic_box_office_gross": "Int64"})
    international = utils.cast_columns(international, {"box_office_gross_usd": "Int64"})
    financials = utils.cast_columns(financials, {"production_budget_usd": "Int64", "marketing_spend_usd": "Int64"})

    domestic = domestic.rename(columns={
        "box_office_gross_usd": "box_office_gross_domestic",
    }).drop(columns=["ingestion_date"])

    international = international.rename(columns={
        "box_office_gross_usd": "box_office_gross_international",
    }).drop(columns=["ingestion_date"])

    financials = financials.rename(columns={
        "production_budget_usd": "production_budget",
        "marketing_spend_usd": "marketing_budget",
    }).drop(columns=["ingestion_date"])

    for df, cols in [(domestic, ["box_office_gross_domestic"]),
                     (international, ["box_office_gross_international"]),
                     (financials, ["production_budget", "marketing_budget"])]:
        for col in cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df = domestic.merge(international, on=["film_name", "year_of_release"], how="outer")
    df = df.merge(financials, on=["film_name", "year_of_release"], how="outer")

    df["ingestion_date"] = ingestion_date #We can assume the original ingestion date is the same for all the files

    df = df.rename(columns={
        "film_name": "title",
        "year_of_release": "year"
    })
    df["provider"] = "provider3"

    out_path = silver_path / f"ingestion_date={ingestion_date}"
    utils.save_parquet(df, out_path, "stage_provider3.parquet")

    return df