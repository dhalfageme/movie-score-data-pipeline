from pathlib import Path
import pandas as pd

from pathlib import Path
from typing import List, Optional, Dict
import pandas as pd

def stage_generic(
    provider: str,
    file_name: str,
    bronze_path: Optional[Path] = None,
    silver_path: Optional[Path] = None,
    rename_map: Optional[Dict[str,str]] = None,
    cast_map: Optional[Dict[str,str]] = None,
    ingestion_date: Optional[str] = None
):
    """
    Simplified generic staging function for single CSV per provider.
    Handles casting, renaming, adding provider column, and saving Parquet.
    """
    project_root = Path(__file__).resolve().parents[3]

    bronze_path = Path(bronze_path) if bronze_path else project_root / f"data/bronze/{provider}/"
    silver_path = Path(silver_path) if silver_path else project_root / f"data/silver/staging/{provider}/"

    if ingestion_date is None:
        partitions = [p for p in bronze_path.iterdir() if p.is_dir()]
        if not partitions:
            raise FileNotFoundError(f"No partitions in {bronze_path}")
        ingestion_date = sorted(partitions)[-1].name.split("=")[-1]

    df =  read_latest_partition(bronze_path, file_name)

    # Cast columns
    if cast_map:
        for col, dtype in cast_map.items():
            if col in df.columns:
                if dtype == "Int64":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                else:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

    if rename_map:
        df = df.rename(columns={k:v for k,v in rename_map.items() if k in df.columns})

    df["provider"] = provider
    out_path = silver_path / f"ingestion_date={ingestion_date}"
    out_path.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path / f"stage_{provider}.parquet", index=False, engine="pyarrow")

    return df

def get_last_ingestion_date(bronze_path: Path) -> str:
    partitions = [p for p in bronze_path.iterdir() if p.is_dir()]
    if not partitions:
        raise FileNotFoundError(f"No partitions in {bronze_path}")
    last_partition = sorted(partitions)[-1]
    return last_partition.name.split("=")[-1]


def read_latest_partition(bronze_path: Path, file_name: str) -> pd.DataFrame:
    last_date = get_last_ingestion_date(bronze_path)
    file_path = bronze_path / f"ingestion_date={last_date}" / file_name
    return pd.read_csv(file_path)

def cast_columns(df: pd.DataFrame, cast_map: dict):
    for col, dtype in cast_map.items():
        if col in df.columns:
            if dtype == "Int64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dtype == "float":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(dtype)
    return df

def save_parquet(df: pd.DataFrame, out_path: Path, file_name: str):
    out_path.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path / file_name, index=False, engine="pyarrow")