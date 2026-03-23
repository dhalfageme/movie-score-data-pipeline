from pathlib import Path
import pandas as pd
from typing import List, Dict, Optional

def stage_generic(
    provider: str,
    file_names: List[str],
    bronze_path: Optional[Path] = None,
    silver_path: Optional[Path] = None,
    rename_map: Optional[Dict[str,str]] = None,
    cast_map: Optional[Dict[str,str]] = None,
    merge_keys: Optional[List[str]] = None,
    ingestion_date: Optional[str] = None
):
    """
    Generic staging function for multiple CSVs per provider.
    """
    project_root = Path(__file__).resolve().parents[3]

    bronze_path = Path(bronze_path) if bronze_path else project_root / f"data/bronze/{provider}/"
    silver_path = Path(silver_path) if silver_path else project_root / f"data/silver/staging/{provider}/"

    if ingestion_date is None:
        partitions = [p for p in bronze_path.iterdir() if p.is_dir()]
        if not partitions:
            raise FileNotFoundError(f"No partitions in {bronze_path}")
        ingestion_date = sorted(partitions)[-1].name.split("=")[-1]

    dfs = []
    for file in file_names:
        df = pd.read_csv(bronze_path / f"ingestion_date={ingestion_date}" / file)
        # Cast columns
        if cast_map:
            for col, dtype in cast_map.items():
                if col in df.columns:
                    if dtype == "Int64":
                        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                    else:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
        dfs.append(df)

    if len(dfs) > 1:
        if not merge_keys:
            raise ValueError("merge_keys required when multiple CSVs are provided")
        df = dfs[0]
        for df_next in dfs[1:]:
            df = df.merge(df_next, on=merge_keys, how="outer")
    else:
        df = dfs[0]

    if rename_map:
        df = df.rename(columns={k:v for k,v in rename_map.items() if k in df.columns})

    df["provider"] = provider

    out_path = silver_path / f"ingestion_date={ingestion_date}"
    out_path.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path / f"stage_{provider}.parquet", index=False, engine="pyarrow")

    return df

def save_silver(df: pd.DataFrame, provider: str, silver_path: Path, ingestion_date: str):
    out_path = silver_path / f"ingestion_date={ingestion_date}"
    out_path.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path / f"stage_{provider}.parquet", index=False, engine="pyarrow")
    return df

def get_latest_partition(bronze_path: Path):
    partitions = [p for p in bronze_path.iterdir() if p.is_dir()]
    if not partitions:
        raise FileNotFoundError(f"No partitions found in {bronze_path}")
    return sorted(partitions)[-1].name.split("=")[-1]

def read_bronze_csv(bronze_path: Path, ingestion_date: str, file_name: str):
    path = bronze_path / f"ingestion_date={ingestion_date}" / file_name
    return pd.read_csv(path)
