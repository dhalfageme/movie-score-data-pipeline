from pathlib import Path
import pandas as pd
from datetime import datetime

def build_gold_incremental(silver_path: str = None, gold_path: str = None):
    """
    Build incremental gold dataset from the latest Silver intermediate Parquet partition.
    - silver_path: path to the intermediate Silver folder
    - gold_path: path to save gold_master.parquet
    """
    current_file = Path(__file__).resolve()
    project_root = current_file.parents[2]

    silver_path = Path(silver_path) if silver_path else project_root / "data/silver/intermediate/"
    gold_path = Path(gold_path) if gold_path else project_root / "data/gold/gold_master.parquet"
    gold_path.parent.mkdir(parents=True, exist_ok=True)

    partitions = [p for p in silver_path.iterdir() if p.is_dir()]
    if not partitions:
        raise FileNotFoundError(f"No partitions found in {silver_path}")
    latest_partition = sorted(partitions)[-1]

    parquet_files = list(latest_partition.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in latest partition {latest_partition}")
    intermediate_df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True, sort=False)

    if gold_path.exists():
        gold_master = pd.read_parquet(gold_path)
        # Keep most recent ingestion by title + year + provider
        combined = pd.concat([gold_master, intermediate_df], ignore_index=True, sort=False)
        combined = combined.sort_values("ingestion_date")
        gold_master = combined.drop_duplicates(subset=["title", "year", "provider"], keep="last")
    else:
        gold_master = intermediate_df

    gold_master["gold_ingestion_date"] = datetime.today().strftime("%Y-%m-%d")

    gold_master.to_parquet(gold_path, index=False, engine="pyarrow")
    print(f"Gold dataset saved at {gold_path} ({len(gold_master)} rows)")

    return gold_master