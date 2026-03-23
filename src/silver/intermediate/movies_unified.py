from pathlib import Path
import pandas as pd
from datetime import datetime

def build_intermediate_unified(silver_path: str = None):
    """
    Build intermediate unified dataframe by reading the latest Silver staging Parquet files.
    """
    if silver_path is None:
        current_file = Path(__file__).resolve()
        project_root = current_file.parents[3]
        silver_path = project_root / "data/silver"

    intermediate_path = silver_path / "intermediate/"
    intermediate_path.mkdir(parents=True, exist_ok=True)

    dfs = []
    for provider in ["provider1", "provider2", "provider3"]:
        provider_path = silver_path / "staging" / provider
        partitions = [p for p in provider_path.iterdir() if p.is_dir()]
        if not partitions:
            print(f"No partitions found for {provider}, skipping...")
            continue
        latest_partition = sorted(partitions)[-1]
        parquet_file = latest_partition / f"stage_{provider}.parquet"
        if parquet_file.exists():
            print(f"Loading {parquet_file}...")
            df = pd.read_parquet(parquet_file, engine="pyarrow")
            dfs.append(df)
        else:
            print(f"Parquet file for {provider} not found in {latest_partition}, skipping...")

    if not dfs:
        raise FileNotFoundError("No staging Parquet files found for any provider.")

    unified_df = pd.concat(dfs, ignore_index=True, sort=False)
    unified_df["silver_ingestion_date"] = datetime.today().strftime("%Y-%m-%d")

    unified_file = intermediate_path / "intermediate_unified.parquet"
    unified_df.to_parquet(unified_file, index=False, engine="pyarrow")
    print(f"Intermediate unified dataset saved at {unified_file} ({len(unified_df)} rows)")

    return unified_df