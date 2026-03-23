from pathlib import Path
import pandas as pd
from datetime import datetime

def build_intermediate_unified(silver_path: str = None, providers=None) -> pd.DataFrame:
    """
    Build intermediate unified dataframe by reading the latest Silver staging Parquet files.
    The unified dataframe is saved partitioned by the max ingestion date among all providers.
    """

    # Default providers if not provided
    if providers is None:
        providers = ["provider1", "provider2", "provider3"]

    if silver_path is None:
        current_file = Path(__file__).resolve()
        project_root = current_file.parents[3]
        silver_path = project_root / "data/silver"

    intermediate_path = silver_path / "intermediate/"
    intermediate_path.mkdir(parents=True, exist_ok=True)

    dfs = []
    partition_dates = []

    for provider in providers:
        provider_path = silver_path / "staging" / provider
        partitions = [p for p in provider_path.iterdir() if p.is_dir()]
        if not partitions:
            print(f"No partitions found for {provider}, skipping...")
            continue
        latest_partition = sorted(partitions)[-1]
        partition_dates.append(latest_partition.name.split("=")[-1])
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

    # Use the max ingestion date from the provider partitions for the unified dataset
    max_ingestion_date = max(partition_dates)
    unified_df["silver_ingestion_date"] = datetime.today().strftime("%Y-%m-%d")

    # Save the unified parquet partitioned by the max ingestion date
    out_path = intermediate_path / f"ingestion_date={max_ingestion_date}"
    out_path.mkdir(parents=True, exist_ok=True)
    unified_file = out_path / "intermediate_unified.parquet"
    unified_df.to_parquet(unified_file, index=False, engine="pyarrow")

    print(f"Intermediate unified dataset saved at {unified_file} ({len(unified_df)} rows)")

    return unified_df