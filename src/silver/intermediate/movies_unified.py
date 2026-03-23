from pathlib import Path
import pandas as pd
from datetime import datetime

from src.silver.staging import _stg_provider1 as stg1
from src.silver.staging import _stg_provider2 as stg2
from src.silver.staging import _stg_provider3 as stg3

def build_intermediate_unified(silver_path: str = None):
    """
    Build intermediate unified dataframe combining all Silver staging providers.
    Reads the latest partition of each provider from disk.
    Saves intermediate unified dataframe as Parquet in /data/silver/intermediate/
    """
    if silver_path is None:
        current_file = Path(__file__).resolve()
        project_root = current_file.parents[3]
        silver_path = project_root / "data/silver"

    intermediate_path = silver_path / "intermediate/"
    intermediate_path.mkdir(parents=True, exist_ok=True)

    print("Loading staging Provider1...")
    df1 = stg1.stage_provider1()
    print("Loading staging Provider2...")
    df2 = stg2.stage_provider2()
    print("Loading staging Provider3...")
    df3 = stg3.stage_provider3()

    unified_df = pd.concat([df1, df2, df3], ignore_index=True, sort=False)

    unified_df["silver_ingestion_date"] = datetime.today().strftime("%Y-%m-%d")

    unified_file = intermediate_path / "intermediate_unified.parquet"
    unified_df.to_parquet(unified_file, index=False, engine="pyarrow")
    print(f"Intermediate unified dataset saved at {unified_file} ({len(unified_df)} rows)")

    return unified_df