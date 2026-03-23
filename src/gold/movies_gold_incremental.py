from pathlib import Path
import pandas as pd

def build_gold_incremental(intermediate_df: pd.DataFrame, gold_path: str = None):
    """
    Incremental Gold model:
    - Uses ingestion_date from Silver
    - Uses gold_ingestion_date from existing Gold
    - Keeps the latest record per (title, year)
    """

    current_file = Path(__file__).resolve()
    project_root = current_file.parents[2]

    if gold_path is None:
        gold_path = project_root / "data/gold/gold_master.parquet"
    else:
        gold_path = Path(gold_path)

    gold_path.parent.mkdir(parents=True, exist_ok=True)

    incoming = intermediate_df.copy()

    if "ingestion_date" not in incoming.columns:
        raise ValueError("intermediate_df must contain 'ingestion_date' column")

    incoming["ingestion_date"] = pd.to_datetime(incoming["ingestion_date"])
    incoming = incoming.rename(columns={"ingestion_date": "gold_ingestion_date"})

    if gold_path.exists():
        gold_master = pd.read_parquet(gold_path)
        gold_master["gold_ingestion_date"] = pd.to_datetime(gold_master["gold_ingestion_date"])
        combined = pd.concat([gold_master, incoming], ignore_index=True, sort=False)

    else:
        combined = incoming

    combined = combined.sort_values(["title", "year", "gold_ingestion_date"])
    gold_master = combined.drop_duplicates(
        subset=["title", "year"],
        keep="last"
    )

    gold_master.to_parquet(gold_path, index=False, engine="pyarrow")

    print(f"Gold dataset saved at {gold_path} ({len(gold_master)} rows)")

    return gold_master