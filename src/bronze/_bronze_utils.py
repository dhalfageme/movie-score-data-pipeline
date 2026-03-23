from pathlib import Path
import pandas as pd
from datetime import datetime

def bronze_generic(provider: str,
                   input_files: list,
                   input_format: str = "csv",
                   base_path: str = None,
                   bronze_path: str = None):
    """
    Generic Bronze ingestion function.

    Saves partitioned CSVs with ingestion_date, converting JSON to CSV if needed.
    """
    project_root = Path(__file__).resolve().parents[2]

    if base_path is None:
        base_path = project_root / f"data/raw/{provider}/"
    else:
        base_path = Path(base_path)

    if bronze_path is None:
        bronze_path = project_root / f"data/bronze/{provider}/"
    else:
        bronze_path = Path(bronze_path)

    ingestion_date = datetime.today().strftime("%Y-%m-%d")
    out_path = bronze_path / f"ingestion_date={ingestion_date}"
    out_path.mkdir(parents=True, exist_ok=True)

    dfs = []
    for file in input_files:
        file_path = base_path / file
        if input_format.lower() == "csv":
            df = pd.read_csv(file_path)
        elif input_format.lower() == "json":
            df = pd.read_json(file_path)
        else:
            raise ValueError(f"Unsupported input format: {input_format}")

        df["ingestion_date"] = ingestion_date

        out_file = out_path / (Path(file).stem + ".csv")
        df.to_csv(out_file, index=False)
        dfs.append(df)

    return dfs if len(dfs) > 1 else dfs[0]