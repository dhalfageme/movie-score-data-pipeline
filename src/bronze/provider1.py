from src.bronze._bronze_utils import bronze_generic

def bronze_provider1(raw_path=None, bronze_path=None):
    return bronze_generic(
        provider="provider1",
        input_files=["provider1.csv"],
        input_format="csv",
        base_path=raw_path,
        bronze_path=bronze_path
    )