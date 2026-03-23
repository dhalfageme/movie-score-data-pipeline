from src.bronze._bronze_utils import bronze_generic

def bronze_provider2(raw_path=None, bronze_path=None):
    return bronze_generic(
        provider="provider2",
        input_files=["provider2.json"],
        input_format="json",
        base_path=raw_path,
        bronze_path=bronze_path
    )