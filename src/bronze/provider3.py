from src.bronze._bronze_utils import bronze_generic

def bronze_provider3(base_path=None, bronze_path=None):
    return bronze_generic(
        provider="provider3",
        input_files=[
            "provider3_domestic.csv",
            "provider3_international.csv",
            "provider3_financials.csv"
        ],
        input_format="csv",
        base_path=base_path,
        bronze_path=bronze_path
    )