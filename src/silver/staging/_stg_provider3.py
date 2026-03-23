
from ._stg_utils import stage_generic

def stage_provider3(ingestion_date: str = None, bronze_path: str = None, silver_path: str = None):
    return stage_generic(
        provider="provider3",
        file_names=["provider3_domestic.csv","provider3_international.csv","provider3_financials.csv"],
        merge_keys=["film_name","year_of_release"],
        rename_map={
            "film_name":"title",
            "year_of_release":"year",
            "box_office_gross_usd_domestic":"box_office_domestic",
            "box_office_gross_usd_international":"box_office_international",
            "production_budget_usd":"production_budget",
            "marketing_spend_usd":"marketing_budget"
        },
        cast_map={
            "box_office_gross_usd_domestic":"Int64",
            "box_office_gross_usd_international":"Int64",
            "production_budget_usd":"Int64",
            "marketing_spend_usd":"Int64"
        },
        bronze_path=bronze_path,
        silver_path=silver_path,
        ingestion_date=ingestion_date
)