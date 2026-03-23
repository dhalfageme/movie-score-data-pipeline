from ._stg_utils import stage_generic

def stage_provider2(ingestion_date: str = None, bronze_path: str = None, silver_path: str = None):
    """
    Stage Provider2 using the generic staging function.
    """
    return stage_generic(
        provider="provider2",
        file_names=["provider2.csv"],
        rename_map={
            "title": "title",
            "year": "year",
            "audience_average_score": "audience_score",
            "total_audience_ratings": "audience_reviews_count",
            "domestic_box_office_gross": "box_office_domestic"
        },
        cast_map={
            "total_audience_ratings": "Int64",
            "domestic_box_office_gross": "Int64",
            "audience_average_score": "float"
        },
        bronze_path=bronze_path,
        silver_path=silver_path,
        ingestion_date=ingestion_date
    )