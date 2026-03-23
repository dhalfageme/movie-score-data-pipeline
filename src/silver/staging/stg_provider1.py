from .stg_utils import stage_generic

def stage_provider1(ingestion_date: str = None, bronze_path: str = None, silver_path: str = None):
    return stage_generic(
    provider="provider1",
    file_name="provider1.csv",
    rename_map={
        "movie_title":"title",
        "release_year":"year",
        "critic_score_percentage":"critic_score",
        "top_critic_score":"top_critic_score",
        "total_critic_reviews_counted":"critic_reviews_count"
    },
    cast_map={
        "total_critic_reviews_counted":"Int64",
        "critic_score_percentage":"Int64",
        "top_critic_score":"float"
    },
    bronze_path=bronze_path,

    silver_path=silver_path,
    ingestion_date=ingestion_date
)