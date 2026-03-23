import pandas as pd
import pytest
from pathlib import Path
from src.silver.staging._stg_utils import stage_generic

@pytest.mark.parametrize("provider, file_names, rename_map, cast_map, merge_keys", [
    (
        "provider1",
        ["provider1.csv"],
        {
            "movie_title": "title",
            "release_year": "year",
            "critic_score_percentage": "critic_score",
            "top_critic_score": "critic_average_score",
            "total_critic_reviews_counted": "critic_reviews_count"
        },
        {
            "total_critic_reviews_counted": "Int64",
            "critic_score_percentage": "Int64",
            "top_critic_score": "float"
        },
        None
    ),
    (
        "provider2",
        ["provider2.csv"],
        {
            "title": "title",
            "year": "year",
            "audience_average_score": "audience_score",
            "total_audience_ratings": "audience_reviews_count",
            "domestic_box_office_gross": "box_office_domestic"
        },
        {
            "total_audience_ratings": "Int64",
            "domestic_box_office_gross": "Int64",
            "audience_average_score": "float"
        },
        None
    ),
    (
        "provider3",
        ["provider3_domestic.csv", "provider3_international.csv", "provider3_financials.csv"],
        {
            "film_name":"title",
            "year_of_release":"year",
            "box_office_gross_usd_domestic":"box_office_domestic",
            "box_office_gross_usd_international":"box_office_international",
            "production_budget_usd":"production_budget",
            "marketing_spend_usd":"marketing_budget"
        },
        {
            "box_office_gross_usd_domestic": "Int64",
            "box_office_gross_usd_international": "Int64",
            "production_budget_usd": "Int64",
            "marketing_spend_usd": "Int64"
        },
        ["film_name", "year_of_release"]
    )
])
def test_stage_generic(tmp_path, provider, file_names, rename_map, cast_map, merge_keys):
    """
    Test generic staging function with temporary files and partition.
    """

    ingestion_date = "2026-03-23"

    bronze_path = tmp_path / provider
    partition_path = bronze_path / f"ingestion_date={ingestion_date}"
    partition_path.mkdir(parents=True, exist_ok=True)

    for file in file_names:
        df = pd.DataFrame({
            col: [1, 2] if "count" in col or "budget" in col or "gross" in col else ["A", "B"]
            for col in rename_map.keys()
        })
        df.to_csv(partition_path / file, index=False)

    silver_path = tmp_path / f"silver_{provider}"

    df_out = stage_generic(
        provider=provider,
        file_names=file_names,
        bronze_path=bronze_path,
        silver_path=silver_path,
        rename_map=rename_map,
        cast_map=cast_map,
        merge_keys=merge_keys,
        ingestion_date=ingestion_date
    )

    for new_col in rename_map.values():
        assert new_col in df_out.columns

    assert "provider" in df_out.columns
    assert all(df_out["provider"] == provider)

    parquet_file = silver_path / f"ingestion_date={ingestion_date}" / f"stage_{provider}.parquet"
    assert parquet_file.exists()

    assert not df_out.empty