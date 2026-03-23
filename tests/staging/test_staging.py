import pytest
import pandas as pd
from pathlib import Path

from src.silver.staging.stg_utils import (
    stage_generic,
    read_latest_partition,
    get_last_ingestion_date,
    cast_columns,
    save_parquet
)
from src.silver.staging.stg_provider1 import stage_provider1
from src.silver.staging.stg_provider2 import stage_provider2
from src.silver.staging.stg_provider3 import stage_provider3

@pytest.fixture
def project_root(tmp_path):
    return tmp_path / "project"


@pytest.fixture
def setup_provider1_data(project_root):
    bronze_dir = project_root / "data/bronze/provider1" / "ingestion_date=2026-03-23"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    csv_path = bronze_dir / "provider1.csv"

    data = [
        {"movie_title": "Test Movie 1", "release_year": "2020", "critic_score_percentage": "85.0",
         "top_critic_score": "8.5", "total_critic_reviews_counted": "150"},
        {"movie_title": "Test Movie 2", "release_year": "2021", "critic_score_percentage": "92",
         "top_critic_score": "9.2", "total_critic_reviews_counted": "200"}
    ]
    pd.DataFrame(data).to_csv(csv_path, index=False)
    return bronze_dir


@pytest.fixture
def setup_provider2_data(project_root):
    bronze_dir = project_root / "data/bronze/provider2" / "ingestion_date=2026-03-23"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    csv_path = bronze_dir / "provider2.csv"

    data = [
        {"title": "Movie A", "year": "2019", "audience_average_score": "7.5", "total_audience_ratings": "1000",
         "domestic_box_office_gross": "50000000"},
        {"title": "Movie B", "year": "2020", "audience_average_score": "8.0", "total_audience_ratings": "1500",
         "domestic_box_office_gross": "75000000"}
    ]
    pd.DataFrame(data).to_csv(csv_path, index=False)
    return bronze_dir


@pytest.fixture
def setup_provider3_data(project_root):
    bronze_dir = project_root / "data/bronze/provider3" / "ingestion_date=2026-03-23"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    domestic_data = [
        {"film_name": "Film X", "year_of_release": "2020", "box_office_gross_usd": "60000000",
         "ingestion_date": "2026-03-23"},
        {"film_name": "Film Y", "year_of_release": "2021", "box_office_gross_usd": "80000000",
         "ingestion_date": "2026-03-23"}
    ]
    international_data = [
        {"film_name": "Film X", "year_of_release": "2020", "box_office_gross_usd": "40000000",
         "ingestion_date": "2026-03-23"},
        {"film_name": "Film Y", "year_of_release": "2021", "box_office_gross_usd": "50000000",
         "ingestion_date": "2026-03-23"}
    ]
    financials_data = [
        {"film_name": "Film X", "year_of_release": "2020", "production_budget_usd": "30000000",
         "marketing_spend_usd": "10000000", "ingestion_date": "2026-03-23"},
        {"film_name": "Film Y", "year_of_release": "2021", "production_budget_usd": "40000000",
         "marketing_spend_usd": "15000000", "ingestion_date": "2026-03-23"}
    ]

    pd.DataFrame(domestic_data).to_csv(bronze_dir / "provider3_domestic.csv", index=False)
    pd.DataFrame(international_data).to_csv(bronze_dir / "provider3_international.csv", index=False)
    pd.DataFrame(financials_data).to_csv(bronze_dir / "provider3_financials.csv", index=False)

    return bronze_dir


def test_get_last_ingestion_date(project_root, setup_provider1_data):
    bronze_path = project_root / "data/bronze/provider1"
    date = get_last_ingestion_date(bronze_path)
    assert date == "2026-03-23"


def test_read_latest_partition(project_root, setup_provider1_data):
    bronze_path = project_root / "data/bronze/provider1"
    df = read_latest_partition(bronze_path, "provider1.csv")
    assert len(df) == 2
    assert df.iloc[0]["movie_title"] == "Test Movie 1"


def test_cast_columns():
    df = pd.DataFrame({
        "int_col": ["1", "2", "invalid"],
        "float_col": ["3.5", "4.2", "bad"],
        "string_col": ["a", "b", "c"]
    })
    cast_map = {"int_col": "Int64", "float_col": "float"}
    df_cast = cast_columns(df, cast_map)

    # Valores válidos serrán convertidos
    assert df_cast["int_col"].iloc[0] == 1
    assert df_cast["int_col"].iloc[1] == 2
    assert pd.isna(df_cast["int_col"].iloc[2])

    assert df_cast["float_col"].iloc[0] == 3.5
    assert df_cast["float_col"].iloc[1] == 4.2
    assert pd.isna(df_cast["float_col"].iloc[2])

    assert df_cast["string_col"].tolist() == ["a", "b", "c"]


def test_save_parquet(tmp_path):
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    out_path = tmp_path / "test"
    file_name = "test.parquet"

    save_parquet(df, out_path, file_name)

    parquet_path = out_path / file_name
    assert parquet_path.exists()
    df_read = pd.read_parquet(parquet_path)
    pd.testing.assert_frame_equal(df, df_read)

def test_stage_provider1(project_root, setup_provider1_data):
    bronze_path = str(project_root / "data/bronze/provider1")
    silver_path = str(project_root / "data/silver/staging/provider1")

    df = stage_provider1(bronze_path=bronze_path, silver_path=silver_path)

    # Verifica que el resultado es razonable
    assert len(df) == 2
    assert set(["title", "year", "critic_score", "top_critic_score", "critic_reviews_count", "provider"]) == set(df.columns)

    # Revisión de tipos
    assert pd.api.types.is_integer_dtype(df["critic_score"])
    assert pd.api.types.is_integer_dtype(df["critic_reviews_count"])

    # Verifica que el archivo se guardó
    silver_dir = project_root / "data/silver/staging/provider1" / "ingestion_date=2026-03-23"
    parquet_file = silver_dir / "stage_provider1.parquet"
    assert parquet_file.exists()

def test_stage_provider2(project_root, setup_provider2_data):
    bronze_path = str(project_root / "data/bronze/provider2")
    silver_path = str(project_root / "data/silver/staging/provider2")

    df = stage_provider2(bronze_path=bronze_path, silver_path=silver_path)

    assert len(df) == 2
    assert "audience_score" in df.columns
    assert "audience_reviews_count" in df.columns
    assert "box_office_gross_domestic" in df.columns

    # Verifica que el archivo se guardó
    silver_dir = project_root / "data/silver/staging/provider2" / "ingestion_date=2026-03-23"
    parquet_file = silver_dir / "stage_provider2.parquet"
    assert parquet_file.exists()


def test_stage_provider3(project_root, setup_provider3_data):
    bronze_path = str(project_root / "data/bronze/provider3")
    silver_path = str(project_root / "data/silver/staging/provider3")

    df = stage_provider3(bronze_path=bronze_path, silver_path=silver_path)

    assert len(df) == 2  # Dos películas originales
    expected_cols = [
        "title", "year", "provider", "box_office_gross_domestic",
        "box_office_gross_international", "production_budget", "marketing_budget",
        "ingestion_date"
    ]
    for col in expected_cols:
        assert col in df.columns

    # Verifica que el archivo se guardó
    silver_dir = project_root / "data/silver/staging/provider3" / "ingestion_date=2026-03-23"
    parquet_file = silver_dir / "stage_provider3.parquet"
    assert parquet_file.exists()

def test_stage_provider3_outer_join(project_root, setup_provider3_data):
    bronze_dir = setup_provider3_data
    extra_row = pd.DataFrame([{"film_name": "Film Z", "year_of_release": "2022",
                               "production_budget_usd": "20000000", "marketing_spend_usd": "5000000"}])
    extra_row.to_csv(bronze_dir / "provider3_financials.csv", mode='a', header=False, index=False)

    bronze_path = str(project_root / "data/bronze/provider3")
    silver_path = str(project_root / "data/silver/staging/provider3")

    df = stage_provider3(bronze_path=bronze_path, silver_path=silver_path)
    assert len(df) == 3  # Film X, Y, Z


def test_no_partition_error(project_root):
    bronze_path = project_root / "data/bronze/nonexistent"
    # Justo como en tu código real
    with pytest.raises(FileNotFoundError):  # Solo tipo, no mensaje
        get_last_ingestion_date(bronze_path)