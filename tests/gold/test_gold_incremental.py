import pandas as pd
from pathlib import Path
from datetime import datetime
import pyarrow.parquet as pq
from src.gold.movies_gold_incremental import build_gold_incremental


def create_silver_partition(tmp_path, df, ingestion_date):
    partition_path = tmp_path / ingestion_date
    partition_path.mkdir(parents=True, exist_ok=True)
    file_path = partition_path / "intermediate_unified.parquet"
    df.to_parquet(file_path, index=False, engine="pyarrow")
    return file_path

def test_gold_no_master(tmp_path):
    silver_path = tmp_path / "silver"
    df = pd.DataFrame({
        "title": ["A", "B"],
        "year": [2000, 2001],
        "provider": ["p1", "p2"],
        "ingestion_date": ["2026-01-01", "2026-01-01"]
    })
    create_silver_partition(silver_path, df, "2026-01-01")

    gold_path = tmp_path / "gold_master.parquet"
    result = build_gold_incremental(silver_path=silver_path, gold_path=gold_path)

    assert len(result) == 2
    assert gold_path.exists()


def test_gold_append_new_records(tmp_path):
    silver_path = tmp_path / "silver"

    df_initial = pd.DataFrame({
        "title": ["A"],
        "year": [2000],
        "provider": ["p1"],
        "ingestion_date": ["2026-01-01"]
    })
    create_silver_partition(silver_path, df_initial, "2026-01-01")

    gold_path = tmp_path / "gold_master.parquet"
    build_gold_incremental(silver_path=silver_path, gold_path=gold_path)

    df_new = pd.DataFrame({
        "title": ["B"],
        "year": [2001],
        "provider": ["p2"],
        "ingestion_date": ["2026-01-02"]
    })
    create_silver_partition(silver_path, df_new, "2026-01-02")

    result = build_gold_incremental(silver_path=silver_path, gold_path=gold_path)

    assert len(result) == 2
    assert set(result["title"]) == {"A", "B"}


def test_gold_replace_existing_with_newer(tmp_path):
    silver_path = tmp_path / "silver"

    df_initial = pd.DataFrame({
        "title": ["A"],
        "year": [2000],
        "provider": ["p1"],
        "ingestion_date": ["2026-01-01"],
        "score": [5]
    })
    create_silver_partition(silver_path, df_initial, "2026-01-01")

    gold_path = tmp_path / "gold_master.parquet"
    build_gold_incremental(silver_path=silver_path, gold_path=gold_path)

    df_new = pd.DataFrame({
        "title": ["A"],
        "year": [2000],
        "provider": ["p1"],
        "ingestion_date": ["2026-01-02"],
        "score": [9]
    })
    create_silver_partition(silver_path, df_new, "2026-01-02")

    result = build_gold_incremental(silver_path=silver_path, gold_path=gold_path)

    assert len(result) == 1
    assert result.iloc[0]["score"] == 9


def test_gold_keep_old_if_new_is_older(tmp_path):
    silver_path = tmp_path / "silver"

    df_initial = pd.DataFrame({
        "title": ["A"],
        "year": [2000],
        "provider": ["p1"],
        "ingestion_date": ["2026-01-02"],
        "score": [9]
    })
    create_silver_partition(silver_path, df_initial, "2026-01-02")

    gold_path = tmp_path / "gold_master.parquet"
    build_gold_incremental(silver_path=silver_path, gold_path=gold_path)

    df_old = pd.DataFrame({
        "title": ["A"],
        "year": [2000],
        "provider": ["p1"],
        "ingestion_date": ["2026-01-01"],
        "score": [5]
    })
    create_silver_partition(silver_path, df_old, "2026-01-01")

    result = build_gold_incremental(silver_path=silver_path, gold_path=gold_path)

    assert len(result) == 1
    assert result.iloc[0]["score"] == 9


def test_gold_keep_non_updated_records(tmp_path):
    silver_path = tmp_path / "silver"

    df_initial = pd.DataFrame({
        "title": ["A", "B"],
        "year": [2000, 2001],
        "provider": ["p1", "p2"],
        "ingestion_date": ["2026-01-01", "2026-01-01"]
    })
    create_silver_partition(silver_path, df_initial, "2026-01-01")

    gold_path = tmp_path / "gold_master.parquet"
    build_gold_incremental(silver_path=silver_path, gold_path=gold_path)

    df_new = pd.DataFrame({
        "title": ["A"],
        "year": [2000],
        "provider": ["p1"],
        "ingestion_date": ["2026-01-02"]
    })
    create_silver_partition(silver_path, df_new, "2026-01-02")

    result = build_gold_incremental(silver_path=silver_path, gold_path=gold_path)

    assert len(result) == 2
    assert set(result["title"]) == {"A", "B"}