import pandas as pd
from src.gold.movies_gold_incremental import build_gold_incremental


def test_gold_no_master(tmp_path):
    gold_path = tmp_path / "gold.parquet"

    df = pd.DataFrame({
        "title": ["A", "B"],
        "year": [2000, 2001],
        "ingestion_date": ["2026-01-01", "2026-01-01"]
    })

    result = build_gold_incremental(df, gold_path=gold_path)

    assert len(result) == 2
    assert gold_path.exists()


def test_gold_append_new_records(tmp_path):
    gold_path = tmp_path / "gold.parquet"

    df_initial = pd.DataFrame({
        "title": ["A"],
        "year": [2000],
        "ingestion_date": ["2026-01-01"]
    })
    build_gold_incremental(df_initial, gold_path=gold_path)

    df_new = pd.DataFrame({
        "title": ["B"],
        "year": [2001],
        "ingestion_date": ["2026-01-02"]
    })

    result = build_gold_incremental(df_new, gold_path=gold_path)

    assert len(result) == 2
    assert set(result["title"]) == {"A", "B"}


def test_gold_replace_existing_with_newer(tmp_path):
    gold_path = tmp_path / "gold.parquet"

    df_initial = pd.DataFrame({
        "title": ["A"],
        "year": [2000],
        "ingestion_date": ["2026-01-01"],
        "score": [5]
    })
    build_gold_incremental(df_initial, gold_path=gold_path)

    df_new = pd.DataFrame({
        "title": ["A"],
        "year": [2000],
        "ingestion_date": ["2026-01-02"],
        "score": [9]
    })

    result = build_gold_incremental(df_new, gold_path=gold_path)

    assert len(result) == 1
    assert result.iloc[0]["score"] == 9


def test_gold_keep_old_if_new_is_older(tmp_path):
    gold_path = tmp_path / "gold.parquet"

    df_initial = pd.DataFrame({
        "title": ["A"],
        "year": [2000],
        "ingestion_date": ["2026-01-02"],
        "score": [9]
    })
    build_gold_incremental(df_initial, gold_path=gold_path)

    df_old = pd.DataFrame({
        "title": ["A"],
        "year": [2000],
        "ingestion_date": ["2026-01-01"],
        "score": [5]
    })

    result = build_gold_incremental(df_old, gold_path=gold_path)

    assert len(result) == 1
    assert result.iloc[0]["score"] == 9

def test_gold_keep_non_updated_records(tmp_path):
    gold_path = tmp_path / "gold.parquet"

    df_initial = pd.DataFrame({
        "title": ["A", "B"],
        "year": [2000, 2001],
        "ingestion_date": ["2026-01-01", "2026-01-01"]
    })
    build_gold_incremental(df_initial, gold_path=gold_path)

    df_new = pd.DataFrame({
        "title": ["A"],
        "year": [2000],
        "ingestion_date": ["2026-01-02"]
    })

    result = build_gold_incremental(df_new, gold_path=gold_path)

    assert len(result) == 2
    assert set(result["title"]) == {"A", "B"}