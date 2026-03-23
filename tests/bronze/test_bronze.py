from src.bronze import provider1, provider2, provider3

def test_bronze_provider1(tmp_path):
    raw_dir = tmp_path / "raw_provider1"
    raw_dir.mkdir()
    csv_file = raw_dir / "provider1.csv"
    csv_file.write_text(
        "movie_title,release_year,critic_score_percentage,top_critic_score,total_critic_reviews_counted\n"
        "Inception,2010,87,8.1,450\n"
        "The Dark Knight,2008,94,8.6,350"
    )

    bronze_dir = tmp_path / "bronze_provider1"

    df = provider1.bronze_provider1(raw_path=raw_dir, bronze_path=bronze_dir)

    assert "ingestion_date" in df.columns
    ingestion_date = df["ingestion_date"][0]
    assert (bronze_dir / f"ingestion_date={ingestion_date}").exists()
    assert df.shape[0] == 2

def test_bronze_provider2(tmp_path):
    raw_dir = tmp_path / "raw_provider2"
    raw_dir.mkdir()
    json_file = raw_dir / "provider2.json"
    json_file.write_text(
        '[{"title": "Inception", "year": "2010", "audience_average_score": 9.1, "total_audience_ratings": 1500000, "domestic_box_office_gross": 292576195}]'
    )

    bronze_dir = tmp_path / "bronze_provider2"
    df = provider2.bronze_provider2(raw_path=raw_dir, bronze_path=bronze_dir)

    assert "ingestion_date" in df.columns
    ingestion_date = df["ingestion_date"][0]
    assert (bronze_dir / f"ingestion_date={ingestion_date}").exists()
    assert df.shape[0] == 1


def test_bronze_provider3(tmp_path):
    raw_dir = tmp_path / "raw_provider3"
    raw_dir.mkdir()
    files = {
        "provider3_domestic.csv": "film_name,year_of_release,box_office_gross_usd\nInception,2010,292576195",
        "provider3_international.csv": "film_name,year_of_release,box_office_gross_usd\nInception,2010,535700000",
        "provider3_financials.csv": "film_name,year_of_release,production_budget_usd,marketing_spend_usd\nInception,2010,160000000,100000000"
    }
    for fname, content in files.items():
        (raw_dir / fname).write_text(content)

    bronze_dir = tmp_path / "bronze_provider3"
    dfs = provider3.bronze_provider3(base_path=raw_dir, bronze_path=bronze_dir)

    for df in dfs:
        assert "ingestion_date" in df.columns
        assert df.shape[0] == 1

    ingestion_date = dfs[0]["ingestion_date"][0]
    assert (bronze_dir / f"ingestion_date={ingestion_date}").exists()