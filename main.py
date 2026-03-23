from datetime import datetime
from src.bronze import provider1 as br1
from src.bronze import provider2 as br2
from src.bronze import provider3 as br3
from src.silver.staging import stg_provider1
from src.silver.staging import stg_provider2
from src.silver.staging import stg_provider3
from src.silver.intermediate import movies_unified
from src.gold import movies_gold_incremental

def main():
    print("=== Starting full pipeline ===")

    # --- 1. Bronze: ingest raw data ---
    print("Bronze: Provider1 (weekly)")
    df1 = br1.bronze_provider1()

    print("Bronze: Provider2 (biweekly)")
    df2 = br2.bronze_provider2()

    print("Bronze: Provider3 (monthly)")
    df3_dom, df3_int, df3_fin = br3.bronze_provider3()

    # --- 2. Silver: staging & intermediate ---
    print("Silver: Building Stage Model for Provider 1")
    df_stg1 = stg_provider1.stage_provider1()
    print("Silver: Building Stage Model for Provider 2")
    df_stg2 = stg_provider2.stage_provider2()
    print("Silver: Building Stage Model for Provider 3")
    df_stg3 = stg_provider3.stage_provider3()
    print("Silver: Building intermediate unified dataframe")
    intermediate_df = movies_unified.build_intermediate_unified()

    # --- 3. Gold: incremental ---
    print("Gold: Building incremental gold dataset")
    gold_df = movies_gold_incremental.build_gold_incremental(intermediate_df=intermediate_df)

    # --- 4. Optionally save gold locally ---
    output_path = "./data/gold/gold_final.parquet"
    gold_df.to_parquet(output_path, index=False)
    print(f"Gold dataset saved at {output_path}")

    print("=== Pipeline finished successfully ===")
    print(f"Number of movies in Gold: {len(gold_df)}")
    print(gold_df.head())

if __name__ == "__main__":
    main()