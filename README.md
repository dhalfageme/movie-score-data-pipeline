# Movies Data Unification Pipeline

## Project Purpose
This project **combines data from heterogeneous sources** into a single, unified dataset ready for analytics.  
The solution is **extensible**, allowing new data sources to be easily incorporated.

---

## Architecture Overview
The project follows a **Medallion architecture** (Bronze → Silver → Gold) designed for sources with different update frequencies:

- **Bronze/Silver layers** preserve historical data snapshots  
- **Gold layer** always contains the most recent, consolidated view

## Project Structure

```
movie-score-data-pipeline/
├── data/
│ ├── raw/ # Mocked data from 3 providers
│ ├── bronze/ # Raw data + metadata, partitioned by date
│ ├── silver/ # Unified column names + staging models
│ └── gold/ # Final unified dataset (one row per movie)
├── src/
│ ├── dags/ # Airflow DAGs orchestrating the pipeline
│ ├── bronze/ # Bronze layer models per provider
│ ├── silver/ # Silver staging + intermediate unified models
│ └── gold/ # Incremental gold model for analytics
└── main.py # Execute full pipeline with default parameters
```

## Layers Description

### 🥉 Bronze Layer
- **Raw data** from each provider with added metadata (`source`, `ingestion_date`)
- **Partitioned by ingestion date** to preserve historical snapshots
- One DAG per provider, frequency matches source update rate

### 🥈 Silver Layer
**Staging models** (per provider):
- Rename columns to **common field names**
- Safe type casting for consistency
- **Intermediate unified model** combines multiple files per provider
- Assumption made: there are no duplicated rows per movie on the same provider. In other case drop duplicates would be needed.

**Uses ExternalTaskSensor** to trigger only when Bronze dependencies complete.

### 🥇 Gold Layer
- **Final unified dataset**: **one row per movie**
- **Conflict resolution** via priority mechanism (certain providers override others for 
  specific fields)
- **Incremental updates**: new data appended, existing rows preserved
- Ready for analytics and business consumption.
- The Gold model consolidates data from multiple sources and maintains a record for each 
  data source and movie.
- This design choice simplifies the approach for the POC, ensuring analysts can access 
  consistent movie information across different providers.
- A potential enhancement would be to merge data for the same movie into a single row. Given the final dataset schema — title, year, critic_score, top_critic_score, critics_review_count, audience_score, audience_review_count, box_office_gross_domestic, box_office_gross_international, production_budget, marketing_budget, ingestion_date, silver_ingestion_date, and gold_ingestion_date — most of these columns are exclusive to a single provider (at the moment), except for the grouping keys (title) and year, which are assumed to be consistent across all sources. There is one special column, box_office_gross_domestic, that appears in both Provider 2 and Provider 3.

We could modify the gold model to join the staging data from different providers by title and apply some precedence rules, or even keep all the values from different providers by adding suffixes (for example, _provider1, _provider2). At this stage, however, I prefer to keep things simple: each provider remains as a separate row in the gold model. Extending this to a unified, joined structure later is straightforward, by re‑joining the split provider data according to the business‑defined rules. The current architecture makes it easy to recompute the gold layer whenever the logic changes, starting from the existing silver models.

<img width="1713" height="258" alt="image" src="https://github.com/user-attachments/assets/f5ae075e-ddac-47e8-9046-634a08194500" />


For simplicity, the current assumption is that the Gold layer allows consumers to select the desired provider afterward.
---

## Key Design Benefits

| Benefit | Description |
|---------|-------------|
| **Historical preservation** | Bronze/Silver keep full history for reproducibility |
| **Modular recomputation** | Change Gold logic without reprocessing lower layers |
| **Extensible** | Add new providers without breaking existing flows |
| **Unified schema** | Single consistent view while maintaining traceability |

---

## Local Development (Airflow)

```bash
# Required environment variables
export PYTHONPATH=/path/to/project:$PYTHONPATH
export AIRFLOW__CORE__DAGS_FOLDER=/path/to/movie-score-data-pipeline/src/dags

# Terminal 1: Scheduler
airflow scheduler

# Terminal 2: Webserver  
airflow webserver

# Terminal 3: Test DAGs
airflow dags list
airflow dags trigger bronze_provider1
Note: Use LocalExecutor in airflow.cfg for parallel task execution.

### Workflow Summary

Raw Data (providers) 
    ↓ (per provider DAGs)
Bronze (raw + metadata, partitioned) 
    ↓ (ExternalTaskSensor)
Silver (unified columns + staging) 
    ↓ 
Gold (final unified dataset)

## Design Benefits

This architecture provides several key advantages:

1. **Incremental extensibility**: New providers can be added without recomputing Bronze and staging Silver models for existing providers. Only the Silver unified model and Gold layer need recalculation.

2. **Modular recomputation**: Gold business rules can be modified and recomputed independently, without re-executing Silver models.

3. **Configurable conflict resolution**: Providers may have conflicting information for the same fields. The Gold layer can implement **precedence logic** as a future imporvement that can be configured for maximum flexibility on the gold layer.

The result is a **production-ready pipeline** that balances maintainability, performance, and adaptability to changing business requirements.
