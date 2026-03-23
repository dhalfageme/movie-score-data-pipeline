# Movies Data Unification Pipeline

## Project Purpose
The purpose of this project is to **combine data from heterogeneous data sources into a single, unified dataset** ready for data analytics.  
The solution is designed to be **extensible**, allowing new data sources to be incorporated easily.

---

## Architecture Overview
The project implements a **medallion architecture** with multiple layers. Each data source updates at a different frequency. To accommodate this:

- Historical data is preserved in the **lower layers** (Bronze and Silver)  
- The **Gold layer** always contains the most recent, consolidated view  

---

## Layers Description

### Bronze
- Contains the **raw data** from each source  
- Adds **metadata** such as the `source` and `ingestion_date`  
- Data is **partitioned by ingestion date** to keep historical snapshots  
- Provides the foundation for all downstream processing  

### Silver
- **Staging models** are built for each data source separately:  
  - Columns with the same meaning are renamed to **common field names**  
  - Each provider remains **separate at this stage**  
- An **intermediate unified model** combines multiple files from the same provider before merging with other sources  
- Ensures consistent structure without mixing providers into single rows prematurely  

### Gold
- Produces the **final unified dataset** with **one row per movie**  
- Combines all providers while resolving conflicts using a **priority mechanism**, where certain sources take precedence for specific fields  
- Provides a **ready-to-use dataset for analytics**  

---

## Design Benefits
- **Preserves raw data history**, enabling reproducibility and debugging  
- **Modular structure** allows recalculating the Gold layer if business logic changes, without recomputing lower layers  
- **Extensible**: new data sources can be incorporated without disrupting existing pipelines  
- **Unified schema** reduces missing data while maintaining traceability  

---

## Notes
- Data is stored in a **Bronze → Silver → Gold** workflow  
- Bronze: raw, partitioned, per provider  
- Silver: staging + intermediate unified models  
- Gold: final single-row-per-movie dataset