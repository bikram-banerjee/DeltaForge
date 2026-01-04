# 🔥 DeltaForge

## DeltaForge – End-to-End Databricks × DBT Lakehouse Data Engineering Project

DeltaForge is a production-grade data engineering project built using Databricks, Apache PySpark, Delta Lake, and DBT.  
It demonstrates an end-to-end lakehouse architecture using the medallion pattern (Bronze, Silver, Gold).

---

## 🧠 Architecture Overview

- **Bronze Layer** – Raw data ingestion (Delta Lake)
- **Silver Layer** – Cleaned & validated data (PySpark)
- **Gold Layer** – Analytics-ready marts (PySpark)
- **Business View** - Business Use Cases Solved (DBT)

---

## 🚀 Tech Stack

- Databricks
- Apache Spark (PySpark)
- Delta Lake
- DBT
- SQL
- Python

---

## 📂 Project Structure

- `databricks/` → PySpark ETL notebooks
- `dbt/` → SQL-based transformations
- `workflows/` → Databricks job orchestration
- `utils/` → Data quality utilities

---

## ▶️ How to Run

1. Upload notebooks to Databricks.
2. Configure Delta tables
3. Make a BronzeIngestion Pipeline which is linked to the Bronze Stage notebook.
4. Make a SilverIngestion ETL Pipeline which will contain the Silver Stage notebook.
5. Run the Gold Notebooks.
4. Execute DBT models:
   ```bash
   dbt run