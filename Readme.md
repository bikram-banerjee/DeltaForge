<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>DeltaForge – README</title>
</head>
<body>

  <h1><strong>DeltaForge</strong></h1>

  <h2><strong>DeltaForge – End-to-End Databricks × DBT Lakehouse Data Engineering Project</strong></h2>

  <p>
    DeltaForge is a production-grade data engineering project built using Databricks, Apache PySpark, Delta Lake, and DBT.
    It is a comprehensive, production-ready data engineering solution implementing the Medallion Architecture
    (Bronze–Silver–Gold) using Databricks, PySpark, Delta Live Tables, and DBT for a flight booking analytics system with
    dynamic, reusable pipeline frameworks.
  </p>

  <h2><strong>Architecture Overview</strong></h2>

  <ul>
    <li><strong>Bronze Layer</strong> – Raw data ingestion (Delta Lake)
      <ul>
        <li>Implemented incremental data ingestion using Databricks Autoloader with Spark Structured Streaming for real-time processing.</li>
        <li>Built dynamic, parameterized notebooks for flexible data loading across multiple source folders.</li>
        <li>Configured schema evolution and idempotent processing with checkpoint management for exactly-once semantics.</li>
        <li>Utilized Unity Catalog Volumes for governed file storage with organized hierarchical structures.</li>
      </ul>
    </li>

    <li><strong>Silver Layer</strong> – Cleaned &amp; validated data (PySpark)
      <ul>
        <li>Designed Lakeflow Declarative Pipelines (Delta Live Tables) for scalable ETL workflows.</li>
        <li>Implemented streaming tables and materialized views for cleaning and standardization.</li>
        <li>Integrated Auto CDC flows for efficient upserts on dimension tables.</li>
        <li>Applied DLT Expectations for ongoing data validation and monitoring.</li>
        <li>Managed incremental processing with modified_date tracking.</li>
      </ul>
    </li>

    <li><strong>Gold Layer</strong> – Analytics-ready marts (PySpark)
      <ul>
        <li>Architected Star Schema with fact and dimension tables for optimized analytics.</li>
        <li>Developed an automated SCD Type 1 builder with surrogate key generation.</li>
        <li>Created dynamic fact table builders with automated dimension lookups.</li>
        <li>Implemented surrogate key management, temporal tracking, and backdated refresh capability.</li>
        <li>Supported initial loads, incremental updates, and historical reprocessing.</li>
      </ul>
    </li>

    <li><strong>Business View</strong> – Business Use Cases Solved (DBT)
      <ul>
        <li>Integrated DBT with Databricks for SQL-first transformation workflows.</li>
        <li>Created DBT models with CTEs, complex joins, and business logic.</li>
        <li>Enabled version control and lineage tracking in DBT Cloud IDE.</li>
        <li>Materialized business views in Unity Catalog for BI tools.</li>
      </ul>
    </li>
  </ul>

  <hr />

  <h2><strong>Tech Stack</strong></h2>
  <ul>
    <li>Databricks</li>
    <li>Apache Spark (PySpark)</li>
    <li>Delta Lake</li>
    <li>DBT</li>
    <li>SQL</li>
    <li>Python</li>
  </ul>

  <h2><strong>Key Implementations &amp; Advanced Features</strong></h2>
  <ul>
    <li><strong>Dynamic Pipeline Framework</strong>: Reusable, parameterized notebooks replacing static code.</li>
    <li><strong>Schema Evolution Management</strong>: Automated handling of schema changes with caching and checkpoints.</li>
    <li><strong>Incremental Loading Patterns</strong>: Idempotent processing with watermarking and state management.</li>
    <li><strong>Dimensional Modeling Automation</strong>: Self-service dimension and fact builders.</li>
    <li><strong>Data Quality Framework</strong>: Expectations and validations across pipeline stages.</li>
    <li><strong>CDC Implementation</strong>: Change propagation across Bronze–Silver–Gold layers.</li>
    <li><strong>Orchestration &amp; Dependencies</strong>: Managed complex job dependencies end-to-end.</li>
  </ul>

  <h2><strong>Business Requirements</strong></h2>

  <h3>View 1: bookings_enriched</h3>
  <p>
    This view provides a unified business table combining bookings with passenger, route, country, date,
    and revenue details — giving analysts everything they need in one place rather than querying only the fact table.
  </p>

  <h3>View 2: airport_revenue_summary</h3>
  <p>
    Designed to answer strategic revenue questions, such as which airports generate the most revenue,
    how revenue trends change over time, and which airports present opportunities for deeper partnerships.
  </p>

  <h3>View 3: route_performance</h3>
  <p>
    Helps executives evaluate route profitability, identify underperforming routes to discontinue or optimize,
    and determine which airlines perform best across specific routes.
  </p>

  <h3>View 4: passenger_value</h3>
  <p>
    Supports marketing teams by highlighting high-value passengers, spending differences across nationalities,
    and customer segments suitable for loyalty and targeted campaigns.
  </p>

  <h3>View 5: data_quality_bookings</h3>
  <p>
    Ensures operational reliability by validating that keys are not null, amounts are never negative,
    and all foreign keys correctly match dimension tables.
  </p>

  <h2><strong>How to Run</strong></h2>

  <ol>
    <li>Upload notebooks to Databricks.</li>
    <li>Configure Delta tables.</li>
    <li>Create a <strong>Bronze Ingestion Pipeline</strong> linked to the Bronze Stage notebook.</li>
    <li>Create a <strong>Silver Ingestion ETL Pipeline</strong> linked to the Silver Stage notebook.</li>
    <li>Run the Gold notebooks.</li>
    <li>Execute DBT models:</li>
  </ol>

  <pre><code>dbt run</code></pre>

</body>
</html>
