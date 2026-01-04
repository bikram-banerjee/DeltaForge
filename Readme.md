<h1><strong>DeltaForge</strong></h1>

<h2><strong>DeltaForge – End-to-End Databricks × DBT Lakehouse Data Engineering Project</strong></h2>

<p>
DeltaForge is a production-grade data engineering project built using Databricks, Apache PySpark, Delta Lake, and DBT.
It is a comprehensive, production-ready solution implementing the Medallion Architecture (Bronze–Silver–Gold) for a flight booking analytics system with dynamic, reusable pipeline frameworks.
</p>

<h2><strong>Architecture Overview</strong></h2>

<ul>
  <li><strong>Bronze Layer</strong> – Raw data ingestion (Delta Lake)
    <ul>
      <li>Incremental ingestion using Databricks Autoloader with Spark Structured Streaming.</li>
      <li>Dynamic, parameterized notebooks for multiple source folders.</li>
      <li>Schema evolution and idempotent processing with checkpointing.</li>
      <li>Unity Catalog Volumes for governed storage.</li>
    </ul>
  </li>

  <li><strong>Silver Layer</strong> – Cleaned &amp; validated data (PySpark)
    <ul>
      <li>Lakeflow Declarative Pipelines (Delta Live Tables).</li>
      <li>Streaming tables and materialized views.</li>
      <li>Auto-CDC upserts to dimension tables.</li>
      <li>DLT Expectations for data quality.</li>
      <li>Incremental processing via modified_date tracking.</li>
    </ul>
  </li>

  <li><strong>Gold Layer</strong> – Analytics-ready marts (PySpark)
    <ul>
      <li>Star Schema design.</li>
      <li>Automated SCD Type 1 builder with surrogate keys.</li>
      <li>Dynamic fact builder with dimension lookups.</li>
      <li>Temporal tracking and backdated refreshes.</li>
      <li>Supports initial + incremental loads.</li>
    </ul>
  </li>

  <li><strong>Business View</strong> – Use cases solved with DBT
    <ul>
      <li>SQL-first transformations in DBT.</li>
      <li>CTEs, joins, and business logic models.</li>
      <li>Version control and lineage in DBT Cloud.</li>
      <li>Views materialized in Unity Catalog.</li>
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
  <li>Dynamic pipeline framework.</li>
  <li>Schema evolution automation.</li>
  <li>Incremental + idempotent loading.</li>
  <li>Automated dimensional modeling.</li>
  <li>Embedded data quality framework.</li>
  <li>CDC across layers.</li>
  <li>Job orchestration &amp; dependencies.</li>
</ul>

<h2><strong>Business Requirements</strong></h2>

<h3>View 1: bookings_enriched</h3>
<p>Unified business table combining fact bookings with passengers, routes, countries, dates, and revenue.</p>

<h3>View 2: airport_revenue_summary</h3>
<p>Shows top-revenue airports, revenue trends over time, and partnership expansion opportunities.</p>

<h3>View 3: route_performance</h3>
<p>Highlights profitable routes, underperforming routes, and airline performance by route.</p>

<h3>View 4: passenger_value</h3>
<p>Identifies high-value passengers, nationality spending trends, and loyalty program targets.</p>

<h3>View 5: data_quality_bookings</h3>
<p>Validates no null keys, no negative amounts, and correct foreign-key relationships.</p>

<h2><strong>How to Run</strong></h2>

<ol>
  <li>Upload notebooks to Databricks.</li>
  <li>Configure Delta tables.</li>
  <li>Create the Bronze Ingestion Pipeline.</li>
  <li>Create the Silver ETL Pipeline.</li>
  <li>Run the Gold notebooks.</li>
  <li>Execute DBT:</li>
</ol>

<pre><code>dbt run</code></pre>
