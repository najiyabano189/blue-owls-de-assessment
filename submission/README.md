# Blue Owls Data Engineer Assessment — Submission

**Candidate:** Najiya Bano  
**Date:** March 2026  
**Stack:** Python, PySpark, SQL, Jupyter Lab, Docker

---

## Overview

This pipeline ingests e-commerce data from the Blue Owls Data API, transforms it through a medallion architecture (Bronze → Silver → Gold), and produces a star schema ready for business analysis.

---

## Notebook Structure

| Notebook | Purpose |
|---|---|
| `01_ingestion.ipynb` | Pulls data from all 6 API endpoints into Bronze layer |
| `02_silver.ipynb` | Cleans and validates Bronze data into Silver layer |
| `03_gold.ipynb` | Builds star schema Gold layer with validation |
| `04_sql_analysis.ipynb` | Runs SQL queries against Gold layer |

---

## Technical Decisions

### 1. Python + requests for ingestion, not PySpark
I used Python's `requests` library for API ingestion rather than PySpark because PySpark is a transformation tool — it processes data already on a filesystem. It is not designed for HTTP authentication, token refresh, or pagination logic. Python handles these natively and cleanly. PySpark takes over once the data lands on disk.

### 2. Exponential backoff for API failures
The API intentionally returns 401, 429, and 500 errors. I handle each differently:
- **401** — token expired — refresh the token immediately and retry without waiting
- **429** — rate limited — wait with exponential backoff (1s, 2s, 4s, 8s...) to give the server time to recover
- **500** — server error — same exponential backoff as 429

I chose exponential backoff over fixed retries because it is more respectful to the API server — each retry waits longer, reducing the chance of making the problem worse.

### 3. Manifest file for idempotency
The ingestion pipeline tracks every successfully ingested page in a `manifest.json` file. If the pipeline crashes mid-run and restarts, it skips already-completed pages and picks up exactly where it left off. Without this, a crash would cause duplicate records in Bronze.

### 4. Hash-based surrogate keys
I used `F.abs(F.hash(natural_key))` instead of `row_number()` to generate surrogate keys in the Gold layer. Row numbers change every time the pipeline runs depending on the order data arrives. Hash-based keys are deterministic — the same natural key always produces the same surrogate key regardless of when or how many times the pipeline runs. This makes the Gold layer stable and reproducible across runs.

### 5. Deduplication on customer_unique_id not customer_id
The dataset assigns a new `customer_id` for every order — the same person placing 3 orders gets 3 different customer_ids. The `customer_unique_id` is the true identifier for a person. Deduplicating on `customer_id` would have created one row per order instead of one row per person in `dim_customers`, inflating customer counts and distorting all customer-level analytics. I confirmed this finding in the Silver layer — 53,441 customer_ids map to only 52,395 unique persons.

### 6. Proportional payment distribution
One payment row covers the entire order, but the fact table is at item level. I distributed the total payment value across items proportionally by price — an item worth 70% of the order total receives 70% of the payment value. This preserves the correct total when summing payment_value at any level of aggregation.

### 7. Null handling strategy
- **Null category names** — filled with "unknown" rather than dropping. Dropping would silently remove products from all downstream analytics.
- **Null delivery dates** — kept as null. Filling with estimates would be fabricating data.
- **Null product dimensions** — `product_volume_cm3` set to null if any dimension is missing rather than calculating a wrong volume.
- **Invalid records** — flagged with `_is_valid = false` rather than deleted. This preserves an audit trail and lets analysts choose whether to include or exclude them.

---

## Assumptions and Trade-offs

- **Date filter:** Applied `date_from=2018-07-01` to orders and order_items as instructed. For customers, products, sellers, and payments — which do not support date filtering — I fetched all records and rely on joins to order data to scope them correctly.
- **Bronze append-only:** Each run appends new records without overwriting existing data. Deduplication on natural keys before writing prevents duplicate rows across runs.
- **Silver upsert:** Silver reflects the current state of each record using `dropDuplicates` on natural keys. Re-processing updates existing records rather than stacking duplicates.
- **Single Spark session:** I used `local[*]` mode throughout which runs Spark on all available CPU cores locally. This is sufficient for this dataset size.

---

## What I Would Change for Production on Azure

### Orchestration
Replace manual notebook execution with **Azure Data Factory** pipelines or **Apache Airflow** for scheduled, monitored runs. Each notebook would become a pipeline stage with dependency management and alerting on failure.

### Storage
Replace CSV files with **Delta Lake** format on **Azure Data Lake Storage Gen2**. Delta Lake provides ACID transactions, schema enforcement, and time travel — you can query data as it looked at any point in the past. This makes Bronze append-only and Silver upsert trivially reliable.

### Security
Store API credentials in **Azure Key Vault** rather than environment variables. No secrets in code or config files. Managed identities for service-to-service authentication.

### Monitoring and Alerting
Use **Azure Monitor** and **Log Analytics** to track pipeline run times, record counts, and data quality metrics over time. Set alerts for anomalies — e.g. if Bronze record count drops 50% compared to the previous run, something is wrong with the API.

### CI/CD
Use **GitHub Actions** to run automated tests on every pull request before merging pipeline changes. Tests would include schema validation, record count checks, and referential integrity checks — the same checks currently in `03_gold.ipynb` but automated.

### Cost Optimisation
Use **Spark partitioning** on `order_date` for the fact table so queries filtering by date only scan relevant partitions. Schedule pipeline runs during off-peak hours to use lower-cost compute. Use **auto-scaling clusters** in Azure Databricks so compute scales with data volume rather than being fixed.

---

## Running the Pipeline

1. Install Docker Desktop
2. Run `docker-compose up` from the repo root
3. Open `http://localhost:8888` in your browser
4. Run notebooks in order: `01` → `02` → `03` → `04`
5. All outputs are written to `work/output/`


---

## Microsoft Fabric Specific Considerations

### Why Microsoft Fabric over standalone Azure services
Microsoft Fabric is an all-in-one analytics platform that combines Data Factory, Synapse, Power BI, and Data Lake into a single unified experience. For a pipeline like this, Fabric would be the modern choice over assembling individual Azure services manually.

### How this pipeline would look in Fabric

| Current (Local) | Microsoft Fabric Equivalent |
|---|---|
| Jupyter notebooks | Fabric Notebooks (same PySpark API) |
| CSV files in folders | OneLake with Delta Lake format |
| Manual runs | Data Factory pipelines in Fabric |
| No monitoring | Fabric Monitor hub |
| Local Spark | Fabric Spark clusters (auto-scaling) |

### OneLake as the storage layer
All Bronze, Silver, and Gold data would live in **OneLake** — Fabric's unified storage layer. Every workspace in the organisation can access the same data without copying it. The Gold layer tables would be registered as **Fabric Lakehouses** so Power BI reports can query them directly with no data movement.

### Scheduling in Fabric
The four notebooks would be orchestrated as a **Fabric Data Pipeline** with dependencies:
- Ingestion runs first
- Silver runs only after ingestion succeeds
- Gold runs only after Silver succeeds
- SQL validation runs last
- Any failure sends an alert via email or Teams

### CI/CD in Fabric
Fabric has native **Git integration** — connect the workspace directly to this GitHub repository. Every merge to main automatically deploys updated notebooks to the Fabric workspace. No manual uploads needed.

### Cost optimisation in Fabric
Fabric uses a **capacity-based pricing model** (Fabric Capacity Units). For a pipeline running once daily, you would pause the capacity overnight and on weekends — paying only for active compute time. Fabric Spark also auto-scales so you never pay for more cores than the job needs.
