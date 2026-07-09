# 🛒 E-Commerce Clickstream Lakehouse on Azure

An end-to-end streaming data platform that ingests simulated e-commerce
clickstream events, processes them through a medallion lakehouse architecture
on Azure, and serves a star schema to Power BI for funnel analytics.

**Skills demonstrated:** Data Engineering · Data Analysis · (ML — planned, see Roadmap)

---

## 📐 Architecture

```
┌──────────────┐    ┌──────────────┐    ┌─────────────────────────────┐    ┌──────────────┐
│ Python Event │───▶│ Azure Event  │───▶│   Azure Databricks          │───▶│   Power BI   │
│  Generator   │    │    Hubs      │    │  Structured Streaming       │    │  (Import via │
│   (Faker)    │    │              │    │  Bronze ─▶ Silver ─▶ Gold   │    │SQL Warehouse)│
└──────────────┘    └──────────────┘    └─────────────────────────────┘    └──────────────┘
                                                      │
                                              Delta Lake tables on
                                                 ADLS Gen2
```

**Key design decisions**
- **Storage/compute separation** — all data lives as Delta tables on ADLS Gen2;
  Databricks is the processing engine, and the data outlives any single workspace.
- **`trigger(availableNow=True)`** — streams run as scheduled incremental batches,
  processing everything new then shutting down. Exactly-once semantics via
  checkpoints, without a 24/7 cluster.
- **Medallion architecture** — raw events are preserved untouched in bronze,
  enabling full replay if downstream logic changes.
- **Import mode in Power BI** — the dashboard is self-contained and works even
  when compute is stopped.

---

## 🗂️ Data Flow (Medallion Layers)

| Layer  | Purpose | Key operations |
|--------|---------|----------------|
| **Bronze** | Raw events, exactly as received | Append-only ingest from Event Hubs, schema-on-read, checkpointed |
| **Silver** | Cleaned, validated, deduplicated | Dedup on `event_id`, type casting, timestamp parsing, quality filters |
| **Gold**   | Analytics-ready star schema | Incremental MERGE loads, surrogate keys, SCD Type 2 on customer dimension |

### Gold Star Schema

```
                 ┌────────────────┐
                 │  dim_customer  │ (SCD Type 2)
                 └───────┬────────┘
                         │
┌──────────────┐   ┌─────▼─────────┐   ┌──────────────┐
│  dim_product │───│  fact_events  │───│   dim_date   │
└──────────────┘   └─────┬─────────┘   └──────────────┘
                         │
                 ┌───────▼────────┐
                 │  dim_device    │
                 └────────────────┘
```

---

## 🧪 Simulated Data

Events are generated with **Faker**, structured as realistic user **sessions**
walking through an e-commerce funnel with dropout probabilities at each stage:

```
page_view ──▶ product_view ──▶ add_to_cart ──▶ checkout ──▶ purchase
   100%           ~60%             ~25%           ~30%         ~80%
```

Each event includes: `event_id` (UUID, dedup key), `session_id`, `user_id`,
`event_type`, `product_id`, `event_timestamp`, `device`, `referrer`.

The generator intentionally embeds behavioral biases (e.g., device and referrer
affect conversion) so downstream analytics — and the future ML model — have
real signal to surface.

---

## 🛠️ Tech Stack

- **Ingestion:** Azure Event Hubs
- **Processing:** Azure Databricks (PySpark Structured Streaming, Delta Lake)
- **Storage:** ADLS Gen2 (Delta format), Unity Catalog
- **Serving:** Databricks SQL Warehouse → Power BI (Import mode)
- **Language:** Python 3.x, SQL

---

## 📊 Analytics (Power BI)

> 🚧 Report currently in progress.

Planned/included analyses:
- Funnel conversion & stage-by-stage drop-off
- Conversion rate by device and referrer
- Session behavior trends over time
- Top products by views vs. purchases

*(Screenshots / demo video will be added here.)*

---

## 🚀 Setup & Run

### Prerequisites
- Azure subscription (Event Hubs + ADLS Gen2 storage account)
- Databricks workspace (or Free Edition)
- Power BI Desktop

### 1. Generate events
```bash
pip install -r requirements.txt
python generator/produce_events.py --events-per-run 5000
```

### 2. Run the pipeline
Databricks notebooks in `/notebooks`, run in order (or via the included Job):
1. `01_bronze_ingest` — Event Hubs → bronze Delta (availableNow trigger)
2. `02_silver_clean` — dedup, validate, conform
3. `03_gold_dimensional` — SCD2 dimension merges + fact load
4. `04_audits` — uniqueness, referential integrity & quality checks

### 3. Connect Power BI
Get Data → Azure Databricks → SQL Warehouse hostname + HTTP path → select
gold tables → Import mode → model star schema relationships.

💡 **Cost note:** All compute uses auto-stop / short-lived runs. The full
pipeline runs comfortably within Azure free-tier credits.

---

## 🗺️ Roadmap

- [x] Event generator with session-based funnel simulation
- [x] Streaming ingestion (Event Hubs → bronze)
- [x] Silver layer: dedup & data quality
- [x] Gold star schema with SCD Type 2 & incremental MERGE
- [ ] Power BI funnel dashboard *(in progress)*
- [ ] **ML: session conversion prediction** — predict purchase probability from
      early-session behavior; MLflow experiment tracking; batch scoring written
      back to gold for dashboard consumption
- [ ] CI/CD for notebooks (GitHub Actions)

---

## 📁 Repository Structure

```
├── generator/            # Faker-based event producer
├── notebooks/            # Databricks pipeline notebooks
├── sql/                  # Gold DDL & analytical queries
├── docs/                 # Architecture diagrams, screenshots
└── README.md
```

---

## 👤 Author

**[Your Name]** — [LinkedIn](#) · [Portfolio](#)
