# 🛒 E-Commerce Clickstream Lakehouse on Azure

An end-to-end streaming data platform that ingests simulated e-commerce
clickstream events, processes them through a medallion lakehouse architecture
on Azure, and serves a star schema to Power BI for funnel analytics.

**Skills demonstrated:** Data Engineering · Data Analysis · (ML — planned, see Roadmap)

---

## 📐 Architecture

```
┌──────────────┐    ┌──────────────┐    ┌─────────────────────────────┐    ┌──────────────┐
│ Python Event │────│ Azure Event  │────│   Azure Databricks          │────│   Power BI   │
│  Generator   │    │    Hubs      │    │  Structured Streaming       │    │  (Import via │
│   (Faker)    │    │              │    │  Bronze ─▶ Silver ─▶ Gold  │    │SQL Warehouse)│
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
| **Silver** | Cleaned, validated, deduplicated | timestamp parsing, quality filters |
| **Gold**   | Analytics-ready star schema | Incremental MERGE loads, surrogate keys, SCD Type 2 |

### Gold Star Schema

```
Fact Events
        ┌────────────────┐             ┌──────────────┐
        │  dim_browser   │             │   dim_date   │
        └────────────────┘             └──────────────┘
                │                              │
                └──────────┐      ┌────────────┘
                           │      │
┌──────────────┐       ┌───────────────┐       ┌──────────────┐
│  dim_country │───────│  fact_events  │───────│  dim_device  │
└──────────────┘       └───────────────┘       └──────────────┘
                        |      |       |
             ┌──────────┘      |       └───────────┐
             |                 |                   |
    ┌────────────────┐ ┌────────────────┐   ┌──────────────┐
    │  dim_referrer  │ │  dim_product   │   │   dim_os     │
    └────────────────┘ └────────────────┘   └──────────────┘


Fact Orders

┌──────────────┐       ┌───────────────┐       ┌──────────────┐       ┌──────────────┐
│  dim_country │───────│  fact_orders  │───────│  dim_product │───────│ dim_category │
└──────────────┘       └───────────────┘       └──────────────┘       └──────────────┘
                          |      
               ┌──────────┘       
               |                     
        ┌────────────────┐ 
        │  dim_date      │ 
        └────────────────┘ 

Fact Sessions

        ┌────────────────┐             ┌──────────────┐
        │  dim_browser   │             │   dim_date   │
        └────────────────┘             └──────────────┘
                │                              │
                └──────────┐      ┌────────────┘
                           │      │
┌──────────────┐       ┌───────────────┐       ┌──────────────┐
│  dim_country │───────│ fact_sessions │───────│  dim_device  │
└──────────────┘       └───────────────┘       └──────────────┘
                          |        |
               ┌──────────┘        └───────────┐
               |                               |
        ┌────────────────┐             ┌──────────────┐
        │  dim_referrer  │             │   dim_os     │
        └────────────────┘             └──────────────┘
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
`event_type`, `product_id`, `event_timestamp`, `device`, `referrer`, `browser`, `os`, `country`, `referrer`,`category`, `revenue`.

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

---

## 📁 Repository Structure

```
├── generator/            # Faker-based event producer
├── notebooks/            # Databricks pipeline notebooks
├── docs/                 # Architecture diagrams, screenshots
└── README.md
```

---

## Author

Nqobile Msibi

Data Engineer | Data Analyst | Analytics Engineer | Cloud Data Enthusiast

- LinkedIn: https://www.linkedin.com/in/nqobile-msibi/
