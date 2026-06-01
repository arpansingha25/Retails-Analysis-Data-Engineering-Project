# ABC Retail Solutions — Data Engineering Pipeline
### Microsoft Fabric · PySpark · Delta Lake · Power BI

---

## Overview

This project implements a complete end-to-end retail data engineering pipeline for **ABC Retail Solutions**, a multinational retail company operating across multiple cities through online and offline channels.

The pipeline ingests raw transactional data from two source systems, resolves data quality issues (duplicates, missing values, inconsistent formatting, invalid records), masks Personally Identifiable Information (PII), computes business KPIs, and delivers insights through an interactive Power BI dashboard — all within a single **Microsoft Fabric** workspace.

---

## Architecture

```
Excel Source Files
        │
        ▼
┌─────────────────────────────────────────────────────┐
│            Microsoft Fabric Workspace               │
│                                                     │
│  ┌──────────────────────────────────────────────┐   │
│  │  Lakehouse — retail_lakehouse (OneLake)      │   │
│  │                                              │   │
│  │  Files/bronze/   ← raw Excel uploads         │   │
│  │  Tables/         ← managed Delta tables      │   │
│  │    retail_silver         (cleaned rows)      │   │
│  │    gold_revenue_by_category                  │   │
│  │    gold_revenue_by_city                      │   │
│  │    gold_top_products                         │   │
│  │    gold_monthly_trend                        │   │
│  │    gold_revenue_by_channel                   │   │
│  └──────────────────────────────────────────────┘   │
│                      │                              │
│                      ▼                              │
│  ┌──────────────────────────────────────────────┐   │
│  │  Fabric Notebook — retail_transformation     │   │
│  │  PySpark transformation pipeline             │   │
│  └──────────────────────────────────────────────┘   │
│                      │                              │
│                      ▼                              │
│  ┌──────────────────────────────────────────────┐   │
│  │  SQL Analytics Endpoint                      │   │
│  │  Auto-generated · queryable via T-SQL        │   │
│  └──────────────────────────────────────────────┘   │
│                      │                              │
│                      ▼                              │
│  ┌──────────────────────────────────────────────┐   │
│  │  Power BI Report — DirectLake mode           │   │
│  │  4 dashboard pages · published to workspace  │   │
│  └──────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

---

## Repository Structure

```
ABC-Retail-Data-Pipeline/
│
├── Code/
│   └── retail_pipeline.py   # Python script exported from notebook
│
├── Notebooks/
│   └── retail_transformation_notebook  # Fabric Notebook (.ipynb export)
│
├── Data/
│   └── raw/
│       ├── retail_data1.xlsx
│       ├── retail_data2.xlsx
│       └── product_details.xlsx
│
├── Documentation/
│   └── Project_Documentation.docx      # Architecture, DFD, assumptions, logic
│
├── PowerBI/
│   ├── Retail_Dashboard.pbix
│   ├── page1_executive_summary.png
│   ├── page2_revenue & regional analysis.png  
│   └── page3_product_performance.png  
│     
└── README.md
```
---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Platform | Microsoft Fabric (Trial) | Unified analytics workspace |
| Storage | Fabric Lakehouse (OneLake) | Bronze / Silver / Gold zones |
| Processing | Fabric Notebook (PySpark) | Data transformation pipeline |
| File format | Delta Lake | Versioned, queryable tables |
| SQL layer | SQL Analytics Endpoint | Ad-hoc queries, semantic model |
| Reporting | Power BI (DirectLake) | Dashboard, KPIs, interactivity |
| Language | Python 3 / PySpark | Transformation logic |
| PII protection | SHA-256 hashing | One-way irreversible masking |

---

## Data Sources

| Dataset | Rows | Description |
|---|---|---|
| `retail_data1.xlsx` | 4,243 | Transaction records from source system 1 |
| `retail_data2.xlsx` | 4,251 | Transaction records from source system 2 |
| `product_details.xlsx` | 10 | Product dimension / reference table |

---

## Pipeline Stages

### Stage 1 — Ingestion
Raw Excel files are uploaded manually into the `Files/bronze/` folder of the Fabric Lakehouse. This simulates a production ingestion layer where files would arrive from source systems automatically. Files are read into PySpark DataFrames using `pandas` (for reliable `.xlsx` support) and then converted to Spark DataFrames via `spark.createDataFrame()`.

### Stage 2 — Transformation and Cleaning
The following cleaning operations are applied in sequence:

| Issue | Resolution |
|---|---|
| Inconsistent column names | Stripped, lowercased, spaces replaced with underscores |
| Duplicate rows | Removed using `dropDuplicates()` |
| Missing categorical values | Filled with `'Unknown'` (city, category, payment_method) |
| Missing customer info | Filled with `'Not Provided'` |
| Invalid quantity (≤ 0) | Rows removed — represent returns or data entry errors |
| Inconsistent text casing | Standardized to Title Case using `initcap(trim())` |
| Abbreviated category values | Mapped to full names (e.g. `"Elec"` → `"Electronics"`) |
| Mixed date formats | Parsed using `coalesce()` across multiple `to_date()` patterns |
| GregorianCalendar date strings | Extracted with `regexp_extract()` and reconstructed with `make_date()` |

### Stage 3 — PII Masking
Customer `email` and `phone` columns are masked using **SHA-256 hashing** via PySpark's built-in `sha2(col, 256)` function. The original columns are dropped and replaced with `email_masked` and `phone_masked`. SHA-256 was chosen over simple redaction because it is:
- **One-way** — cannot be reversed to recover the original value
- **Consistent** — the same input always produces the same hash (useful for counting unique customers without exposing PII)
- **Standards-compliant** — satisfies data protection requirements including India's DPDP Act

### Stage 4 — Enrichment
The cleaned transaction data is left-joined with the `product_details` dimension table on `product_id`. This adds standardized product names and category information. Missing unit prices are back-filled from the `standard_product_price` column in the dimension table.

### Stage 5 — Revenue Calculation
```
revenue = (unit_price_final - discount) × quantity
```
Rows with negative revenue (returns or errors) are removed after calculation.

### Stage 6 — Aggregation (Gold tables)
Five Gold Delta tables are written for direct Power BI consumption:

| Table | Grain | Key Metrics |
|---|---|---|
| `gold_revenue_by_category` | Category | total_revenue, transaction_count, avg_order_value |
| `gold_revenue_by_city` | City | total_revenue, transaction_count, unique_customers |
| `gold_top_products` | Product | units_sold, total_revenue |
| `gold_monthly_trend` | Year + Month | monthly_revenue, transaction_count |
| `gold_revenue_by_channel` | Sales channel | total_revenue, transaction_count |

---

## KPIs

| KPI | Definition |
|---|---|
| Total Revenue | `SUM(revenue)` across all transactions |
| Total Transactions | `COUNT(*)` of all valid rows |
| Average Order Value | `Total Revenue / Total Transactions` |
| Revenue by Category | Revenue aggregated per product category |
| Revenue by City | Revenue aggregated per transaction city |
| Top Products | Products ranked by total revenue |
| Monthly Revenue Trend | Revenue grouped by year and month |
| Revenue MoM % | Month-over-month percentage change in revenue |

---

## Power BI Dashboard

The report consists of four pages published to the Fabric workspace via DirectLake mode (no data export or import — Power BI reads Delta files directly from OneLake).

| Page | Contents |
|---|---|
| Executive Summary | KPI cards, monthly revenue line chart, channel donut, year slicer |
| Revenue Analysis | Revenue by category bar chart, city map, quarter slicer |
| Product Performance | Top 10 products bar chart, product table, category treemap |
| Regional Insights | Filled city map, top cities bar chart, conditional formatted table |

---

## How to Reproduce

### Prerequisites
- Microsoft Fabric trial account (`app.fabric.microsoft.com`)
- Workspace set to **Fabric Trial** license mode
- Source Excel files available locally

### Steps

**1. Set up the Lakehouse**
```
Fabric workspace → + New item → Lakehouse → Name: retail_lakehouse
Files → New subfolder → bronze
Upload: retail_data1.xlsx, retail_data2.xlsx, product_details.xlsx
```

**2. Create and run the notebook**
```
Fabric workspace → + New item → Notebook → Name: retail_transformation_notebook
Add lakehouse → retail_lakehouse
Run all cells in order (Shift + Enter per cell)
```

**3. Verify Delta tables**
```
retail_lakehouse → Tables → confirm retail_silver and all gold_* tables exist
SQL Analytics Endpoint → run validation queries
```

**4. Open the Power BI report**
```
Fabric workspace → + New item → Report
Select semantic model: retail_lakehouse
Add visuals and DAX measures as documented in Project_Documentation.docx
File → Save → Retail Sales Dashboard
```

**5. Schedule the notebook (optional)**
```
retail_transformation_notebook → Schedule → Daily 01:00 AM → Apply
```

---

## Assumptions

- Rows with `quantity <= 0` are treated as returns or invalid entries and excluded from analysis.
- Missing `city`, `category`, and `payment_method` values are filled with `'Unknown'` rather than dropped, to preserve transaction revenue records.
- Where `unit_price` is missing, the `standard_product_price` from the product dimension table is used as a substitute.
- Date strings in GregorianCalendar object format (`YEAR=2024,MONTH=0,...`) are parsed separately and merged with standard date formats.
- Category abbreviations (`"Elec"`, `"Furn"`) are treated as data entry errors and mapped to their full standard names.
- The pipeline is designed to be idempotent — re-running it always produces the same output (Delta tables use `overwrite` mode).

---

## Notebook Cell Reference

| Cell | Purpose |
|---|---|
| Cell 1 | Read Bronze Excel files using pandas |
| Cell 2 | Convert pandas → Spark DataFrames |
| Cell 3 | Standardize column names |
| Cell 4 | Union retail_data1 and retail_data2 |
| Cell 5 | Remove duplicate rows |
| Cell 6 | Handle missing values and invalid quantities |
| Cell 7 | Standardize text, parse mixed date formats |
| Cell 7b | Standardize category abbreviations |
| Cell 8 | PII masking — SHA-256 hash email and phone |
| Cell 9 | Enrich with product dimension table |
| Cell 10 | Calculate revenue column |
| Cell 11 | Write Silver Delta table |
| Cell 12 | Write all Gold aggregation tables |
| Cell 13 | Final summary and validation printout |

---

## Arpan SIngha

Submitted as part of the NeoStats Data Engineering Internship Assessment.

> This project is entirely original work completed independently with help of AI tools such as Claude & Github Copilot as part of the assessment requirements.
