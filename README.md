# Retails-Analysis-Data-Engineering-Project

## Project Overview

This project demonstrates an end-to-end Data Engineering workflow for retail sales data.

The pipeline extracts data from multiple Excel sources, performs data cleaning and transformations, and generates a curated dataset used for Power BI reporting.

---

## Tech Stack

- Python
- PySpark
- Microsoft Fabric
- Excel
- Power BI

---

## Project Architecture

```text
Raw Excel Files 
       ↓
ETL Pipeline (Fabric)
       ↓
Curated Dataset (Sliver layer)
       ↓
Power BI Dashboard
```

---

## Folder Structure

```text
ABC-Retail-Data-Pipeline/
├── Code/
│   └── retail_pipeline.py
├── Notebooks/
│   └── retail_pipeline.ipynb
├── Data/
│   ├── raw/
│   │   ├── retail_data1.xlsx
│   │   ├── retail_data2.xlsx
│   │   └── product_details.xlsx
│   └── processed/
│       └── curated_retail_data.csv
├── Documentation/
│   └── Project_Documentation.docx
├── PowerBI/
│   ├── Retail_Dashboard.pbix
│   └── Dashboard_Screenshot.png
└── README.md
```

---

## Features

- Data Extraction
- Data Cleaning
- Data Transformation
- Data Validation
- Data Aggregation
- Dashboard Reporting

---

## Run pipeline

```bash
python ABC-Retail-Data-Pipeline/Code/retail_pipeline.py
```

The script writes output to:

`ABC-Retail-Data-Pipeline/Data/processed/curated_retail_data.csv`

---

## Dashboard Preview

![Dashboard](PowerBI/Dashboard_Screenshot.png)

---

Arpan Singha
