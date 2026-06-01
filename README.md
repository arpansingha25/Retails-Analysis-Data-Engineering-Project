# Retails-Analysis-Data-Engineering-Project

This repository now follows the required structure:

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

## Run pipeline

```bash
python ABC-Retail-Data-Pipeline/Code/retail_pipeline.py
```

The script writes output to:

`ABC-Retail-Data-Pipeline/Data/processed/curated_retail_data.csv`
