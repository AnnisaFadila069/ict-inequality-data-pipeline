# ICT Infrastructure & Internet Access — Data Pipeline

A data pipeline that processes BPS (Statistics Indonesia) ICT survey data across provinces,
producing a clean star-schema dataset ready for dashboard consumption.

---

## Table of Contents

- [Folder Structure](#folder-structure)
- [Data Groups](#data-groups)
- [Output Schema](#output-schema)
- [Configuration](#configuration)
- [How to Run](#how-to-run)

---

## Folder Structure

```
Repository TA/
│
├── Data Raw/                  # Original Excel files from BPS (do not modify)
├── Data Clean/                # Output of Step 1 — cleaned, normalized files
├── Data Standardized/         # Output of Step 2 — long-format with year & area
├── Data Enriched/             # Output of Step 3 — enriched with metrics
├── Data Dimension/            # Manually maintained dimension tables
├── Data Load/                 # Output of Step 4 — final star-schema tables
├── logs/                      # Auto-created — pipeline run logs
│
├── Tasks/
│   ├── 1_cleansing.py
│   ├── 2_standardization.py
│   ├── 3_enrichment.py
│   ├── 4_load.py
│   ├── 5_load_to_onedrive.py
│   │
│   ├── Cleansing/             # Cleansing sub-modules
│   │   ├── data_internet.py       — clean AI/AIA/AIE/AIL/AIP/BTS/CPS/IFC files
│   │   ├── pdrb.py                — clean PDRB (regional GDP) file
│   │   ├── total_penduduk.py      — clean total population file
│   │   └── rename_indicators.py   — standardize indicator names
│   │
│   ├── Standarization/        # Standardization sub-modules
│   │   ├── _1_transform_to_long.py    — melt wide columns into long format
│   │   ├── _2_add_year_column.py      — add year from filename or .env
│   │   └── _3_add_area_category.py    — add urban / rural / all_area label
│   │
│   └── Enrichment/            # Enrichment sub-modules
│       ├── _1_combine_data.py             — group and merge files by indicator group
│       ├── _2_convert_number_to_percentage.py  — value_num / nov × 100 → value_pct
│       ├── _3__convert_percentage_to_number.py — value_pct / 100 × population → value_num
│       ├── _4_calculate_ratio.py          — urban_pct / rural_pct per indicator
│       └── _5_calculate_gini_index.py     — Gini across provinces & within province
│
├── Run/
│   ├── run_all.py             # Run all 5 pipeline steps in sequence
│   ├── run_all.bat            # Batch wrapper for Task Scheduler
│   ├── send_warning.py        # Send a reminder email before the pipeline
│   ├── send_warning.bat       # Batch wrapper for Task Scheduler
│   └── setup_schedule.py      # Apply schedule settings from .env to Task Scheduler
│
├── Utils/
│   └── email_utils.py         # Shared SMTP email helper
│
├── .env                       # Path and email configuration (see Configuration section)
└── README.md
```

---

## Data Groups
The required datasets can be accessed in this section.
[https://its.id/m/data-group](https://its.id/m/data-group)

---

## Output Schema
### Pipeline Overview

```
Data Raw  →  Data Clean  →  Data Standardized  →  Data Enriched  →  Data Load  →  OneDrive
             (1_cleansing)   (2_standardization)    (3_enrichment)   (4_load)    (5_load_to_onedrive)
```

| Step | Script | Description |
|------|--------|-------------|
| 1 | `1_cleansing.py` | Fix column names, remove noise, normalize raw Excel files |
| 2 | `2_standardization.py` | Reshape wide → long format, add `year` and `area_category` columns |
| 3 | `3_enrichment.py` | Compute percentages, ratios, and Gini index |
| 4 | `4_load.py` | Build dimension and fact tables into star schema |
| 5 | `5_load_to_onedrive.py` | Upsert final tables to OneDrive sync folder |

---

### Dimension Tables

| Table | Primary Key | Description |
|-------|-------------|-------------|
| `dim_province` | `id_province` | Province names (BPS codes 11–97, `0` = Indonesia) |
| `dim_indicator` | `id_indicator` | Indicator labels, groups, and descriptions |
| `dim_area` | `area_category` | Area labels: `urban`, `rural`, `all_area`, `indonesia`, etc. |
| `dim_year` | `year` | Available years (2021–2025) |
| `dim_normalization` | `id_method` | Normalization methods: raw, per village, per GDP |

### Fact Tables

| Table | Primary Key | Description |
|-------|-------------|-------------|
| `fact_ict` | `id_province, id_indicator, year, area_category` | All ICT indicator values (`value_pct`, `value_num`) |
| `fact_ratio` | `id_province, id_indicator, year` | Urban-to-rural ratio per indicator |
| `fact_gini` | `id_indicator, year, id_method` | Gini index across provinces, two normalizations |
| `fact_gini_province` | `id_province, year` | Gini index within each province (all_area) |
| `fact_nov` | `id_province, area_category, year` | Number of villages per province and area |
| `fact_population` | `id_province, area_category, year` | Total population per province and area |
| `fact_pdrb` | `id_province, year` | Regional GDP per province |

### Star Schema Diagram

```
                        dim_normalization
                              │ id_method
                              │
dim_province    dim_indicator │         dim_area      dim_year
id_province ──┐  id_indicator─┤──────────┤             │
              │               │          │ area_category│ year
              ▼               ▼          ▼              ▼
          ┌─────────────────────────────────────────────────┐
          │                  fact_ict                       │
          │  id_province, id_indicator, year, area_category  │
          │  value_pct, value_num                            │
          └──────────────────────────────────────────────────┘

          ┌──────────────────┐   ┌──────────────────────────┐
          │   fact_ratio     │   │       fact_gini           │
          │  id_province     │   │  id_indicator, year       │
          │  id_indicator    │   │  id_method, gini          │
          │  year, ratio     │   └──────────────────────────┘
          └──────────────────┘
          ┌──────────────────┐   ┌──────────────────────────┐
          │ fact_gini_prov   │   │  fact_nov / fact_pdrb /   │
          │  id_province     │   │  fact_population          │
          │  year, gini      │   │  (reference tables)       │
          └──────────────────┘   └──────────────────────────┘
```

## How to Run

### 1. Clone the Repository
```bash
git clone https://github.com/AnnisaFadila069/ict-inequality-data-pipeline.git
cd ict-inequality-data-pipeline
```

### 2. Create and Activate Virtual Environment
```bash
# Create virtual environment
python -m venv .venv

# Activate (Windows)
.venv\Scripts\activate

# Activate (Mac / Linux)
source .venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Setup Environment Variables
Copy the example env file and fill in your local paths and email credentials:
```bash
# Windows
copy .env.example .env

# Mac / Linux
cp .env.example .env
```
Then open `.env` and update all values.

### 5. Run All Pipelines at Once
```bash
python Run\run_all.py
```
This runs all 5 steps in sequence and sends email notifications on success or failure.
Logs are saved to the `logs/` folder.

### 6. Run Individual Steps
Each script is self-contained and can be re-run independently:
```bash
cd Tasks
python 1_cleansing.py
python 2_standardization.py
python 3_enrichment.py
python 4_load.py
python 5_load_to_onedrive.py
```

### 7. Automated Scheduling (Windows Task Scheduler)

The schedule is controlled by two variables in `.env`
To apply the schedule (run once after setup, or again after changing the values above):
```bash
python Run\setup_schedule.py
```

This registers two Windows Task Scheduler tasks automatically:

| Task | Time | Action |
|------|------|--------|
| `TA_SendWarningEmail` | `PIPELINE_RUN_TIME` − `WARNING_HOURS_BEFORE` hours | Sends reminder email to `EMAIL_FROM_ADDRESS` |
| `TA_RunAllPipelines` | `PIPELINE_RUN_TIME` | Runs all pipelines automatically |

#### Email Notifications

| Event | Recipients |
|-------|-----------|
| Reminder (08:00) | `EMAIL_FROM_ADDRESS` |
| All pipelines succeeded | `EMAIL_FROM_ADDRESS` and `EMAIL_TO_ADDRESS` |
| Any pipeline failed | `EMAIL_FROM_ADDRESS` |
