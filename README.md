# ICT Infrastructure & Internet Access — Data Pipeline

A data pipeline that processes BPS (Statistics Indonesia) ICT survey data across provinces,
producing a clean star-schema dataset ready for dashboard consumption.

---

## Table of Contents

- [Pipeline Overview](#pipeline-overview)
- [Folder Structure](#folder-structure)
- [Data Groups](#data-groups)
- [Task Files](#task-files)
- [Output Schema](#output-schema)
- [Configuration](#configuration)
- [How to Run](#how-to-run)

---

## Pipeline Overview

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
├── .env                       # Path configuration (see Configuration section)
└── README.md
```

---

## Data Groups
The required datasets can be accessed in this section.
[text](https://its.id/m/data-group)

---

## Task Files

### `1_cleansing.py`
Reads every file from **Data Raw**, applies group-specific cleaning rules, and writes to **Data Clean**.

### `2_standardization.py`
Reads from **Data Clean**, reshapes each file into long format, and writes to **Data Standardized**.

### `3_enrichment.py`
Reads from **Data Standardized**, runs 6 enrichment steps, and writes to **Data Enriched**.

### `4_load.py`
Reads from **Data Enriched** and **Data Dimension**, restructures into star schema, and writes to **Data Load**.

### `5_load_to_onedrive.py`
Upserts all files from **Data Load** to the OneDrive sync folder defined in `.env`.

---

## Output Schema

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

---

## Configuration

All paths and the target year are set in `.env` at the project root:

```env
YEAR=2024

BASE_FOLDER_RAW_DATA="E:\TA\Repository TA\Data Raw"
BASE_FOLDER_CLEAN_DATA="E:\TA\Repository TA\Data Clean"
BASE_FOLDER_STANDARDIZED_DATA="E:\TA\Repository TA\Data Standardized"
BASE_FOLDER_ENRICHED_DATA="E:\TA\Repository TA\Data Enriched"
BASE_FOLDER_LOAD_DATA="E:\TA\Repository TA\Data Load"
BASE_FOLDER_DIMENSION_DATA="E:\TA\Repository TA\Data Dimension"
BASE_FOLDER_ONE_DRIVE="E:\OneDrive\Tugas Akhir\Data Load"
```

> **To add a new year:** update `YEAR=` and place new raw files in `Data Raw/`.  
> The pipeline will pick up the year automatically from the filename where available,  
> or fall back to the `YEAR` value for files without a year in their name.

---

## How to Run

### Prerequisites

```bash
pip install -r requirements.txt
cp .env.example .env
```
Please Setup Environment Variables.

### Run the Full Pipeline (in order)

```bash
cd "E:\TA\Repository TA\Tasks"

python 1_cleansing.py
python 2_standardization.py
python 3_enrichment.py
python 4_load.py
python 5_load_to_onedrive.py
```

### Run Individual Steps

Each script is self-contained and reads from / writes to the folders defined in `.env`.
You can re-run any single step without re-running the entire pipeline.

```bash
# Re-run only enrichment after changing a formula
python 3_enrichment.py

# Re-sync to OneDrive after any load change
python 5_load_to_onedrive.py
```

### Expected Output per Step

```
Step 1 — 1_cleansing.py
  ✓ Writes cleaned files to:  Data Clean/

Step 2 — 2_standardization.py
  ✓ Writes long-format files to:  Data Standardized/

Step 3 — 3_enrichment.py
  ✓ Writes enriched files to:  Data Enriched/
    Includes: ratio_urban_rural.xlsx, gini_per_nov.xlsx,
              gini_per_pdrb.xlsx, gini_per_province.xlsx

Step 4 — 4_load.py
  ✓ Writes star-schema tables to:  Data Load/
    5 dimension tables + 7 fact tables

Step 5 — 5_load_to_onedrive.py
  ✓ Upserts all tables to:  <OneDrive path from .env>
    [NEW]    on first run
    [UPSERT] on subsequent runs (no duplicates)
```
