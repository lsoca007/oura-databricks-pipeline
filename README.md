# Oura Ring Medallion Data Pipeline with Databricks

End-to-end medallion architecture pipeline built on 2 years of personal Oura Ring health data using PySpark, Delta Lake, and Databricks Unity Catalog.

---

## Architecture

```
Raw CSVs (6 files, ~453,000 rows)
            │
            ▼
    ┌─────────────────┐
    │   Bronze Layer  │  6 Delta tables — raw ingestion, no transformations
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │   Silver Layer  │  5 Delta tables — typed, cleaned, JSON parsed, UTC normalized
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │    Gold Layer   │  3 Delta tables — aggregated, joined, ready for analysis
    └─────────────────┘
```

---

## Tech Stack

- **Platform:** Databricks (Unity Catalog)
- **Language:** Python / PySpark
- **Storage:** Delta Lake
- **Architecture:** Medallion (Bronze / Silver / Gold)

---

## Source Data

6 CSV exports from the Oura Ring personal health app covering approximately 2 years of data (2024–2026). All files use semicolon delimiters.

"Source data is personal health data exported from the Oura Ring API. Raw data is not included in this repo."

| File | Description | Rows |
|------|-------------|------|
| `dailycardiovascularage.csv` | Daily vascular age and pulse wave velocity | ~687 |
| `dailyreadiness.csv` | Readiness score with nested JSON contributor sub-scores | ~693 |
| `dailysmoothedcardiovascularage.csv` | Smoothed cardiovascular metrics | ~731 |
| `dailystress.csv` | Daily stress and recovery minutes | ~724 |
| `heartrate.csv` | High-frequency heart rate samples (~every 20 seconds) | ~450,141 |
| `workout.csv` | Individual workout sessions with type, duration, calories | ~69 |

---

## Pipeline Details

### Bronze Layer
Ingests all 6 CSVs as-is from a Databricks Unity Catalog Volume into Delta tables. No type casting or transformations — all columns land as `StringType` with an added `ingested_at` timestamp. Provides a permanent, queryable copy of the raw data.

**Tables:** `bronze_cardiovascular_age`, `bronze_readiness`, `bronze_smoothed_cardiovascular_age`, `bronze_stress`, `bronze_heartrate`, `bronze_workout`

### Silver Layer
Cleans and normalizes each bronze table:
- Casts `day` columns to `DateType` and timestamps to `TimestampType`
- Casts numeric columns to `IntegerType` / `DoubleType`
- Parses the nested JSON `contributors` column in readiness into 9 individual typed columns using `from_json()` and a defined `StructType` schema
- Normalizes mixed-timezone timestamps to UTC in heartrate and workout tables
- Computes a derived `duration_minutes` column in workout from start and end datetimes

**Tables:** `silver_cardiovascular_age`, `silver_readiness`, `silver_stress`, `silver_heartrate`, `silver_workout`

### Gold Layer
Produces three analytical tables:

- **`gold_heartrate_daily`** — Aggregates 450,141 high-frequency heart rate rows down to daily avg/min/max BPM per source (awake, sleep, rest). Kept separate from the daily summary due to the volume of raw data.
- **`gold_workout_summary`** — Aggregates individual workout sessions to daily totals (calories, duration, session count).
- **`gold_daily_summary`** — Flagship table. One row per day joining all silver tables and gold aggregates on a single `day` key. 24 columns covering cardiovascular, readiness, stress, heart rate, and workout metrics side by side.

---

## Key Technical Highlights

**Nested JSON parsing**
The `dailyreadiness.csv` file contains a JSON object embedded as a string inside each row under a `contributors` column. In silver, this is parsed using `from_json()` with a manually defined `StructType` schema, exploding it into 9 individual typed columns (`activity_balance`, `hrv_balance`, `recovery_index`, etc.).

```python
contributors_schema = StructType([
    StructField("activity_balance", IntegerType()),
    StructField("hrv_balance", IntegerType()),
    StructField("recovery_index", IntegerType()),
    # ... 6 more fields
])

df = df.withColumn("contrib", from_json(col("contributors"), contributors_schema))
```

**High-frequency data aggregation**
The heartrate file contains ~450,141 rows sampled every ~20 seconds. Rather than joining this directly to the daily summary (which would explode row counts), a dedicated `gold_heartrate_daily` table aggregates the data to daily avg/min/max BPM per source before joining.

**Cross-domain daily join**
`gold_daily_summary` joins 5 silver tables and 2 gold aggregates on a single `day` key using LEFT JOINs to preserve all cardiovascular age dates even when other metrics are missing.

**UTC normalization**
Heartrate timestamps and workout start/end datetimes contain mixed timezone offsets. All are normalized to UTC using `to_timestamp()` in the silver layer before any aggregation or joining.

---

## Sample Output

Quarterly cardiovascular age trend from `gold_daily_summary`:

| Quarter | Avg Vascular Age | Avg Stress | Avg Min BPM (Awake) | Days |
|---------|-----------------|------------|----------------------|------|
| 2024 Q2 | 23.4 | 2,420 | 59.3 | 45 |
| 2024 Q3 | 24.0 | 1,937 | 59.0 | 92 |
| 2024 Q4 | 24.5 | 1,385 | 60.4 | 91 |
| 2025 Q1 | 25.5 | 1,960 | 61.5 | 90 |
| 2025 Q2 | 25.5 | 1,982 | 62.3 | 89 |
| 2025 Q3 | 25.8 | 1,996 | 61.7 | 92 |
| 2025 Q4 | 25.9 | 2,421 | 61.5 | 84 |
| 2026 Q1 | 26.9 | 1,463 | 61.4 | 72 |
| 2026 Q2 | 27.6 | 4,725 | 60.8 | 32 |

Correlation analysis between daily metrics and vascular age (computed in gold layer):

| Metric | Correlation with Vascular Age |
|--------|-------------------------------|
| Min BPM (awake) | +0.369 — strongest signal |
| Avg BPM (awake) | +0.250 |
| Readiness score | -0.243 |
| Workout calories | -0.208 |
| Stress high | +0.146 |
| Recovery index | -0.142 |

---

## How to Run

1. Create a Databricks workspace and enable Unity Catalog
2. Create a schema: `CREATE SCHEMA IF NOT EXISTS workspace.oura`
3. Create a Volume at `workspace.oura.raw_data` and upload all 6 CSV files
4. Create a single-node cluster (Databricks Runtime 14.x or later)
5. Run the notebooks in order:
   - `bronze/01_bronze_ingestion.py`
   - `silver/02_silver_cleaning.py`
   - `gold/03_gold_aggregation.py`

---

## Repository Structure

```
oura-databricks-pipeline/
├── README.md
├── bronze/
│   └── 01_bronze_ingestion.py
├── silver/
│   └── 02_silver_cleaning.py
└── gold/
    └── 03_gold_aggregation.py
```
