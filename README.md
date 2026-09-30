# AWS Network Telemetry Data Platform

PySpark portfolio project simulating high-volume Wi-Fi/network telemetry.

## Architecture
Network Devices -> PySpark Generator -> Bronze -> Silver -> Gold -> AWS S3 -> Glue -> Athena

Gold datasets include hourly network KPIs, access-point performance, and regional performance.

## Setup
Requires Python 3.11 and Java 17.

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Run
Start small:
```powershell
python run_pipeline.py --events 200000
```

Portfolio-scale default:
```powershell
python run_pipeline.py
```

## Individual stages
```powershell
python -m src.generators.generate_reference_data
python -m src.generators.generate_telemetry --events 1000000
python -m src.ingestion.raw_to_bronze
python -m src.transformations.bronze_to_silver
python -m src.transformations.build_gold_metrics
python -m src.quality.validate_pipeline
```

## AWS
Never commit AWS credentials. Configure AWS CLI/IAM locally.

```powershell
aws s3 sync data/bronze s3://YOUR_BUCKET/network-telemetry/bronze/
aws s3 sync data/silver s3://YOUR_BUCKET/network-telemetry/silver/
aws s3 sync data/gold s3://YOUR_BUCKET/network-telemetry/gold/
```

Skills: PySpark DataFrames, partitioning, Parquet, window functions, deduplication,
broadcast joins, p95 latency, approximate distinct counts, SLA metrics, Airflow, Athena and CI/CD.
