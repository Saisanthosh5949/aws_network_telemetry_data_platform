-- Example Athena DDL. Replace YOUR_BUCKET.
CREATE DATABASE IF NOT EXISTS network_gold;

CREATE EXTERNAL TABLE IF NOT EXISTS network_gold.regional_metrics (
  region string,
  events bigint,
  unique_devices bigint,
  access_points bigint,
  avg_latency_ms double,
  p95_latency_ms double,
  avg_packet_loss_pct double,
  avg_download_mbps double,
  sla_violations bigint
)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET/network-telemetry/gold/regional_metrics/';
