from pyspark.sql import functions as F
from src.common.spark_session import get_spark_session
def main():
    spark=get_spark_session("QualityValidation")
    s=spark.read.parquet("data/silver/telemetry")
    checks={
      "null_event_ids":s.filter(F.col("event_id").isNull()).count(),
      "null_devices":s.filter(F.col("device_id").isNull()).count(),
      "negative_latency":s.filter(F.col("latency_ms")<0).count(),
      "invalid_packet_loss":s.filter(~F.col("packet_loss_pct").between(0,100)).count(),
      "duplicate_event_ids":s.groupBy("event_id").count().filter("count > 1").count()
    }
    print(f"records: {s.count():,}")
    for k,v in checks.items(): print(f"{k}: {v:,}")
    spark.stop()
    if sum(checks.values()): raise ValueError("Silver quality validation failed")
if __name__=="__main__": main()
