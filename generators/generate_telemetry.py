import argparse
from pyspark.sql import functions as F
from src.common.spark_session import get_spark_session

def main(events):
    spark=get_spark_session("GenerateTelemetry")
    df=(spark.range(events)
      .withColumn("event_id",F.expr("uuid()"))
      .withColumn("device_id",F.format_string("DEV-%08d",(F.floor(F.rand(1)*100000)+1).cast("long")))
      .withColumn("access_point_id",F.format_string("AP-%06d",(F.floor(F.rand(2)*2000)+1).cast("long")))
      .withColumn("event_timestamp",F.expr("current_timestamp() - INTERVAL 1 SECOND * CAST(rand(3)*604800 AS INT)"))
      .withColumn("signal_strength_dbm",F.round(-30-F.rand(4)*65,2))
      .withColumn("latency_ms",F.round(5+F.rand(5)*180,2))
      .withColumn("packet_loss_pct",F.round(F.rand(6)*8,3))
      .withColumn("download_mbps",F.round(10+F.rand(7)*900,2))
      .withColumn("upload_mbps",F.round(5+F.rand(8)*250,2))
      .withColumn("connection_status",F.when(F.rand(9)<.04,"DISCONNECTED").when(F.rand(10)<.08,"DEGRADED").otherwise("CONNECTED"))
      .withColumn("bytes_transferred",(F.rand(11)*5000000000).cast("long"))
      .select("event_id","device_id","access_point_id","event_timestamp","signal_strength_dbm","latency_ms",
              "packet_loss_pct","download_mbps","upload_mbps","connection_status","bytes_transferred"))
    df=(df.withColumn("device_id",F.when(F.rand(20)<.002,F.lit(None)).otherwise(F.col("device_id")))
          .withColumn("latency_ms",F.when(F.rand(21)<.002,F.lit(-25.0)).otherwise(F.col("latency_ms")))
          .withColumn("packet_loss_pct",F.when(F.rand(22)<.002,F.lit(125.0)).otherwise(F.col("packet_loss_pct"))))
    df.write.mode("overwrite").parquet("data/raw/telemetry")
    print(f"Generated {events:,} telemetry events")
    spark.stop()
if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument("--events",type=int,default=1000000)
    main(p.parse_args().events)
