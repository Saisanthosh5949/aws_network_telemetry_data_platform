from pyspark.sql import functions as F
from src.common.spark_session import get_spark_session
def main():
    spark=get_spark_session("GoldMetrics")
    t=spark.read.parquet("data/silver/telemetry")
    aps=F.broadcast(spark.read.parquet("data/raw/access_points"))
    dev=F.broadcast(spark.read.parquet("data/raw/devices"))
    e=(t.join(aps,"access_point_id","left").join(dev,"device_id","left")
       .withColumn("event_hour",F.date_trunc("hour","event_timestamp")))
    hourly=(e.groupBy("event_hour","region").agg(
        F.count("*").alias("events"),F.round(F.avg("latency_ms"),2).alias("avg_latency_ms"),
        F.expr("percentile_approx(latency_ms,0.95)").alias("p95_latency_ms"),
        F.round(F.avg("packet_loss_pct"),3).alias("avg_packet_loss_pct"),
        F.round(F.avg("download_mbps"),2).alias("avg_download_mbps"),
        F.sum(F.col("sla_violation").cast("int")).alias("sla_violations"),
        F.sum((F.col("connection_status")=="DISCONNECTED").cast("int")).alias("disconnected_events"))
        .withColumn("availability_pct",F.round((1-F.col("disconnected_events")/F.col("events"))*100,4)))
    ap=(e.groupBy("access_point_id","region","model").agg(
        F.count("*").alias("events"),F.approx_count_distinct("device_id").alias("unique_devices"),
        F.round(F.avg("latency_ms"),2).alias("avg_latency_ms"),
        F.round(F.avg("packet_loss_pct"),3).alias("avg_packet_loss_pct"),
        F.sum(F.col("sla_violation").cast("int")).alias("sla_violations")))
    region=e.groupBy("region").agg(
        F.count("*").alias("events"),F.approx_count_distinct("device_id").alias("unique_devices"),
        F.approx_count_distinct("access_point_id").alias("access_points"),
        F.round(F.avg("latency_ms"),2).alias("avg_latency_ms"),
        F.expr("percentile_approx(latency_ms,0.95)").alias("p95_latency_ms"),
        F.round(F.avg("packet_loss_pct"),3).alias("avg_packet_loss_pct"),
        F.round(F.avg("download_mbps"),2).alias("avg_download_mbps"),
        F.sum(F.col("sla_violation").cast("int")).alias("sla_violations"))
    hourly.write.mode("overwrite").parquet("data/gold/hourly_network_metrics")
    ap.write.mode("overwrite").parquet("data/gold/access_point_metrics")
    region.write.mode("overwrite").parquet("data/gold/regional_metrics")
    region.orderBy("region").show(truncate=False)
    spark.stop()
if __name__=="__main__": main()
