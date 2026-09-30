from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.common.spark_session import get_spark_session
def main():
    spark=get_spark_session("BronzeToSilver")
    b=spark.read.parquet("data/bronze/telemetry")
    good=(b.filter(F.col("event_id").isNotNull() & F.col("device_id").isNotNull() & F.col("access_point_id").isNotNull())
          .filter(F.col("latency_ms")>=0).filter(F.col("packet_loss_pct").between(0,100))
          .filter(F.col("signal_strength_dbm").between(-120,0))
          .filter(F.col("connection_status").isin("CONNECTED","DEGRADED","DISCONNECTED")))
    w=Window.partitionBy("event_id").orderBy(F.col("ingested_at").desc())
    s=(good.withColumn("rn",F.row_number().over(w)).filter("rn=1").drop("rn")
       .withColumn("is_high_latency",F.col("latency_ms")>100)
       .withColumn("is_high_packet_loss",F.col("packet_loss_pct")>2)
       .withColumn("is_weak_signal",F.col("signal_strength_dbm") < -75)
       .withColumn("sla_violation",F.col("is_high_latency")|F.col("is_high_packet_loss")|
                   F.col("is_weak_signal")|(F.col("connection_status")=="DISCONNECTED")))
    s.repartition("event_date").write.mode("overwrite").partitionBy("event_date").parquet("data/silver/telemetry")
    b.join(s.select("event_id"),"event_id","left_anti").write.mode("overwrite").parquet("data/silver/rejected_telemetry")
    print(f"Silver valid records: {s.count():,}")
    spark.stop()
if __name__=="__main__": main()
