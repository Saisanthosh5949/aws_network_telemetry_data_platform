from pyspark.sql import functions as F
from src.common.spark_session import get_spark_session
def main():
    spark=get_spark_session("RawToBronze")
    df=(spark.read.parquet("data/raw/telemetry")
        .withColumn("ingested_at",F.current_timestamp())
        .withColumn("source_system",F.lit("NETWORK_TELEMETRY"))
        .withColumn("event_date",F.to_date("event_timestamp")))
    df.repartition("event_date").write.mode("overwrite").partitionBy("event_date").parquet("data/bronze/telemetry")
    print(f"Bronze records: {df.count():,}")
    spark.stop()
if __name__=="__main__": main()
