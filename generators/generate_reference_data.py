from pyspark.sql import functions as F
from src.common.spark_session import get_spark_session

def main():
    spark=get_spark_session("GenerateReferenceData")
    regions=["CENTRAL","WEST","MOUNTAIN","NORTHEAST","SOUTHEAST"]
    models=["AX6000","AX3000","WIFI6E","ENTERPRISE-X"]
    aps=(spark.range(1,2001)
         .withColumn("access_point_id",F.format_string("AP-%06d",F.col("id")))
         .withColumn("region",F.element_at(F.array(*[F.lit(x) for x in regions]),((F.col("id")-1)%5+1).cast("int")))
         .withColumn("model",F.element_at(F.array(*[F.lit(x) for x in models]),((F.col("id")-1)%4+1).cast("int")))
         .select("access_point_id","region","model"))
    devices=(spark.range(1,100001)
         .withColumn("device_id",F.format_string("DEV-%08d",F.col("id")))
         .withColumn("device_type",F.element_at(F.array(*[F.lit(x) for x in ["PHONE","LAPTOP","TABLET","IOT","TV"]]),((F.col("id")-1)%5+1).cast("int")))
         .select("device_id","device_type"))
    aps.write.mode("overwrite").parquet("data/raw/access_points")
    devices.write.mode("overwrite").parquet("data/raw/devices")
    print("Generated 100,000 devices and 2,000 access points")
    spark.stop()
if __name__=="__main__": main()
