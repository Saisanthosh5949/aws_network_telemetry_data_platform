from src.common.spark_session import get_spark_session
def test_spark_starts():
    spark=get_spark_session("TestSpark")
    assert spark.range(10).count()==10
    spark.stop()
