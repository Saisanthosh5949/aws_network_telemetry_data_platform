from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
with DAG("network_telemetry_pipeline",start_date=datetime(2026,1,1),schedule="@daily",catchup=False) as dag:
    generate=BashOperator(task_id="generate",bash_command="python -m src.generators.generate_telemetry --events 1000000")
    bronze=BashOperator(task_id="bronze",bash_command="python -m src.ingestion.raw_to_bronze")
    silver=BashOperator(task_id="silver",bash_command="python -m src.transformations.bronze_to_silver")
    gold=BashOperator(task_id="gold",bash_command="python -m src.transformations.build_gold_metrics")
    quality=BashOperator(task_id="quality",bash_command="python -m src.quality.validate_pipeline")
    generate >> bronze >> silver >> gold >> quality
