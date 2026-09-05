from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


def notify_failure(context):
    """Basic failure alerting — logs clearly; swap in Slack/email webhook as needed."""
    task_instance = context.get('task_instance')
    print(f"ALERT: Task {task_instance.task_id} failed in DAG {task_instance.dag_id} "
          f"at {context.get('execution_date')}")
    # Example Slack webhook call (uncomment and configure):
    # import requests
    # requests.post(
    #     "https://hooks.slack.com/services/YOUR/WEBHOOK/URL",
    #     json={"text": f"🚨 Airflow task failed: {task_instance.task_id}"}
    # )


default_args = {
    "owner": "flight-delay-pipeline",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
    "email_on_failure": False,
}

# Resolve the Job ID directly as an int at parse time — avoids Jinja templating
# quirks with job_id on DatabricksRunNowOperator.
INGEST_FLIGHTS_JOB_ID = int(Variable.get("ingest_flights_job_id"))

with DAG(
    dag_id="flight_delay_pipeline",
    default_args=default_args,
    description="Ingest BTS data via existing Databricks notebooks, transform via dbt, validate with tests",
    schedule_interval="@monthly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["flight-delay", "production"],
) as dag:

    # Triggers your existing flight-data ingestion notebook (download, unzip, load to Bronze)
    ingest_flights_task = DatabricksRunNowOperator(
        task_id="ingest_flights",
        databricks_conn_id="databricks_default",
        job_id=INGEST_FLIGHTS_JOB_ID,
    )

    dbt_snapshot_task = BashOperator(
        task_id="dbt_snapshot",
        bash_command="cd /opt/airflow/dbt-project && dbt snapshot",
    )

    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt-project && dbt run",
    )

    dbt_test_task = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt-project && dbt test",
        # dbt test exits non-zero on any error-severity failure, which
        # automatically fails this task — warn-severity tests won't block it.
    )

    #Running the pipeline
    [ingest_flights_task] >> dbt_snapshot_task >> dbt_run_task >> dbt_test_task