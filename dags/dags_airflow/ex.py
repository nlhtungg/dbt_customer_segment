from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.models.variable import Variable
from airflow.operators.dummy import DummyOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.profiles import SparkThriftProfileMapping

# Config Cosmos
profile_config = ProfileConfig(
    profile_name="dbt_main_project",
    target_name="dev",
    profile_mapping=SparkThriftProfileMapping(
        conn_id="dbt_spark",
        profile_args={
            "schema": "default",
            "s3_access_key_id": "minioadmin",
            "s3_secret_access_key": "minioadmin",
            "s3_endpoint": "http://minio:9000",
            "s3_path_style_access": True,
            "hive_metastore_uris": "thrift://hive-metastore:9083",
            "catalog": "hive",
        },
    ),
)

project_config = ProjectConfig(
    dbt_project_path="/opt/airflow/dags/dbt_main_project",
)

with DAG(
    dag_id="example_iceberg",
    start_date=pendulum.datetime(2025, 3, 8, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["dbt", "cosmos", "iceberg", "minio"],
) as dag:
    start = DummyOperator(task_id="start")

    def read_variable(ti):
        """
        Đọc giá trị processing_date từ Airflow Variables và đẩy
        nó ra XCom để task dbt có thể sử dụng
        """
        date_var = Variable.get("processing_date", default_var="1970-01-01")
        print(f"Processing for date: {date_var}")
        return date_var

    read_variable_task = PythonOperator(
        task_id="read_processing_date_var",
        python_callable=read_variable,
    )

    dbt_tasks = DbtTaskGroup(
        group_id="dbt_processing",
        project_config=project_config,
        profile_config=profile_config,
        operator_args={
            "vars": {
                "processing_date": "{{ ti.xcom_pull(task_ids='read_processing_date_var') }}"
            },
            "append_env": True,
            "install_deps": True,
        },
    )

    success = DummyOperator(task_id="success")
    fail = DummyOperator(task_id="fail", trigger_rule="one_failed")

    start >> read_variable_task >> dbt_tasks
    dbt_tasks >> success
    dbt_tasks >> fail