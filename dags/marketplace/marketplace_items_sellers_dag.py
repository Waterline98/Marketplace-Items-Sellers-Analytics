import pendulum
import os 

from airflow import DAG, AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable

def get_env_var(key: str, default: str = None) -> str:
    value = os.getenv(key, default)
    if value is None:
        raise AirflowException(f"Required environment variable {key} is not set!")
    return value


K8S_SPARK_NAMESPACE = get_env_var("K8S_SPARK_NAMESPACE")
K8S_CONNECTION_ID = get_env_var("K8S_CONNECTION_ID")
GREENPLUM_ID = get_env_var("GREENPLUM_ID")
GP_SCHEMA_NAME = get_env_var("GP_SCHEMA_NAME")
GP_EXTERNAL_LOCATION = get_env_var("GP_EXTERNAL_LOCATION")


_emails = Variable.get("marketplace_dag_alert_emails", default_var="alert@example.com").strip()
DEFAULT_EMAILS = [e.strip() for e in _emails.split(",") if e.strip()]



def log_to_monitoring(context):
    task_instance = context['task_instance']
    print(f"Task {task_instance.task_id} completed with state {task_instance.state}")



def sla_miss_handler(dag, task_list, blocking_task_list, slas, blocking_tis):
    for sla in slas:
        print(f"SLA MISS: DAG {sla.dag_id}, Task {sla.task_id}, Deadline {sla.execution_date}")



def _build_submit_operator(task_id: str, application_file: str, link_dag, execution_timeout=None):
    kwargs = dict(
        task_id=task_id,
        namespace=K8S_SPARK_NAMESPACE,
        application_file=application_file,
        kubernetes_conn_id=K8S_CONNECTION_ID,
        do_xcom_push=True,
        dag=link_dag,
    )
    if execution_timeout is not None:
        kwargs["execution_timeout"] = execution_timeout
    return SparkKubernetesOperator(**kwargs)


def _build_sensor(task_id: str, application_name: str, link_dag, timeout=5400, poke_interval=60, attach_log=True):
    return SparkKubernetesSensor(
        task_id=task_id,
        namespace=K8S_SPARK_NAMESPACE,
        application_name=application_name,
        kubernetes_conn_id=K8S_CONNECTION_ID,
        attach_log=attach_log,
        timeout=timeout,
        poke_interval=poke_interval,
        dag=link_dag,
    )



default_args = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
    "max_active_runs":1,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": DEFAULT_EMAILS,
    "execution_timeout": pendulum.duration(hours=2),
    "on_success_callback": log_to_monitoring,
    "on_failure_callback": log_to_monitoring,
}



with DAG(
    dag_id="marketplace_items_sellers_analytics_dag",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=pendulum.datetime(2026, 2, 12, tz="UTC"),
    tags=["marketplace", "items_analytics", "sellers_analytics", "spark"],
    catchup=False,
    sla=pendulum.duration(hours=3),
    sla_miss_callback=sla_miss_handler,
    description="DAG for marketplace items and sellers analytics. Creates external tables and views in Greenplum based on Spark processing.",
) as dag:

    start = EmptyOperator(task_id="start", dag=dag)
    end = EmptyOperator(task_id="end", dag=dag)

    submit_task = _build_submit_operator(
        task_id='job_submit',
        application_file='spark_jobs/marketplace_items_metrics/application.yaml',
        execution_timeout=pendulum.duration(hours=1),
        link_dag=dag
    )

    sensor_task = _build_sensor(
        task_id='job_sensor',
        application_name=f"{{{{task_instance.xcom_pull(task_ids='job_submit')['metadata']['name']}}}}",
        attach_log=True,
        timeout=5400,
        poke_interval=60,
        link_dag=dag
    )

    
    drop_objects = SQLExecuteQueryOperator(
        task_id="drop_objects",
        conn_id=GREENPLUM_ID,
        sql=f"""
            DROP VIEW IF EXISTS "{GP_SCHEMA_NAME}".unreliable_sellers_view;
            DROP VIEW IF EXISTS "{GP_SCHEMA_NAME}".item_brands_view;
            DROP EXTERNAL TABLE IF EXISTS "{GP_SCHEMA_NAME}".seller_items;
        """,
        pool="greenplum_writes"
    )

    _create_table_sql = (
        f'CREATE EXTERNAL TABLE "{GP_SCHEMA_NAME}".seller_items('
        "sku_id BIGINT, title TEXT, category TEXT, brand TEXT, seller TEXT, "
        "group_type TEXT, country TEXT, availability_items_count BIGINT, "
        "ordered_items_count BIGINT, warehouses_count BIGINT, item_price BIGINT, "
        "goods_sold_count BIGINT, item_rate FLOAT8, days_on_sell BIGINT, "
        "avg_percent_to_sold BIGINT, returned_items_count INTEGER, potential_revenue BIGINT, "
        "total_revenue BIGINT, avg_daily_sales FLOAT8, days_to_sold FLOAT8, item_rate_percent FLOAT8"
        f") LOCATION ('{GP_EXTERNAL_LOCATION}') "
        "ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';"
    )
    items_datamart = SQLExecuteQueryOperator(
        task_id="items_datamart",
        conn_id=GREENPLUM_ID,
        sql=_create_table_sql,
        pool="greenplum_writes",
    )

    create_unreliable_sellers_report_view = SQLExecuteQueryOperator(
        task_id="create_unreliable_sellers_report_view",
        conn_id=GREENPLUM_ID,
        sql=f"""
            CREATE VIEW "{GP_SCHEMA_NAME}".unreliable_sellers_view AS
            SELECT 
                seller,
                SUM(availability_items_count) as total_overload_items_count,
                BOOL_OR(
                    days_on_sell > 100 
                    AND availability_items_count > ordered_items_count
                ) as is_unreliable
            FROM "{GP_SCHEMA_NAME}".seller_items
            GROUP BY seller;
        """,
    )

    create_brands_report_view = SQLExecuteQueryOperator(
        task_id="create_brands_report_view",
        conn_id=GREENPLUM_ID,
        sql=f"""
            CREATE VIEW "{GP_SCHEMA_NAME}".item_brands_view AS
            SELECT 
                brand,
                group_type,
                country,
                SUM(potential_revenue) as potential_revenue,
                SUM(total_revenue) as total_revenue,
                COUNT(sku_id) as items_count
            FROM "{GP_SCHEMA_NAME}".seller_items
            GROUP BY brand, group_type, country;
        """,
    )

    start >> submit_task >> sensor_task >> drop_objects >> items_datamart
    items_datamart >> create_unreliable_sellers_report_view >> end 
    items_datamart >> create_brands_report_view >> end
