from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'email': ['airflow@example.com'],
    'depends_on_past': False,
}

with DAG(
    dag_id='build_model_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(2)
) as dag:

    preprocess_data = DatabricksSubmitRunOperator(task_id='preprocess_data',
                                                  spark_python_taskdict= ,
                                                  existing_cluster_ids= ,
                                                  libraries=[{'jar': 'dbfs:/lib/etl-0.1.jar'}]
                                                  )
    build_model = DatabricksSubmitRunOperator( task_id='build_model',
                                               spark_python_taskdict= ,
                                               existing_cluster_ids=  ,
                                               libraries=[{'jar': 'dbfs:/lib/etl-0.1.jar'}])


    # build_model = DatabricksSubmitRunOperator(
    #    task_id='build_model',
    #    new_cluster=small_cluster,
    #    spark_jar_task={'main_class_name': 'com.example.ProcessData'},
    #    libraries=[{'jar': 'dbfs:/lib/etl-0.1.jar'}],
    #)
    preprocess_data >> build_model
