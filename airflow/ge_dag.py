from datetime import datetime
import airflow
from airflow import AirflowException
from airflow.operators.bash_operator import BashOperator
from airflow.operatros.python_operator import PythonOperator
from airflow import DAG
import os
from dotenv import load_dotenv

load_dotenv(".env.airflow")

import pandas as pd
from sqlalchemy import create_engine
import great_expectations as ge


def get_root_dir():
    if "airflow" in os.path.listdir():
        return os.getcwd()
    else:
        os.chdir("..")
        return os.getcwd()

def get_gedc_path():
    if os.path.exists(
        get_root_dir(),
        "great_expectations"
    ):
        return os.path.join(get_root_dir(), "great_expectations")
    else:
        print("Your current working directory is: ", os.getcwd())
        return


# global varaibles
GE_DB_URL = os.getenv("GE_DB_URL")
ROOTH_PATH = os.get("PROJECT_ROOT_DIR") or os.get_root_dir()
DATA_DIR = os.getenv("DATA_DIR")

ge_data_context_path = os.getenv("GE_DATACONTEXT_PATH") or get_gedc_path()

default_args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(1)
}

dag = DAG(
    dag_id="ge_dag",
    default_args=default_args,
    schedule_interval=None,
)

def load_files_into_db(ds, **kwargs):
    """
    Method to load parqueet data into database using SQLAlchemy
    """

    engine = create_engine(GE_DB_URL)

    with engine.connect() as conn:
        conn.execute("DROP TABLE IF EXISTS taxi_rides CASCADE")

        yellow_trip_df = pd.read_parquet(os.path.join(DATA_DIR, "yellow_tripdata_2022-01.parquet"))
        yellow_trip_df.to_sql(
            "taxi_rides",
            engine,
            schema=None,
            if_exists="replace",
            index=False,
            index_label=None,
            chunksize=None,
            dtype=None
        )

    return "Loaded files into database"


def validate_source_data(ds, **kwargs):
    
    context = ge.data_context.DataContext(ge_data_context_path)

    batch_kwargs_file = {
        "path": os.path.join(DATA_DIR, "yellow_tripdata_2022-01.parquet"),
        "datasource": "input_files"
    }

    # outputs validation suite object
    batch_file = context.get_batch(
        batch_kwargs_file, 
        "yellow_tripdata.critical"
    )

    results = context.run_validation_operator(
        "action_list_operator",
        assets_to_validate=[batch_file],
        run_id="airflow:{}:{}".format(kwargs["dag_run"].run_id, str(kwargs["dag_run"].start_date))
    )

    if not results["success"]:
        raise AirflowException("Validation of the source data is not successful.")


def validate_source_data_load(ds, **kwargs):

    context = ge.data_context.DataContext(ge_data_context_path)

    datasource_name_file = "input_files"
    expectation_suite_name = "yellow_tripdata.critical"
    batch_kwargs_file = {
        "path": os.path.join(DATA_DIR, "yellow_tripdata_2022-01.parquet"),
        "datasource": "input_files"
    }

    batch_file = context.get_batch(batch_kwargs_file, expectation_suite_name)

    expectation_suite_name_db = "yellow_tripdata_table.critical"
    datasource_name_file_db = "expectations_store"

    # if you would like to validate an entire table or view in your database's default schema:
    batch_kwargs_db = {
        "table": "taxi_rides",
        "datasource": datasource_name_file_db
    }

    # if you would like to validate an entire table or view from a non-default schema in your database:
    # batch_kwargs = {
    #     "table": "taxi_rides",
    #     "schema": "YOUR_SCHEMA",
    #     "datasource": datasource_name
    # }

   # if you would liket o validate the result set of a query:
    # batch_kwargs = {
    # "query": "SELECT your rows FROM YOUR_TABlE",
    # "datasource": datasource
    # }

    batch_db = context.get_batch(batch_kwargs_db, expectation_suite_name)

    # call a validation operator to validate the batch
    # the operator will evaluate the data against the expectations
    # and perform a list of actions, such as saving the validation
    # result, updating the Data Docs, and firing a notifiation (e.g., Slack)
    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%S.%fZ")
    run_id = "airflow:{}:{}".format(kwargs["dag_run"].run_id, str(kwargs["dag_run"].start_date))
    results = context.run_validation_operator(
        "action_list_operator",
        assets_to_validate=[batch_file, batch_db],
        run_id=run_id # e.g., Airflow run id or some run identifier that your pipeline uses
    )

    if not results["success"]:
        raise AirflowException("Validation of the source data laoding is not successful.")


def validate_analytical_output(ds, **kwargs):

    context = ge.data_context.DataContext(ge_data_context_path)

    datasource_name = "expectations_store" # a data source configured in great_expectations.yml

    # tell GE how ot fetch the batch of data that should be validated...

    # ... from the result set of a SQL query:
    # batch_kwargs = {
    #     "query": "SELECT QUERY",
    #     "datasource": "datasource_name"
    # }

    # ... or from a database table:
    batch_kwargs = {
        "table": "count_providers_state",
        "datasource": datasource_name
    }

    # ... or from a file:
    # batch_kwargs = {
    #     "path": "path to your data file",
    #     "datasource": datasource_name
    # }

    # ... or from a Pandas or PySpark Dataframe
    # batch_kwargs = {
    #     "dataset": "your pandas or pyspark df",
    #     "datasource": datasource_name
    # }

    # get the batch of data that you want to validate
    # specify the name of the expectation suite that holds the expectations
    expectation_suite_name = "dn_ge_demo.expecations" # this is an exmaple of a suite that you created
    # suite that you created
    batch = context.get_batch(batch_kwargs, expectation_suite_name)

    # call a validation operator to validate the batch
    # the operator will evaluate the data against the expectations
    # and perform a list of actions, such as saving the validation
    # result, updating the Data Docs, and firing a notification (e.g., Slack).
    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%S.%fZ")
    run_id = "airflow:{}:{}".format(kwargs["dag_run"].run_id, str(kwargs["dag_run"].start_date))
    results = context.run_validation_operator(
        "action_list_operator",
        assets_to_validate=[batch],
        run_id=run_id # e.g., Airflow run id or some run identifier that your pipeline uses.
    )

    if not results["success"]:
        raise AirflowException(
                "The analytical output does not meet the expectations in the suite: {0:s}".\
                    format(expectation_suite_name)
            )


def publish_to_prod():
    """
    Method to simply promote a table in a database by renaming it using SQLAlchemy
    """

    engine = create_engine(GE_DB_URL)

    with engine.connect() as conn:
        conn.execute("DROP TABLE IF EXISTS prod_count_providers_by_state")
        conn.execute("ALTER TABLE count_providers_by_state RENAME TO prod_count_providers_by_state")
        conn.commit()


task_validate_source_data = PythonOperator(
    task_id="task_validate_source_data",
    python_callable=validate_source_data,
    provide_context=True,
    dag=dag
)

task_load_files_into_db = PythonOperator(
    task_id="task_load_files_into_db",
    python_callable=load_files_into_db,
    provide_context=True,
    dag=dag
)

task_validate_source_data_load = PythonOperator(
    task_id="task_validate_source_data_load", 
    python_callable=validate_source_data_load,
    provide_context=True,
    dag=dag
)

task_transform_data_in_db = BashOperator(
    task_id="task_transform_data_in_db", 
    bash_command="dbt run --project-dir {}".format(os.path.join(ROOTH_PATH, "dbt")),
    dag=dag
)

task_validate_analytical_output = PythonOperator(
    task_id="task_validate_analytical_output", 
    python_callable=validate_analytical_output, 
    provide_context=True, 
    dag=dag
)

task_publish = PythonOperator(
    task_id="task_publish", 
    python_callable=publish_to_prod, 
    dag=dag
)

# DAG Dependencies
task_validate_source_data >> task_load_files_into_db >> task_validate_source_data_load >> task_transform_data_in_db >> task_validate_analytical_output >> task_publish