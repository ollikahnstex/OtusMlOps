from datetime import datetime, timedelta
from typing import Dict

from airflow.decorators import dag, task

from _constants import DEFAULT_CLUSTER_SETTINGS
from data_cleaning import main as clean_data
from classes.cluster_handler import SparkClusterHandler


@dag(
    dag_id='DataCleaning',
    schedule_interval='@daily',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=120),
    tags=['otus', 'mlops', 'data_cleaning'],
)
def clean_data():

    @task(task_id='spark_cluster_creation')
    def create_cluster_task(
            cluster_name: str,
            bucket_name: str,
            zone: str,
            service_account_name: str,
            image_version: str,
            services: str,
            ssh_pub_key_path: str,
            deletion_protection: str,
            security_group_ids: str,
            subclusters: Dict
    ):
        """Creates Spark-cluster on Yandex Cloud."""
        cluster = SparkClusterHandler(
            cluster_name,
            bucket_name,
            zone,
            service_account_name,
            image_version,
            services,
            ssh_pub_key_path,
            deletion_protection,
            security_group_ids,
            subclusters
        )
        return cluster

    @task(task_id='data_copy_from_s3_to_hdfs')
    def copy_unprocessed_data_from_s3_to_hdfs_task():
        """Copies the data from s3 to HDFS."""
        filenames = search_unprocessed_data_on_s3()
        copy_unprocessed_data_from_s3_to_hdfs(filenames)
        print(f'{len(filenames)} successfully copied from s3 to HDFS: ')
        print(*filenames, sep='\n')
        return filenames

    @task(task_id=f'data_cleaning')
    def clean_data_task(data_filepath, spark_cluster_url):
        """Cleans the data on Spark-cluster."""
        clean_data(data_filepath, spark_cluster_url)
        print(f'Data successfully cleaned!')

    @task(task_id='spark_cluster_deletion')
    def delete_cluster_task(cluster: SparkClusterHandler):
        """Deletes Spark-cluster on Yandex Cloud."""
        if cluster.exists:
            cluster.delete_cluster()

    cluster_ = create_cluster_task(**DEFAULT_CLUSTER_SETTINGS)
    filenames_ = copy_unprocessed_data_from_s3_to_hdfs_task()
    for i, filename_ in enumerate(filenames_):
        clean_data_task.override(task_id=f'data_cleaning_{i}')()
    delete_cluster_task(cluster_)


clean_data()