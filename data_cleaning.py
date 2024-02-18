import argparse
import functools
import os
import subprocess
from timeit import default_timer as timer

import findspark
findspark.init()

import boto3
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, FloatType, ByteType

# Артефакты HDFS с исходными данными.
SPARK_CLUSTER_URL = 'hdfs://rc1a-dataproc-m-rig0u7i3lwc2dxan.mdb.yandexcloud.net/'
HDFS_FRAUD_DATA_DIR = '/user/ubuntu/fraud_data/'

# Артефакты S3-хранилища для сохранения результатов очистки.
BUCKET_NAME = 'otus-mlops-dproc/'
BUCKET_RAW_DATA_ACCESS_ID = os.environ['BUCKET_RAW_DATA_ACCESS_ID']
BUCKET_RAW_DATA_ACCESS_KEY = os.environ['BUCKET_RAW_DATA_ACCESS_KEY']
S3_REGION = 'ru-central1'
S3_URL = 'https://storage.yandexcloud.net'
S3_FRAUD_DATA_DIR = 'fraud_data/'
S3_FRAUD_DATA_PATH = BUCKET_NAME + S3_FRAUD_DATA_DIR

# Список столбцов и их типов для pyspark.
DATA_COLS = {
    'tx_id': IntegerType,
    'tx_datetime': TimestampType,
    'customer_id': IntegerType,
    'terminal_id': IntegerType,
    'tx_amount': FloatType,
    'tx_time_in_seconds': IntegerType,
    'tx_time_in_days': IntegerType,
    'tx_is_fraud': ByteType,
    'tx_fraud_scenario': ByteType,
}
# Схема данных по транзакциям.
DATA_SCHEMA = StructType(
    [
        StructField(col_name, col_type(), True)
        for col_name, col_type in DATA_COLS.items()
    ]
)


# Декоратор с stdout и формирование информации об этапе очистки.
def count_decorator(filter_func):
    @functools.wraps(filter_func)
    def wrapper(*args, **kwargs):
        data = args[0]
        n_before = kwargs['n_before']
        stage_name = kwargs['stage_name_']

        print(f'Начат этап очистки "{stage_name}"...')
        print(f'  Размер датасета до очистки: {n_before}.')
        start_time = timer()
        data = filter_func(data)
        print(f'  Подсчёт размера датасета после очистки...')
        n_after = data.count()
        print(f'  Размер датасета после очистки: {n_after}.')
        n_diff = n_before - n_after
        print(f'  Количество удалённых строк: {n_diff}.')
        end_time = timer()
        duration = end_time - start_time
        print(f'  Продолжительность этапа: {duration:.2f} сек.')
        print(f'Этап очистки "{stage_name}" успешно завершён!')

        return data, n_after

    return wrapper


# Удаление полных дубликатов.
@count_decorator
def _drop_duplicates(data):
    return data.dropDuplicates()


# Удаление отрицательных индентификаторов клиентов.
@count_decorator
def _drop_negative_customers(data):
    return data.filter(data['customer_id'] >= 0)


# Удаление строк с нулевым объёмом перевода.
@count_decorator
def _drop_nonpositive_amount(data):
    return data.filter(data['tx_amount'] > 0)


def prepare_initial_data_filepath(filepath: str, spark_cluster_url: str = None) -> str:
    if spark_cluster_url is None:
        spark_cluster_url = SPARK_CLUSTER_URL

    print(f'Путь до файла с исходными данными: {filepath}.')
    if not filepath.startswith(HDFS_FRAUD_DATA_DIR):
        filepath = HDFS_FRAUD_DATA_DIR + filepath.lstrip('/')
    if not filepath.startswith(spark_cluster_url):
        filepath = spark_cluster_url + filepath.lstrip('/')
    print(f'Полный путь до файла исходными с данными: {filepath}.')
    return filepath


def check_cleaned_file_existence(filepath: str) -> bool:
    path_to_check = 's3://' + filepath + '/'
    check_command = f's3cmd ls {path_to_check} ' + r"| grep _SUCCESS$ | awk '{print $4}'"
    res = subprocess.run(check_command, shell=True, capture_output=True)
    is_file_exists = False
    if res.stdout:
        print(f'Файл {path_to_check} уже успешно очищен и сохранён!')
        is_file_exists = True
    return is_file_exists


def create_spark_session():
    print('Создание Spark-сессии...')
    spark = SparkSession.builder.appName('data_cleaning') \
        .config(conf=SparkConf()) \
        .config('spark.sql.execution.arrow.pyspark.enabled', True) \
        .config('spark.driver.memory', '8G') \
        .getOrCreate()
    print('Spark-сессия успешно создана!')
    return spark


def load_transaction_data(spark, filepath: str, data_schema):
    print(f'Загрузка данных из файла "{filepath}"...')
    data = spark.read.csv(filepath, schema=data_schema, header=False, sep=',', comment='#')
    print('Данные успешно загружены!')
    return data


def clean_data(data):
    n_before = data.count()
    data, n_before = _drop_duplicates(data, n_before=n_before,
                                      stage_name_="Удаление полных дубликатов")
    data, n_before = _drop_negative_customers(data, n_before=n_before,
                                              stage_name_="Удаление отрицательных индентификаторов клиентов")
    data, n_before = _drop_nonpositive_amount(data, n_before=n_before,
                                              stage_name_="Удаление строк с нулевым объёмом перевода")
    return data


def get_s3_client(url_, key_id_, access_key_, region_):
    session = boto3.Session(
        aws_access_key_id=key_id_,
        aws_secret_access_key=access_key_,
        region_name=region_,
    )
    return session.client('s3', endpoint_url=url_)


def prepare_path_to_save_data(filepath: str, s3_path: str) -> str:
    print(f'Подготовка пути для сохранения файла')
    filepath = filepath.rsplit('/', 1)[1].replace('.txt', '.parquet')
    filepath = 's3a://' + s3_path + filepath
    print(f'Путь для сохранения файла: {filepath}')
    return filepath


def save_data(data, filepath) -> None:
    print(f'Сохранение данных в файл "{filepath}"...')
    if not filepath.endswith('.parquet'):
        filepath = filepath + f'.parquet'
    if not filepath.startswith('s3a://'):
        filepath = 's3a://' + filepath
    print(f'Полный путь: "{filepath}"...')
    data.coalesce(1).write.parquet(filepath, mode='overwrite')
    print('Файл успешно сохранен!')


def main(data_filepath, spark_cluster_url):
    # Подготовка путей к файлам.
    data_filepath = prepare_initial_data_filepath(data_filepath, spark_cluster_url)
    path_to_save = prepare_path_to_save_data(data_filepath, S3_FRAUD_DATA_PATH)
    is_cleaned_file_exists = check_cleaned_file_existence(path_to_save)
    if is_cleaned_file_exists:
        exit(0)

    # Загрузка данных.
    spark = create_spark_session()
    data = load_transaction_data(spark, data_filepath, DATA_SCHEMA)

    # Чистка данных.
    data = clean_data(data)

    # Сохранение данных.
    save_data(data, path_to_save)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        prog='DataCleaning',
        description='Cleans the transaction data using PySpark',
        epilog='Argument is a path to file with transaction data'
    )
    parser.add_argument('data_filepath')
    parser.add_argument('spark_cluster_url')
    arguments = parser.parse_args()
    data_filepath_ = arguments.data_filepath
    spark_cluster_url_ = arguments.spark_cluster_url

    main(data_filepath_, spark_cluster_url_)
