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
        data_ = args[0]
        n_before_ = kwargs['n_before_']
        stage_name_ = kwargs['stage_name_']

        print(f'Начат этап очистки "{stage_name_}"...')
        print(f'  Размер датасета до очистки: {n_before_}.')
        start_time_ = timer()
        data_ = filter_func(data_)
        print(f'  Подсчёт размера датасета после очистки...')
        n_after = data_.count()
        print(f'  Размер датасета после очистки: {n_after}.')
        n_diff = n_before_ - n_after
        print(f'  Количество удалённых строк: {n_diff}.')
        end_time_ = timer()
        duration_ = end_time_ - start_time_
        print(f'  Продолжительность этапа: {duration_:.2f} сек.')
        print(f'Этап очистки "{stage_name_}" успешно завершён!')

        return data_, n_after

    return wrapper


# Удаление полных дубликатов.
@count_decorator
def _drop_duplicates(data_):
    return data_.dropDuplicates()


# Удаление отрицательных индентификаторов клиентов.
@count_decorator
def _drop_negative_customers(data_):
    return data_.filter(data_['customer_id'] >= 0)


# Удаление строк с нулевым объёмом перевода.
@count_decorator
def _drop_nonpositive_amount(data_):
    return data_.filter(data_['tx_amount'] > 0)


def prepare_initial_data_filepath(filepath_: str, spark_cluster_url_: str = None) -> str:
    if spark_cluster_url_ is None:
        spark_cluster_url_ = SPARK_CLUSTER_URL

    print(f'Путь до файла с исходными данными: {filepath_}.')
    if not filepath_.startswith(HDFS_FRAUD_DATA_DIR):
        filepath_ = HDFS_FRAUD_DATA_DIR + filepath_.lstrip('/')
    if not filepath_.startswith(spark_cluster_url_):
        filepath_ = spark_cluster_url_ + filepath_.lstrip('/')
    print(f'Полный путь до файла исходными с данными: {filepath_}.')
    return filepath_


def check_cleaned_file_existence(filepath_: str) -> bool:
    path_to_check = 's3://' + filepath_ + '/'
    check_command = f's3cmd ls {path_to_check} ' + r"| grep _SUCCESS$ | awk '{print $4}'"
    res = subprocess.run(check_command, shell=True, capture_output=True)
    is_file_exists = False
    if res.stdout:
        print(f'Файл {path_to_check} уже успешно очищен и сохранён!')
        is_file_exists = True
    return is_file_exists


def create_spark_session():
    print('Создание Spark-сессии...')
    spark_ = SparkSession.builder.appName('data_cleaning') \
        .config(conf=SparkConf()) \
        .config('spark.sql.execution.arrow.pyspark.enabled', True) \
        .config('spark.driver.memory', '8G') \
        .getOrCreate()
    print('Spark-сессия успешно создана!')
    return spark_


def load_transaction_data(filepath_: str, data_schema_):
    print(f'Загрузка данных из файла "{filepath_}"...')
    data_ = spark.read.csv(filepath_, schema=data_schema_, header=False, sep=',', comment='#')
    print('Данные успешно загружены!')
    return data_


def clean_data(data_):
    n_before_ = data_.count()
    data_, n_before_ = _drop_duplicates(data_, n_before_=n_before_,
                                        stage_name_="Удаление полных дубликатов")
    data_, n_before_ = _drop_negative_customers(data_, n_before_=n_before_,
                                                stage_name_="Удаление отрицательных индентификаторов клиентов")
    data_, n_before_ = _drop_nonpositive_amount(data_, n_before_=n_before_,
                                                stage_name_="Удаление строк с нулевым объёмом перевода")
    return data_


def get_s3_client(url_, key_id_, access_key_, region_):
    session = boto3.Session(
        aws_access_key_id=key_id_,
        aws_secret_access_key=access_key_,
        region_name=region_,
    )
    return session.client('s3', endpoint_url=url_)


def prepare_path_to_save_data(filepath_: str, s3_path_: str) -> str:
    print(f'Подготовка пути для сохранения файла')
    filepath_ = filepath_.rsplit('/', 1)[1].replace('.txt', '.parquet')
    filepath_ = 's3a://' + s3_path_ + filepath_
    print(f'Путь для сохранения файла: {filepath_}')
    return filepath_


def save_data(data_, filepath_) -> None:
    print(f'Сохранение данных в файл "{filepath_}"...')
    if not filepath_.endswith('.parquet'):
        filepath_ = filepath_ + f'.parquet'
    if not filepath_.startswith('s3a://'):
        filepath_ = 's3a://' + filepath_
    print(f'Полный путь: "{filepath_}"...')
    data_.coalesce(1).write.parquet(filepath_, mode='overwrite')
    print('Файл успешно сохранен!')


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        prog='DataCleaning',
        description='Cleans the transaction data using PySpark',
        epilog='Argument is a path to file with transaction data'
    )
    parser.add_argument('data_filepath')
    parser.add_argument('spark_cluster_url')
    arguments = parser.parse_args()
    data_filepath = arguments.data_filepath
    spark_cluster_url = arguments.spark_cluster_url

    # Подготовка путей к файлам.
    data_filepath = prepare_initial_data_filepath(data_filepath, spark_cluster_url)
    path_to_save = prepare_path_to_save_data(data_filepath, S3_FRAUD_DATA_PATH)
    is_cleaned_file_exists = check_cleaned_file_existence(path_to_save)
    if is_cleaned_file_exists:
        exit(0)

    # Загрузка данных.
    spark = create_spark_session()
    data = load_transaction_data(data_filepath, DATA_SCHEMA)

    # Чистка данных.
    data = clean_data(data)

    # Сохранение данных.
    s3_client = get_s3_client(S3_URL, BUCKET_RAW_DATA_ACCESS_ID, BUCKET_RAW_DATA_ACCESS_KEY, S3_REGION)
    save_data(data, path_to_save)
