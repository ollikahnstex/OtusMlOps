from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, FloatType, ByteType

# Значения по умолчанию для автоматического создания и удаления Spark-кластера.
DEFAULT_CLUSTER_NAME = 'dataproc123'
DEFAULT_BUCKET_NAME = 'otus-mlops-af'
DEFAULT_ZONE = 'ru-central1-a'
DEFAULT_SERVICE_ACCOUNT_NAME = 'ollikahnstex-dataproc'
DEFAULT_IMAGE_VERSION = '2.0'
DEFAULT_SERVICES = 'hdfs,yarn,mapreduce,tez,spark'
DEFAULT_SSH_PUB_KEY_PATH = '/home/ollikahnstex/.ssh/id_rsa.pub'
DEFAULT_DELETION_PROTECTION = 'false'
DEFAULT_SECURITY_GROUP_IDS = 'enp0m0qdpu6qo783air4'
DEFAULT_SUBCLUSTERTS = {}  # TODO: Заполнить, сейчас захардкожено в функции.
DEFAULT_CLUSTER_SETTINGS = {
    'cluster_name': DEFAULT_CLUSTER_NAME,
    'bucket_name': DEFAULT_BUCKET_NAME,
    'zone': DEFAULT_ZONE,
    'service_account_name': DEFAULT_SERVICE_ACCOUNT_NAME,
    'image_version': DEFAULT_IMAGE_VERSION,
    'services': DEFAULT_SERVICES,
    'ssh_pub_key_path': DEFAULT_SSH_PUB_KEY_PATH,
    'deletion_protection': DEFAULT_DELETION_PROTECTION,
    'security_group_ids': DEFAULT_SECURITY_GROUP_IDS,
    'subclusters': DEFAULT_SUBCLUSTERTS,
}

# Артефакты HDFS с исходными данными.
SPARK_CLUSTER_URL = 'hdfs://rc1a-dataproc-m-rig0u7i3lwc2dxan.mdb.yandexcloud.net/'
HDFS_FRAUD_DATA_DIR = '/user/ubuntu/fraud_data/'

# # Артефакты S3-хранилища с исходными данными.
# BUCKET_NAME = 'otus-mlops-bucket/'
# BUCKET_RAW_DATA_ACCESS_ID = os.environ['BUCKET_RAW_DATA_ACCESS_ID']
# BUCKET_RAW_DATA_ACCESS_KEY = os.environ['BUCKET_RAW_DATA_ACCESS_KEY']
# S3_REGION = 'ru-central1'
# S3_URL = 'https://storage.yandexcloud.net'
# S3_FRAUD_DATA_DIR = 'fraud_data/'
# S3_FRAUD_DATA_PATH = BUCKET_NAME + S3_FRAUD_DATA_DIR
#
# # Артефакты S3-хранилища для сохранения результатов очистки.
# BUCKET_NAME = 'otus-mlops-dproc/'
# BUCKET_RAW_DATA_ACCESS_ID = os.environ['BUCKET_RAW_DATA_ACCESS_ID']
# BUCKET_RAW_DATA_ACCESS_KEY = os.environ['BUCKET_RAW_DATA_ACCESS_KEY']
# S3_REGION = 'ru-central1'
# S3_URL = 'https://storage.yandexcloud.net'
# S3_FRAUD_DATA_DIR = 'fraud_data/'
# S3_FRAUD_DATA_PATH = BUCKET_NAME + S3_FRAUD_DATA_DIR

# Список столбцов и их типов для pyspark.
DATA_COLS_TYPES = {
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
        for col_name, col_type in DATA_COLS_TYPES.items()
    ]
)
