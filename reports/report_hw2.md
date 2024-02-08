# Отчёт по ДЗ №2

Данные по транзакциям перенесены в объектное хранилище: **s3://otus-mlops-bucket/fraud_data/**  
![Данные в s3-хранилище](hw2_img/s3_data.png "Данные в s3-хранилище")  
Перенос выполнен командой `s3cmd sync s3://mlops-data/fraud-data/ s3://otus-mlops-bucket/fraud_data/ --acl-public`.
Без последнего ключа возникала ошибка Access Denied.

Также был создан Spark-кластер в DataProc с двумя подкластерами.  
![Spark-кластер](hw2_img/data_proc_cluster.png "Spark-кластер")  
![Подкластеры](hw2_img/data_proc_subclusters.png "Подкластеры")  
Данные из объектного хранилища были скопированы в HDFS-хранилище.  
![Процесс копирования данных на HDFS-кластер](hw2_img/hdfs_distcp_1.PNG "Процесс копирования данных на HDFS-кластер")  
![Процесс копирования данных на HDFS-кластер](hw2_img/hdfs_distcp_2.PNG "Процесс копирования данных на HDFS-кластер")  
![Данные на HDFS-кластере](hw2_img/hdfs_data.png "Данные на HDFS-кластере")  
