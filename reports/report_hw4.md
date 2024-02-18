# Отчёт по ДЗ №4

## Автоматизация чистки данных

### Общий порядок
Смотрим данные в s3 с имходными данными.  
Смотрим данные в s3 с обработанными данными.  
Разница этого – необработанные файлы.  
Автоматически создаём Spark-кластер.  
Необработанные файлы кидаем на кластер.  
Чистим файлы на кластере.  
Записываем обработанные файлы в s3.  
Автоматически удаляем Spark-кластер. 

### Автоматическое создание Spark-кластера
Spark-кластер автоматически создаётся при каждом запуске DAG. Пример соответствующей команды:  
```bash
yc dataproc cluster create dataproc123 \
        --bucket=otus-mlops-af \
        --zone=ru-central1-a \
        --service-account-name=ollikahnstex-dataproc \
        --version=2.0 \
        --services=hdfs,yarn,mapreduce,tez,spark \
        --ssh-public-keys-file="/home/ollikahnstex/.ssh/id_rsa.pub" \
        --subcluster name=dataproc123_subcluster_m1,`
                     `role=masternode,`
                     `resource-preset=s3-c2-m8,`
                     `disk-type=network-ssd,`
                     `disk-size=20,`
                     `subnet-name=otus-mlops-ru-central1-a,`
                     `assign-public-ip=true \
        --subcluster name=dataproc123_subcluster_d1,`
                     `role=datanode,`
                     `resource-preset=s3-c4-m16,`
                     `disk-type=network-hdd,`
                     `disk-size=20,`
                     `subnet-name=otus-mlops-ru-central1-a,`
                     `hosts-count=1,`
                     `assign-public-ip=false \
        --deletion-protection=false \
        --security-group-ids=enp0m0qdpu6qo783air4
```  
Далее по имени созданного кластера определяется его ID. Пример:  
```bash
yc dataproc cluster list | grep 'dataproc123' | awk '{print $2}'
```  
И по этому ID определяется FQDN мастер-ноды кластера. Пример:  
```bash
yc dataproc cluster list-hosts c9q45go0c2er6fbj02of | grep 'MASTERNODE' | awk '{print $2}'
```  
Пример результата: `rc1a-dataproc-m-00mjmvggm8iyttw7.mdb.yandexcloud.net`.  
Полученный FQDN далее используется для создания Spark-сессии.  
