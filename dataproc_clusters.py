import subprocess
from timeit import default_timer as timer
from typing import Dict

from cluster_handler import SparkClusterHandler


def create_cluster(cluster_name: str,
                   bucket_name: str,
                   zone: str,
                   service_account_name: str,
                   image_version: str,
                   services: str,
                   ssh_pub_key_path: str,
                   deletion_protection: str,
                   security_group_ids: str,
                   subclusters: Dict):
    cluster = SparkClusterHandler()

    yc_create_cmd = 'yc dataproc cluster create'
    subclusters_info = _prepare_subclusters_info(subclusters)

    command = f'''{yc_create_cmd} {cluster_name} \\
        --bucket={bucket_name} \\
        --zone={zone} \\
        --service-account-name={service_account_name} \\
        --version={image_version} \\
        --services={services} \\
        --ssh-public-keys-file="{ssh_pub_key_path}" \\
        {subclusters_info} \\
        --deletion-protection={deletion_protection} \\
        --security-group-ids={security_group_ids}'''

    print(f'Creating new Spark-cluster "{cluster_name}" using command:\n{command}')
    start_time = timer()
    res = subprocess.run(command, shell=True, capture_output=True)
    print(res.stdout)
    print(f'Creating time: {timer() - start_time}')
    return cluster


def delete_cluster(cluster_name: str):
    yc_delete_cmd = 'yc dataproc cluster delete'

    command = f'{yc_delete_cmd} --name={cluster_name}'

    print(f'Deleting Spark-cluster "{cluster_name}" using command:\n{command}')
    start_time = timer()
    res = subprocess.run(command, shell=True, capture_output=True)
    print(res.stdout)
    print(f'Deleting time: {timer() - start_time}')


def _prepare_subclusters_info(subclusters: Dict) -> str:
    subclusters_info = f'''--subcluster name=dataproc123_subcluster_m1,`
                     `role=masternode,`
                     `resource-preset=s3-c2-m8,`
                     `disk-type=network-ssd,`
                     `disk-size=20,`
                     `subnet-name=otus-mlops-ru-central1-a,`
                     `assign-public-ip=true \\
        --subcluster name=dataproc123_subcluster_d1,`
                     `role=datanode,`
                     `resource-preset=s3-c4-m16,`
                     `disk-type=network-hdd,`
                     `disk-size=20,`
                     `subnet-name=otus-mlops-ru-central1-a,`
                     `hosts-count=1,`
                     `assign-public-ip=false'''
    return subclusters_info


if __name__ == '__main__':
    from _constants import *
    create_cluster(
        DEFAULT_CLUSTER_NAME,
        DEFAULT_BUCKET_NAME,
        DEFAULT_ZONE,
        DEFAULT_SERVICE_ACCOUNT_NAME,
        DEFAULT_IMAGE_VERSION,
        DEFAULT_SERVICES,
        DEFAULT_SSH_PUB_KEY_PATH,
        DEFAULT_DELETION_PROTECTION,
        DEFAULT_SECURITY_GROUP_IDS,
        DEFAULT_SUBCLUSTERTS
    )
#     delete_cluster(DEFAULT_CLUSTER_NAME)
