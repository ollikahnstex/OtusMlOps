from typing import Dict

from .yc_actors import YcDataprocClusterActor


class SparkClusterHandler:

    def __init__(self,
                 cluster_name: str,
                 bucket_name: str,
                 zone: str,
                 service_account_name: str,
                 image_version: str,
                 services: str,
                 ssh_pub_key_path: str,
                 deletion_protection: str,
                 security_group_ids: str,
                 subclusters: Dict):
        self.cluster_name = cluster_name
        self.bucket_name = bucket_name
        self.zone = zone
        self.service_account_name = service_account_name
        self.image_version = image_version
        self.services = services
        self.ssh_pub_key_path = ssh_pub_key_path
        self.deletion_protection = deletion_protection
        self.security_group_ids = security_group_ids
        self.subclusters = subclusters

        self.yc = YcDataprocClusterActor()
        self.yc.create_cluster(self.cluster_name,
                               self.bucket_name,
                               self.zone,
                               self.service_account_name,
                               self.image_version,
                               self.services,
                               self.ssh_pub_key_path,
                               self.deletion_protection,
                               self.security_group_ids,
                               self.subclusters)

        self.cluster_id = self.yc.get_cluster_id(cluster_name)
        self.masternode_fqdn = self.yc.get_node_fqdn(self.cluster_id, node_type='MASTERNODE')
        self.hdfs_url = f'hdfs://{self.masternode_fqdn}'
        self.hdfs_fraud_data_dir = self.hdfs_url + '/user/ubuntu/fraud_data/'

        self.exists = True

    def delete_cluster(self):
        if self.exists:
            self.yc.delete_cluster(self.cluster_name)
            self.exists = False
        else:
            print(f'Cluster {self.cluster_name} does not exists!')
