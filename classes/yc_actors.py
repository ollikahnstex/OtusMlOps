import re
from typing import Dict

from .terminal_actors import TerminalActor


VALID_NODE_TYPES = {
    'MASTERNODE',
    'DATANODE',
    'COMPUTENODE',
}


class YcDataprocClusterActor:

    def __init__(self):
        self.terminal = TerminalActor()

    def create_cluster(self,
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
        yc_create_cmd = 'yc dataproc cluster create'
        subclusters_info = self._prepare_subclusters_info(subclusters)
        creating_command = f'''{yc_create_cmd} {cluster_name} \\
            --bucket={bucket_name} \\
            --zone={zone} \\
            --service-account-name={service_account_name} \\
            --version={image_version} \\
            --services={services} \\
            --ssh-public-keys-file="{ssh_pub_key_path}" \\
            {subclusters_info} \\
            --deletion-protection={deletion_protection} \\
            --security-group-ids={security_group_ids}'''
        print(f'Creating new Spark-cluster {cluster_name}...')
        self.terminal.run_shell_cmd(creating_command, print_stdout=True)
        print(f'Spark-cluster {cluster_name} successfully created!')

    def delete_cluster(self, cluster_name: str):
        yc_delete_cmd = 'yc dataproc cluster delete'
        deleting_command = f'{yc_delete_cmd} --name={cluster_name}'
        print(f'Deleting Spark-cluster {cluster_name}...')
        self.terminal.run_shell_cmd(deleting_command, print_stdout=True)
        print(f'Cluster {cluster_name} successfully deleted!')

    def get_cluster_id(self, cluster_name: str) -> str:
        cmd_get_cluster_id = ' | '.join([
            "yc dataproc cluster list",
            f"grep '{cluster_name}'",
            "awk '{print $2}'"
        ])
        print('Getting cluster ID...')
        res = self.terminal.run_shell_cmd(cmd_get_cluster_id)
        cluster_id = res.stdout.decode("utf-8")
        cluster_id = re.sub(r'[\r\n\t]', '', cluster_id)
        print(f'Cluster ID successfully got: {cluster_id}!')
        return cluster_id

    def get_node_fqdn(self, cluster_id: str, node_type: str = 'MASTERNODE') -> str:
        node_type = node_type.upper()
        self._check_node_type(node_type)
        getting_fqdn_cmd = ' | '.join([
            f"yc dataproc cluster list-hosts {cluster_id}",
            f"grep '{node_type}'",
            "awk '{print $2}'"
        ])
        print(f'Getting node ({node_type}) FQDN...')
        res = self.terminal.run_shell_cmd(getting_fqdn_cmd)
        node_fqdn = res.stdout.decode("utf-8")
        node_fqdn = re.sub(r'[\r\n\t]', '', node_fqdn)
        print(f'Node ({node_type}) FQDN successfully got: {node_fqdn}!')
        return node_fqdn

    @staticmethod
    def _check_node_type(node_type):
        if node_type not in VALID_NODE_TYPES:
            raise ValueError(f'Value node_type={node_type} is not valid! '
                             f'Select from: {",".join(VALID_NODE_TYPES)}!')

    @staticmethod
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
