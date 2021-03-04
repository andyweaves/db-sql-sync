import requests
import time


class Wrapper:

    def __init__(self, workspace, token):
        self.workspace = workspace
        self.token = token

    def start_cluster(self, cluster_id):

        status = self.get_cluster(cluster_id)

        if status != "RUNNING":

            print(f"Starting cluster {cluster_id} please wait...")

            requests.post(f"{self.workspace}/api/2.0/clusters/start",
                          headers={"Authorization": f"Bearer {self.token}"},
                          json={"cluster_id": cluster_id})

            self.wait_until_running(cluster_id, status)

        else:
            print(f"Cluster {cluster_id} is already {status}")

    def get_cluster(self, cluster_id):

        r = requests.get(f"{self.workspace}/api/2.0/clusters/get",
                         headers={"Authorization": f"Bearer {self.token}"},
                         params={"cluster_id": cluster_id})

        return r.json()["state"]

    def wait_until_running(self, cluster_id, status):

        while status != "RUNNING":
            if self.get_cluster(cluster_id) != "RUNNING":
                print(f"The status of cluster {cluster_id} is {status}, waiting...")
                time.sleep(5)
            else:
                status = self.get_cluster(cluster_id)
                print(f"The status of cluster {cluster_id} is {status}, ready to proceed...")
