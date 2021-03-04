import requests


class CommandsAPI:

    """
    Wrapper around the Databricks Command Execution API (See the docs below for more detail)
    https://docs.databricks.com/dev-tools/api/1.2/index.html#command-execution-1
    """

    def __init__(self, workspace, token, cluster, language):
        self.workspace = workspace
        self.token = token
        self.language = language
        self.cluster = cluster

        r = requests.post(f"{self.workspace}/api/1.2/contexts/create",
                          headers={"Authorization": f"Bearer {self.token}"},
                          data={"language": self.language, "clusterId": self.cluster})

        self.context_id = int(r.json()["id"])

    @staticmethod
    def __check_status(r, *args, **kwargs):
        if r.json()['status'] != "Finished":
            print(f"Status is {r.json()['status']}, retrying...")

    def execute_query(self, command):
        r = requests.post(f"{self.workspace}/api/1.2/commands/execute",
                          headers={"Authorization": f"Bearer {self.token}"},
                          data={"language": self.language, "clusterId": self.cluster,
                                "contextId": self.context_id,
                                "command": command})

        return r.json()["id"]

    def get_results(self, command_id):

        r = requests.get(f"{self.workspace}/api/1.2/commands/status",
                         headers={"Authorization": f"Bearer {self.token}"},
                         params={"clusterId": self.cluster, "contextId": self.context_id,
                                 "commandId": command_id},
                         hooks={"response": CommandsAPI.__check_status})
        return r.json()
