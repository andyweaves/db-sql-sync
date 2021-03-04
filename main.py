import os
from databricks import catalog, rest_api
from dotenv import load_dotenv

load_dotenv()

if __name__ == '__main__':

    rest_api.Wrapper(workspace=os.getenv("WORKSPACE1_URL"),
                     token=os.getenv("WORKSPACE1_TOKEN")).start_cluster(os.getenv("WORKSPACE1_CLUSTER"))

    rest_api.Wrapper(workspace=os.getenv("WORKSPACE2_URL"),
                     token=os.getenv("WORKSPACE2_TOKEN")).start_cluster(os.getenv("WORKSPACE2_CLUSTER"))

    catalog_1 = catalog.DatabricksCatalog(workspace=os.getenv("WORKSPACE1_URL"),
                                          token=os.getenv("WORKSPACE1_TOKEN"),
                                          cluster=os.getenv("WORKSPACE1_CLUSTER"))

    catalog_2 = catalog.DatabricksCatalog(workspace=os.getenv("WORKSPACE2_URL"),
                                          token=os.getenv("WORKSPACE2_TOKEN"),
                                          cluster=os.getenv("WORKSPACE2_CLUSTER"))

    databases = catalog_1.get_all_databases(like="andrew_weaver*")

    catalog_2.create_databases(databases, catalog_1.workspace)

    tables = catalog_1.get_all_tables(databases)

    catalog_2.create_tables(tables)
