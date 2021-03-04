import commands_api


class DatabricksCatalog:

    """
    A class based representation of a Databricks Catalog (the Databases / Tables / Views registered in a Hive Metastore)
    """

    def __init__(self, workspace, token, cluster):
        self.workspace = workspace
        self.token = token
        self.cluster = cluster
        self.commands_api = commands_api.CommandsAPI(workspace=self.workspace,
                                                     token=self.token,
                                                     cluster=self.cluster,
                                                     language="sql")

    def get_all_databases(self, like=False):

        """
        Get all Databases within a catalog

        :param like: A regex that can be used to filter the list of databases
        :return: A dictionary of all databases within a catalog
        """

        print(f"Getting Databases in {self.workspace}")

        databases = []

        if like:
            query = f"SHOW DATABASES LIKE '{like}'"
        else:
            query = "SHOW DATABASES"

        command_id = self.commands_api.execute_query(query)
        res = self.commands_api.get_results(command_id)
        for database in list(res['results']['data']):
            database_name = database[0]
            command_id = self.commands_api.execute_query(f"DESCRIBE DATABASE {database_name}")
            res = self.commands_api.get_results(command_id)

            # In theory you could extract comments and if you were to use DESCRIBE EXTENDED even properties too
            location = res['results']['data'][2][1]

            print(f"Getting ACLs on database {database_name} in {self.workspace}")
            command_id = self.commands_api.execute_query(f"SHOW GRANT ON DATABASE {database_name}")
            acls = self.commands_api.get_results(command_id)['results']['data']

            databases.append({"NAME": database_name, "LOCATION": location, "ACLs": acls})

        return databases

    def create_databases(self, databases, source):

        """
        Creates Databases and applies ACLs within the catalog

        :param tables: A dictionary of databases, usually the output of the method get_all_databases() above
        :return: void
        """

        for database in databases:
            print(f"Creating Database {database['NAME']} in {self.workspace}")
            self.commands_api.execute_query(f"""
            CREATE DATABASE IF NOT EXISTS {database['NAME']} LOCATION '{database['LOCATION']}' 
            COMMENT 'Synced from workspace {source}'""")

            for acl in database["ACLs"]:
                self.commands_api.execute_query(f"""
                GRANT {acl[1]} ON {acl[2]} {acl[3]} TO {acl[0]}
                """)

        print("Database sync complete")

    def get_all_tables(self, databases, like=False):

        """
        Get all tables within a dictionary of Databases

        :param databases: A dictionary of databases, usually the output of the method get_all_databases() above
        :param like: A regex that can be used to filter the list of tables searched
        :return: A dictionary of tables
        """

        tables = []

        for database in databases:

            print(f"Getting Tables FOR {database['NAME']} in {self.workspace}")

            if like:
                query = f"SHOW TABLES IN {database['NAME']} LIKE '{like}'"
            else:
                query = f"SHOW TABLES IN {database['NAME']}"

            command_id = self.commands_api.execute_query(query)
            res = self.commands_api.get_results(command_id)

            for table in list(res['results']['data']):

                # Ignore temp tables
                if not bool(table[2]):
                    database = table[0]
                    table = table[1]

                    print(f"Getting CREATE TABLE statement for {database}.{table} in {self.workspace}")
                    command_id = self.commands_api.execute_query(f"SHOW CREATE TABLE {database}.{table}")
                    create = self.commands_api.get_results(command_id)['results']['data'][0][0]

                    print(f"Getting ACLs on table {database}.{table} in {self.workspace}")
                    command_id = self.commands_api.execute_query(f"SHOW GRANT ON {database}.{table}")
                    acls = self.commands_api.get_results(command_id)['results']['data']

                    tables.append({"DATABASE": database, "TABLE": table, "CREATE_STATEMENT": create, "ACLs": acls})

                else:
                    print(f"Not syncing temp table {table[0]}.{table[1]}")

        return tables

    def create_tables(self, tables):

        """
        Creates Tables and applies ACLs within the catalog

        :param tables: A dictionary of tables, usually the output of the method get_all_tables() above
        :return: void
        """

        for table in tables:
            print(f"Creating Table {table['DATABASE']}.{table['TABLE']} in {self.workspace}")
            self.commands_api.execute_query(table['CREATE_STATEMENT'])
            print(f"Applying ACLs to Table {table['DATABASE']}.{table['TABLE']} in {self.workspace}")
            for acl in table["ACLs"]:
                self.commands_api.execute_query(f"""
                GRANT {acl[1]} ON {acl[2]} {acl[3]} TO {acl[0]}
                """)

        print("Table sync complete")
