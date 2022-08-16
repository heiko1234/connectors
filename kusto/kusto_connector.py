

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.identity import AzureCliCredential


class KustoConnector:
    def __init__(self, cluster_url="https://help.kusto.windows.net", sub_db= None, with_AAD=False):

        # https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-data/tests/sample.py

        self.cluster_url = cluster_url

        if with_AAD:
            connection_string = KustoConnectionStringBuilder.with_aad_device_authentication(connection_string= self.cluster_url)

        else:
            connection_string = KustoConnectionStringBuilder.with_az_cli_authentication(self.cluster_url)

        self.client = KustoClient(connection_string)
        self.sub_db = sub_db

    def get_credentail(self):
        credentail = AzureCliCredential()
        return credentail

    def fetch_kusto_data(self, query):
        response = self.client.execute(database=self.sub_db, query=query)
        output = dataframe_from_result_table(response.primary_results[0])

        return output




