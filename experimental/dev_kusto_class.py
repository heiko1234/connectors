




# https://github.com/Azure/azure-cli

# https://stackoverflow.com/questions/56334954/how-to-properly-authenticate-kusto-using-a-python-client



# from ensurepip import version
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.identity import AzureCliCredential






cluster_url= "https://help.kusto.windows.net"
db = "Samples"
query = "StormEvents | take 10"




# ########


connection_string = KustoConnectionStringBuilder.with_aad_device_authentication(connection_string= cluster_url)

client = KustoClient(connection_string)

response = client.execute(database=db, query=query)
output = dataframe_from_result_table(response.primary_results[0])
output



# 

# pwsh: az --version


credentail = AzureCliCredential()

connection_string = KustoConnectionStringBuilder.with_az_cli_authentication(cluster_url)

client = KustoClient(connection_string)

# kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster)
# client = KustoClient(kcsb)

response = client.execute(database=db, query=query)
output = dataframe_from_result_table(response.primary_results[0])
output



# azure.kusto.data.exceptions.KustoAuthenticationError: KustoAuthenticationError('AzCliTokenProvider', 
# 'KustoClientError("Failed to obtain Az Cli token for 'https://help.kusto.windows.net'.
# \nPlease be sure AzCli version 2.3.0 and above is intalled.\nPlease run 'az login' to set up an account")', 
# '{'authority:': 'AzCliTokenProvider', 'kusto_uri': 'https://help.kusto.windows.net'}')


