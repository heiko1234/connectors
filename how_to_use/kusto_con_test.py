

from kusto.kusto_connector import (
    KustoConnector
)



cluster_url= "https://help.kusto.windows.net"
db = "Samples"
query = "StormEvents | take 10"



kusto_datafetcher = KustoConnector(cluster_url="https://help.kusto.windows.net", sub_db= db, with_AAD=True)


data=kusto_datafetcher.fetch_kusto_data(query=query)

data.head()
list(data.columns)

data

