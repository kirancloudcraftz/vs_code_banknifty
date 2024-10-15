from google.cloud import bigquery 
# import pandas as pd

query = """
SELECT * FROM eis-global-dev.BankNiftyTest.BankNifty WHERE EXTRACT(Date FROM ExchDateTime) = '2020-01-02'
"""
# Construct a BigQuery client object.
client = bigquery.Client()

# query job to complete and convert it into pandas dataframe and print it with schema
df = client.query(query).to_dataframe()
print(df)
df.info()
