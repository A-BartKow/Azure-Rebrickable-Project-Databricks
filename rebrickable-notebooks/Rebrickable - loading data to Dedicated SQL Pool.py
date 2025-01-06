# Databricks notebook source
service_credential = dbutils.secrets.get(
    scope="Rebrickable-Scope", key="Databricks-SP-Secret"
)
# Define the Service Principal credentials for the Azure storage account
spark.conf.set(
    "fs.azure.account.auth.type.rebrickabledevadlsgen2.dfs.core.windows.net", "OAuth"
)
spark.conf.set(
    "fs.azure.account.oauth.provider.type.rebrickabledevadlsgen2.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
)
spark.conf.set(
    "fs.azure.account.oauth2.client.id.rebrickabledevadlsgen2.dfs.core.windows.net",
    "36963351-9501-4671-b524-108d56d2656b",
)
spark.conf.set(
    "fs.azure.account.oauth2.client.secret.rebrickabledevadlsgen2.dfs.core.windows.net",
    service_credential,
)
spark.conf.set(
    "fs.azure.account.oauth2.client.endpoint.rebrickabledevadlsgen2.dfs.core.windows.net",
    "https://login.microsoftonline.com/818ef175-5c29-482f-b1cf-11f5f6f50f5f/oauth2/token",
)

## Define a separate set of service principal credentials for Azure Synapse Analytics (If not defined, the connector will use the Azure storage account credentials)
spark.conf.set(
    "spark.databricks.sqldw.jdbc.service.principal.client.id",
    "36963351-9501-4671-b524-108d56d2656b",
)
spark.conf.set(
    "spark.databricks.sqldw.jdbc.service.principal.client.secret", service_credential
)

# COMMAND ----------


tables = ['OwnedSets','Sets','Profile','Date']

for t in tables:
    df_profile = spark.read.table('rebrickabledevadb.rebrickable.' + t)
    df_profile.write.format("sqldw").option(
    "url",
    "jdbc:sqlserver://rebrickabledevsynapse.sql.azuresynapse.net:1433;database=rebrickabledevsqldw",
).option(
    "tempDir", "abfss://stage@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/" + t
).option(
    "forwardSparkAzureStorageCredentials", "false"
).option(
    "dbtable", "dbo." + t
).option(
    "enableServicePrincipalAuth", "true"
).mode('overwrite').save()
