# Databricks notebook source
# MAGIC %md
# MAGIC Connecting to Azure Data Lake Storage Gen 2 Rebrickable data using Access Key Vault configured with the Access policies and Secret Scope configured in Databricks

# COMMAND ----------

service_credential = dbutils.secrets.get(
    scope="Rebrickable-Scope", key="Databricks-SP-Secret"
)

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

# COMMAND ----------

# MAGIC %md
# MAGIC The below code is reading the newly loaded data for today's day from ADLS Gen2 rebrickabledevadlsgen2 raw container and creating the data as Delta tables in curated container without any transformations and with the overwrite mode.

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import explode

year = datetime.today().strftime("%Y")
month = datetime.today().strftime("%m")
day = datetime.today().strftime("%d")

dataset = ["Sets", "Profile"]

path_to_file_sets = (
    "LEGO/"
    + dataset[0]
    + "/"
    + str(year)
    + "/"
    + str(month)
    + "/"
    + str(day)
    + "/"
    + "sets.csv"
)

dbutils.fs.ls(
    "abfss://raw@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/"
    + path_to_file_sets
)
df_sets = spark.read.csv(
    "abfss://raw@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/"
    + path_to_file_sets,
    header=True,
    inferSchema=True
)
df_sets.write.format("delta").mode('overwrite').save(
    "abfss://curated@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/"
    + "LEGO/"
    + dataset[0]
)

path_to_file_users = (
    "Users/"
    + dataset[1]
    + "/"
    + str(year)
    + "/"
    + str(month)
    + "/"
    + str(day)
    + "/"
    + "profile.json"
)

dbutils.fs.ls(
    "abfss://raw@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/"
    + path_to_file_users
)
df_profile = spark.read.json(
    "abfss://raw@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/"
    + path_to_file_users
)
df_profile.write.format("delta").mode('overwrite').save(
    "abfss://curated@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/"
    + "Users/"
    + dataset[1]
)


path_to_file_owned_sets = (
    "Users/"
    + dataset[0]
    + "/"
    + str(year)
    + "/"
    + str(month)
    + "/"
    + str(day)
    + "/"
    + "sets.json"
)

dbutils.fs.ls(
    "abfss://raw@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/"
    + path_to_file_owned_sets
)
df_owned_sets = spark.read.json(
    "abfss://raw@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/"
    + path_to_file_owned_sets
)
df_owned_sets.write.format("delta").mode('overwrite').save(
    "abfss://curated@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/"
    + "Users/"
    + dataset[0]
)

path_to_file_themes = (
    "LEGO/"
    + "Themes"
    + "/"
    + str(year)
    + "/"
    + str(month)
    + "/"
    + str(day)
    + "/"
    + "themes.csv"
)

dbutils.fs.ls(
    "abfss://raw@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/"
    + path_to_file_themes
)
df_themes = spark.read.csv(
    "abfss://raw@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/"
    + path_to_file_themes,
    header=True,
    inferSchema=True
)

#display(df_themes)
#display(df_profile)
#display(df_sets)
#display(df_owned_sets)

# COMMAND ----------

# MAGIC %md
# MAGIC The below code is cleaning the data to be able to insert it into Managed tables.

# COMMAND ----------

# Flatteing json hierarchy
df_owned_sets = df_owned_sets.withColumn(
    "explodedArray", explode(df_owned_sets.results)
)

# Selecting necessary columns
df_owned_sets = df_owned_sets.select("explodedArray.set.set_num")
df_profile = df_profile.select("user_id")

display(df_profile)
display(df_sets)
display(df_owned_sets)

# COMMAND ----------

# MAGIC %md
# MAGIC The below code is dynamically creating the Date dimension covering today's date.

# COMMAND ----------

#Generting the Date Dimension using pandas
import pandas as pd
import datetime as dt

def create_date_table(start='2024-12-01', end=datetime.today()):
    df = pd.DataFrame(pd.date_range(start, end), columns=['Date'])
    df["Month"] = df["Date"].dt.month
    df["Day"] = df["Date"].dt.day
    df["Year"] = df["Date"].dt.year
    df["Date"] = df["Date"].dt.date
    return df
df_date = spark.createDataFrame(create_date_table())

# COMMAND ----------

# MAGIC %md
# MAGIC The below code is creating the temporary views which will be used later on for upadting/inserting the dimension and fact tables.

# COMMAND ----------

# Creating the temporary views:
df_owned_sets.createOrReplaceTempView("owned_sets")
df_sets.createOrReplaceTempView("sets")
df_profile.createOrReplaceTempView("profiles")
df_themes.createOrReplaceTempView("themes")
df_date.createOrReplaceTempView("date")

# COMMAND ----------

# MAGIC %md
# MAGIC The Dimensions tables are SCD type 1 so the managed tables are being truncated before loading the new data.

# COMMAND ----------

# MAGIC %sql
# MAGIC --Truncating Dimension table:
# MAGIC TRUNCATE TABLE rebrickabledevadb.Rebrickable.date

# COMMAND ----------

# MAGIC %sql
# MAGIC --Loading the data to Date Dimension table:
# MAGIC INSERT INTO
# MAGIC rebrickabledevadb.Rebrickable.date
# MAGIC SELECT
# MAGIC   *
# MAGIC from
# MAGIC   date

# COMMAND ----------

# MAGIC %sql
# MAGIC --Truncating data to Sets Dimension table:
# MAGIC TRUNCATE TABLE rebrickabledevadb.Rebrickable.sets

# COMMAND ----------

# MAGIC %sql
# MAGIC --Loading the data to Sets Dimension table:
# MAGIC INSERT INTO
# MAGIC   rebrickabledevadb.Rebrickable.sets (
# MAGIC     SetNumber,
# MAGIC     SetName,
# MAGIC     CreationYear,
# MAGIC     NumberOfParts,
# MAGIC     Theme,
# MAGIC     ImageURL
# MAGIC   )
# MAGIC SELECT
# MAGIC   s.set_num,
# MAGIC   s.name,
# MAGIC   s.year,
# MAGIC   s.num_parts,
# MAGIC   t.name,
# MAGIC   s.img_url
# MAGIC FROM
# MAGIC   sets s
# MAGIC   LEFT JOIN themes t ON s.theme_id = t.id

# COMMAND ----------

# MAGIC %sql
# MAGIC --Truncating data to Profile dimension table:
# MAGIC TRUNCATE TABLE rebrickabledevadb.Rebrickable.profile

# COMMAND ----------

# MAGIC %sql
# MAGIC --Loading the data to Profile dimension table:
# MAGIC INSERT INTO
# MAGIC   rebrickabledevadb.Rebrickable.profile
# MAGIC SELECT
# MAGIC   user_id
# MAGIC from
# MAGIC   profiles

# COMMAND ----------

# MAGIC %md
# MAGIC In this project there is the assumption that we do not take into account owned sets which have been disposed and any changes to price and date so the merge function cover only the insert part.

# COMMAND ----------

# MAGIC %sql
# MAGIC --Inserting the newly added datasets to Fact Table Owned Sets
# MAGIC MERGE INTO rebrickabledevadb.Rebrickable.ownedsets USING owned_sets ON rebrickabledevadb.Rebrickable.ownedsets.set_num = owned_sets.set_num
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   (
# MAGIC     set_num,
# MAGIC     Profile_ID,
# MAGIC     Date,
# MAGIC     Price
# MAGIC   )
# MAGIC VALUES
# MAGIC   (
# MAGIC     owned_sets.set_num,
# MAGIC     null,
# MAGIC     CURRENT_DATE(),
# MAGIC     0.0
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC Becuase of the Rebrickable limition to list only owner's sets and not the other users we assume that all sets belong to one person so we can update all Onwed Sets with the same profile_id.

# COMMAND ----------

# MAGIC %sql
# MAGIC --Adding Profile_ID in the Fact Table Owned Sets
# MAGIC UPDATE
# MAGIC   rebrickabledevadb.Rebrickable.ownedsets
# MAGIC SET
# MAGIC   Profile_ID = (
# MAGIC     select
# MAGIC       user_id
# MAGIC     from
# MAGIC       rebrickabledevadb.Rebrickable.profile
# MAGIC   )
