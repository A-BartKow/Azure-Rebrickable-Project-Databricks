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
# MAGIC The below code in cell has been designed to execute the first full load of the Rebrickable project.
# MAGIC The code bedlow is reading two different source of data:
# MAGIC - "LEGO" source data, which is part of Rebrickable data obtained from Rebrickable Downloads from Web page in form of CSV files.
# MAGIC - "Users" source data, which was obtained from Rebrickable API as json files. 
# MAGIC
# MAGIC The data in raw container is partitioned by:
# MAGIC 1. Source - LEGO or Users API
# MAGIC 2. Dataset
# MAGIC 3. Year/Month/Day
# MAGIC
# MAGIC The solution has been designed as follows:
# MAGIC 1. Directories are being read from raw container dynamically.
# MAGIC 2. There is the assumption that file names are taken from Rebrickable download page and Datasets follows the file names without special characters and are written in camel case.
# MAGIC 3. The files are being written to curated container as Delta Tables without no transformations.
# MAGIC 4. The full load is being done on particular day give as a value to attributes

# COMMAND ----------

# Import necessary variables:

from pyspark.sql.functions import split
import os


def get_dataset_list(source_name):
    # Lisiting all the directories created per datasets in particular source
    directories_list = dbutils.fs.ls(
        "abfss://raw@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/"
        + source_name
    )
    # "Creating a dataframe with the list of datasets names
    dataset_list = spark.createDataFrame(directories_list).select("name")
    # Removing the "/" character from dataset name by using split function and taking the first element of the resulted table
    dataset_list = dataset_list.withColumn("name", split(dataset_list.name, "/")[0])
    return dataset_list


def construct_filename(dataset_list, fileformat):
    dict = {}
    for i in dataset_list.collect():
        # If the dataset name starts with Inventory there is a different naming convention for a file with the underscore"
        if "Inventory" in i.name:
            dict[i.name] = (
                i.name[: len("Inventory")].lower()
                + "_"
                + i.name[9:].lower()
                + fileformat
            )
        else:
            dict[i.name] = i.name.lower() + fileformat
    return dict


# Reading the csv files iterativaley to dataframes and saving as delta tables in curated container for LEGO source:
def read_and_save_csv_to_delta(names_dictionary, year, month, day, source_name):
    for dataset in names_dictionary.keys():
        path_to_file = (
            source_name
            + "/"
            + dataset
            + "/"
            + str(year)
            + "/"
            + str(month)
            + "/"
            + str(day)
            + "/"
            + names_dictionary[dataset]
        )
        try:
            dbutils.fs.ls(
                "abfss://raw@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/"
                + path_to_file
            )
            df = spark.read.csv(
                "abfss://raw@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/"
                + path_to_file,
                header=True,
                inferSchema=True,
            )
            df.write.format("delta").save(
                "abfss://curated@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/"
                + source_name
                + "/"
                + dataset
            )
        except:
            break


# Reading the json files iterativaley to dataframes and saving as delta tables in curated container for LEGO source:
def read_and_save_json_to_delta(names_dictionary, year, month, day, source_name):
    for dataset in names_dictionary.keys():
        path_to_file = (
            source_name
            + "/"
            + dataset
            + "/"
            + str(year)
            + "/"
            + str(month)
            + "/"
            + str(day)
            + "/"
            + names_dictionary[dataset]
        )
        try:
            dbutils.fs.ls(
                "abfss://raw@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/"
                + path_to_file
            )
            df = spark.read.json(
                "abfss://raw@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/"
                + path_to_file
            )
            df.write.format("delta").save(
                "abfss://curated@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/"
                + source_name
                + "/"
                + dataset
            )
        except:
            break


# Initialize variables for LEGO source:
dataset_list = get_dataset_list("LEGO")
dataset_filename_dict = construct_filename(dataset_list, ".csv")

# Initialize variables for Users source:
dataset_list_users = get_dataset_list("Users")
dataset_filename_dict_users = construct_filename(dataset_list_users, ".json")

# Initialize partition variables for doing the first full load:
year = "2025"
month = "01"
day = "02"

read_and_save_json_to_delta(dataset_filename_dict_users, year, month, day, "Users")
read_and_save_csv_to_delta(dataset_filename_dict, year, month, day, "LEGO")

# COMMAND ----------

# MAGIC %md
# MAGIC Below code is used for inspecting the data and creating the data warehouse model as follows:
# MAGIC 1. 4 Dimensions table
# MAGIC - Sets - covering all lego sets listed at the Rebrickable webpage (Sets list should also cover the "Theme" value from Themes delta table)
# MAGIC - Date - dimension for dates
# MAGIC - Profile - dimension for user profile
# MAGIC 2. Fact table:
# MAGIC - Owned Sets - table containing list of all sets owned by particular user with date of purchase and price

# COMMAND ----------

import pandas as pd
import datetime as dt

df_sets = spark.read.format("delta").load(
    "abfss://curated@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/LEGO/Sets"
)

df_themes = spark.read.format("delta").load(
    "abfss://curated@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/LEGO/Themes"
)

# Renaming the columns of Sets Delta Table
df_sets = df_sets.withColumnsRenamed(
    {
        "set_num": "SetNumber",
        "name": "SetName",
        "year": "CreationYear",
        "num_parts": "NumberOfParts",
        "theme_id": "ThemeID",
        "img_url": "ImageURL",
    }
)

# Joining the Themes table to Sets
df_sets = df_sets.join(df_themes, df_sets.ThemeID == df_themes.id, "left")
df_sets = df_sets.select(
    "SetNumber", "SetName", "CreationYear", "NumberOfParts", "name", "ImageURL"
)
df_sets = df_sets.withColumnRenamed("name", "Theme")
display(df_sets)

# Creating the Temporary view to save it later as managed table in Hive metastore
df_sets.createOrReplaceTempView("Sets")

# COMMAND ----------

from pyspark.sql.functions import explode
import pandas as pd
import datetime as dt

df_owned_sets = spark.read.format("delta").load(
    "abfss://curated@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/Users/Sets"
)
# Flatteing json hierarchy
df_owned_sets = df_owned_sets.withColumn(
    "explodedArray", explode(df_owned_sets.results)
)

# Selecting necessary columns
df_owned_sets = df_owned_sets.select("explodedArray.set.set_num")

# Creating the Temporary view to save it later as external table in Hive metastore
df_owned_sets.createOrReplaceTempView("OwnedSets")


# Create a Delta Table for Profile dimension
df_profile = spark.read.format("delta").load(
    "abfss://curated@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/Users/Profile"
)
df_profile = df_profile.select("user_id")
display(df_profile)

# Creating the Temporary view to save it later as external table in Hive metastore
df_profile.createOrReplaceTempView("Profile")


# Create a Date dimension table
def create_date_table(start="2024-12-01", end="2025-12-31"):
    df = pd.DataFrame(pd.date_range(start, end), columns=["Date"])
    df["Month"] = df["Date"].dt.month
    df["Day"] = df["Date"].dt.day
    df["Year"] = df["Date"].dt.year
    df["Date"] = df["Date"].dt.date
    return df


# Converting pandas dataframe to Spark dataframe to allow execute 'createOrReplaceTempView' command:
df_date = spark.createDataFrame(create_date_table())

# Creating the Temporary view to save it later as external table in Hive metastore
df_date.createOrReplaceTempView("Date")

# COMMAND ----------

# MAGIC %md
# MAGIC Creating the new catalog for schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG rebrickabledevadb
# MAGIC MANAGED LOCATION 'abfss://cleansed@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/'

# COMMAND ----------

# MAGIC %md
# MAGIC Creating the new schema for external tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE rebrickabledevadb.Rebrickable

# COMMAND ----------

# MAGIC %md
# MAGIC Creating the external location for 'cleansed' container in Rebrickable ADLS Gen 2 to be able to create an external tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION `RebrickableCleansed` URL 'abfss://cleansed@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/'
# MAGIC     WITH (CREDENTIAL `rebrickabledevadbconnector`)

# COMMAND ----------

# MAGIC %md
# MAGIC Creating the external tables using CTAS and temporary views, which have been created in previous steps.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE rebrickabledevadb.Rebrickable.Sets 
# MAGIC LOCATION 'abfss://cleansed@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/Sets'
# MAGIC AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   Sets;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE rebrickabledevadb.Rebrickable.OwnedSets 
# MAGIC LOCATION 'abfss://cleansed@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/OwnedSets'
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC from
# MAGIC   OwnedSets

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE rebrickabledevadb.Rebrickable.Profile 
# MAGIC LOCATION 'abfss://cleansed@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/Profile'
# MAGIC AS
# MAGIC SELECT
# MAGIC   *
# MAGIC from
# MAGIC   Profile

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE rebrickabledevadb.Rebrickable.Date 
# MAGIC LOCATION 'abfss://cleansed@rebrickabledevadlsgen2.dfs.core.windows.net/Rebrickable/Date'
# MAGIC AS
# MAGIC SELECT
# MAGIC   *
# MAGIC from
# MAGIC   Date

# COMMAND ----------

# MAGIC %sql
# MAGIC --Adding necessary fields to Fact Table
# MAGIC ALTER TABLE
# MAGIC   rebrickabledevadb.Rebrickable.OwnedSets
# MAGIC ADD
# MAGIC   COLUMN Profile_ID LONG,
# MAGIC   Date DATE,
# MAGIC   Price DECIMAL(10, 2)
