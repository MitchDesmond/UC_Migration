# Databricks notebook source
# MAGIC %md
# MAGIC # Source Catalog inventory and Migration to UC Catalog
# MAGIC
# MAGIC The purpose of this notebook is to quickly build an inventory of the source catalog (for HMS use hive_metastore) and migrate the information into a UC catalog
# MAGIC
# MAGIC Migrations in this notebook:
# MAGIC - HMS Managed to UC Managed - using [clone](https://docs.databricks.com/delta/clone.html)
# MAGIC - HMS External to UC Managed  - using [clone](https://docs.databricks.com/delta/clone.html)
# MAGIC - HMS Managed(Mount) to UC External
# MAGIC - HMS External to UC External
# MAGIC
# MAGIC Author: mitch desmond (mitch.desmond@databricks.com)
# MAGIC last update: 5/9/23

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.dropdown("save_metadata_flag", "False", ["True", "False"], "Save Metadata Inventory?")
dbutils.widgets.text("save_metadata_location", "field_demos.mitch_desmond", "Save Metadata Location")
dbutils.widgets.text("source_catalog", "hive_metastore", "Source Catalog")
dbutils.widgets.text("destination_catalog", "field_demos", "Destination Catalog")
dbutils.widgets.dropdown("sync_dry_run_flag", "True", ["True", "False"], "Sync Dry Run?")
dbutils.widgets.text("external_location_credential", "poc-catalog", "External Location Credential")


# COMMAND ----------

save_metadata_flag = dbutils.widgets.get("save_metadata_flag")
#save_metadata_flag = dbutils.widgets.get("save_metadata_flag") == "True"
save_metadata_location = dbutils.widgets.get("save_metadata_location")
source_catalog = dbutils.widgets.get("source_catalog")
destination_catalog = dbutils.widgets.get("destination_catalog")
sync_dry_run_flag = dbutils.widgets.get("sync_dry_run_flag")
external_location_credential = dbutils.widgets.get("external_location_credential")

#optional
database_filter_list = ['dbdemos_lakehouse_churn_alan_mazankiewicz']
#database_filter_list = ["mitch_desmond_a2ol_da_dewd", "flightschool_mitch_desmond"]

# COMMAND ----------

#dependent packages
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Get Database Metadata

# COMMAND ----------

#set the source catalog as the active catalog
spark.sql(f'use catalog {source_catalog}')

#listDatabase creats metadata list of each database - takes longer
database_df = spark.createDataFrame(spark.catalog.listDatabases())


# COMMAND ----------

#check number of databases
print("total number of database in catalog: " + str(database_df.count()))

# COMMAND ----------

#Optional: if a sub list of databases was given filter for those databases
if len(database_filter_list) > 0:
  database_df = database_df.filter(database_df.name.isin(database_filter_list))
  #database_list = [x for x in database_list if x in database_filter_list]

print("filtered number of database in catalog: " + str(database_df.count()))

# COMMAND ----------

#save database metadata
if save_metadata_flag == "True":
  database_df.write.mode("overwrite").saveAsTable(save_metadata_location + ".database_metadata")

# COMMAND ----------

display(database_df.head(4))

# COMMAND ----------

# MAGIC %md
# MAGIC # Get Table Metadata

# COMMAND ----------

#create a list of all databases
database_list = database_df.select('name').rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

#remove - for performance testing
#database_list = database_list[0:100]

# COMMAND ----------

len(database_list)

# COMMAND ----------

#Fastest option to get table level metadata
from functools import reduce
from pyspark.sql import DataFrame
#set a blank list to collect results
SeriesAppend = []

for database_name in database_list:
  try:
    temp_df = spark.sql(f"SHOW TABLE EXTENDED in hive_metastore.{database_name} like '*'")
    SeriesAppend.append(temp_df)
  except Exception as e:
    error = str(e)[0:200]
    print(f"{database_name} failed due to error: {error}")

table_df = reduce(DataFrame.unionAll, SeriesAppend)

# COMMAND ----------

display(table_df.head(4))

# COMMAND ----------

#extract the detail information from the from the string column
from pyspark.sql.functions import regexp_extract, col
#remove all the column information after the word schema
#table_df = table_df.withColumn('information', regexp_extract(col('information'), '((.|\n|\r)*)Schema:', 1))
#extract the relevent information
table_df = table_df.withColumn('catalog', regexp_extract(col('information'), 'Catalog: (.*)', 1)) \
                      .withColumn('created_time', regexp_extract(col('information'), 'Created Time: (.*)', 1)) \
                      .withColumn('last_access', regexp_extract(col('information'), 'Last Access: (.*)', 1)) \
                      .withColumn('type', regexp_extract(col('information'), 'Type: (.*)', 1)) \
                      .withColumn('provider', regexp_extract(col('information'), 'Provider: (.*)', 1)) \
                      .withColumn('location', regexp_extract(col('information'), 'Location: (.*)', 1)) \
                      .withColumn('serde_library', regexp_extract(col('information'), 'Serde Library: (.*)', 1))

#change the provider for hive serde and ORC tables
#when serde_library like 'hadoop.hive.serde' then parquet, when serde_library like 'ql.io.orc' then ORC
table_df = table_df.withColumn("provider", when(table_df.provider == 'hive', 'parquet') \
                          .otherwise(table_df.provider))

table_df = table_df.withColumn("provider", when(table_df.serde_library == 'org.apache.hadoop.hive.ql.io.orc.OrcSerde', 'ORC') \
                          .otherwise(table_df.provider))


# COMMAND ----------

#check number of tables
print("total number of tables in databases: " + str(table_df.count()))

# COMMAND ----------

#save database metadata
if save_metadata_flag == "True":
  table_df.write.mode("overwrite").saveAsTable(save_metadata_location + ".table_metadata")

# COMMAND ----------

display(table_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: Get Table Detail Metadata - slow

# COMMAND ----------

##warning - may be very slow
##to do: add try except for errors
from functools import reduce
from pyspark.sql import DataFrame
SeriesAppend = []

#create a mapping for each table key
table_detail_list = table_df.filter("Type='MANAGED' or Type='EXTERNAL'") \
                    .select('Catalog', 'database', 'tableName').collect()
#run the loop to get the table detail
for table in table_detail_list:
 temp_df = spark.sql(f"describe detail {table.Catalog}.{table.database}.{table.tableName}")
 SeriesAppend.append(temp_df)

table_detail_df = reduce(DataFrame.unionAll, SeriesAppend)

# COMMAND ----------

#check number of tables
print("total number of tables detail: " + str(table_detail_df.count()))

# COMMAND ----------

#save database metadata
if save_metadata_flag == "True":
  table_detail_df.write.mode("overwrite").saveAsTable(save_metadata_location + ".table_detail_metadata")

# COMMAND ----------

#display(display(table_detail_df))

# COMMAND ----------

# MAGIC %md
# MAGIC # External Table to UC External

# COMMAND ----------

# MAGIC %md
# MAGIC ##Change managed mount tables to External
# MAGIC Note: you may not have any tables in this scenario

# COMMAND ----------

#to do: filter out tables that have isTemporary = 'true'
#find a way to set the owner of external location to a group - set this to a widget
# set database owner to widget

# COMMAND ----------

#filter for tables that are managed mount that need to be changed to external
mount_table_df = table_df.filter("type='MANAGED' and location not like 'dbfs:%'") \
                   .select('database', 'tableName')

#mount_table_df = table_df.select('database', 'tableName').limit(4)
print(mount_table_df.count())

# COMMAND ----------

#create a temp view so the data can be passed into scala
mount_table_df.createOrReplaceTempView('mount_table_temp')

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
# MAGIC import org.apache.spark.sql.catalyst.TableIdentifier
# MAGIC
# MAGIC val mount_table_temp = sqlContext.sql("select * from mount_table_temp")
# MAGIC
# MAGIC for (row <- mount_table_temp.rdd.collect)
# MAGIC {   
# MAGIC     val dbName = row.mkString(",").split(",")(0)
# MAGIC     val tableName = row.mkString(",").split(",")(1)
# MAGIC     print(dbName, tableName)
# MAGIC     val oldTable: CatalogTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName, Some(dbName)))
# MAGIC     val alteredTable: CatalogTable = oldTable.copy(tableType = CatalogTableType.EXTERNAL)
# MAGIC     spark.sessionState.catalog.alterTable(alteredTable)
# MAGIC }

# COMMAND ----------

for (row <- mount_table_temp.rdd.collect)
  {   
    val dbName = row.mkString(",").split(",")(0)
    val tableName = row.mkString(",").split(",")(1)
    print(dbName, tableName)
  }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create external locations and set credentials
# MAGIC Here we're assuming that:
# MAGIC - the same storage credential can access all external locations
# MAGIC - one external location per database will be created - this can be generalized to different levels of folder
# MAGIC - the external location name will be the last level of folder (on the initial code, the table name)

# COMMAND ----------

#to check: should this be run for only database that need an external location (have external tables)
#Note: make sure you have the storage credential set above
#external_location_credential = ''
#note: make sure you set the owner if you want that changed to a group
external_owner = 'alf@melmak.et'
#creat a list of databases and locations
database_external_list = database_df.select('name', 'locationUri').collect()
#create a list to hold output
SeriesAppend = []

for db in database_external_list:
  try:
    spark.sql(f"""CREATE EXTERNAL LOCATION IF NOT EXISTS {db.name}
                  URL '{db.locationUri}'
                  WITH (STORAGE CREDENTIAL {external_location_credential})""")
    #optional: change the owner of the external location
    #spark.sql("ALTER EXTERNAL LOCATION {db.name} OWNER TO `{external_owner}`")
    #add the success info to the dataframe
    SeriesAppend.append((db.name, db.locationUri, "Complete"))
  except Exception as e:
    #add the error information to the dataframe
    SeriesAppend.append((db.name, db.locationUri, str(e)[0:200]))
    print(f"could not create external location for db: {db.name} due to error: " + str(e)[0:200])

external_location_results = spark.createDataFrame(SeriesAppend, ['database_name', 'database_location', 'error'])

# COMMAND ----------

display(external_location_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create databases

# COMMAND ----------

#Create all missing databases on destination catalog
database_create_list = table_df.select("database").distinct().collect()
#set the database owner
db_owner = 'alf@melmak.et'
#create a list to hold output
SeriesAppend = []

for database in database_create_list:
    try:
      spark.sql(f"CREATE DATABASE IF NOT EXISTS {destination_catalog}.{database.database}")
      #Optional: change the database owner
      #spark.sql(f"ALTER SCHEMA {destination_catalog}.{database.database} OWNER TO `{db_owner}`")
      #add the success info to the dataframe
      SeriesAppend.append((destination_catalog, database.database, "Complete"))
    except Exception as e:
      #add the error information to the dataframe
      SeriesAppend.append((destination_catalog, database.database, str(e)[0:200]))
      print(f"could not create new database {destination_catalog}.{database.database}: " + str(e)[0:200])

create_database_results = spark.createDataFrame(SeriesAppend, ['catalog_name', 'database_name', 'error'])

# COMMAND ----------

display(create_database_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync Metadata - database level

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame
#Sync all new databases from source databases
database_sync_list = database_df.select("name").collect()
#create a list to hold output
SeriesAppend = []
#set dry run to "" to run actual sync
if sync_dry_run_flag == 'True':
  dry_run = "DRY RUN"
else:
  dry_run = ""

for database in database_sync_list:
    try:
      temp_df = spark.sql(f"SYNC DATABASE {destination_catalog}.{database.name} FROM {source_catalog}.{database.name} {dry_run}")
      SeriesAppend.append(temp_df)
    except Exception as e:
      print(f"could not sync new database {destination_catalog}.{database.name}: " + str(e)[0:200])

sync_database_results = reduce(DataFrame.unionAll, SeriesAppend)

# COMMAND ----------

display(sync_database_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync Metadata - table level

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame
#Sync all new table from source table
table_sync_list = table_df.select("database", "tableName").collect()
#create a list to hold output
SeriesAppend = []
#set dry run to "" to run actual sync
if sync_dry_run_flag == 'True':
  dry_run = "DRY RUN"
else:
  dry_run = ""

for table in table_sync_list:
    try:
      temp_df = spark.sql(f"""SYNC table {destination_catalog}.{table.database}.{table.tableName}
                              FROM {source_catalog}.{table.database}.{table.tableName} {dry_run}""")
      SeriesAppend.append(temp_df)
    except Exception as e:
      print(f"could not sync new table {destination_catalog}.{table.database}.{table.tableName}: " + str(e)[0:200])

sync_table_results = reduce(DataFrame.unionAll, SeriesAppend)

# COMMAND ----------

display(sync_table_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: Create UC External table individualy - instead of using DB Sync

# COMMAND ----------

#create a list of external tables
##Note: this process will not suport updated schema changes and the table will have to be dropped and rebuilt
table_create_list = table_df.select("database", "tableName", "provider", "location").distinct().collect()
#create a list to hold output
SeriesAppend = []

#run for loop to create tables
for table in table_create_list:
    try:
      spark.sql(f"""CREATE EXTERNAL TABLE IF NOT EXISTS {destination_catalog}.{table.database}.{table.tableName}
                USING {table.provider}
                LOCATION '{table.location}'""")
      #add the success info to the dataframe
      SeriesAppend.append((table.database, table.tableName, table.provider, "EXTERNAL", table.location, "Complete"))
    except Exception as e:
      #add the error information to the dataframe
      SeriesAppend.append((table.database, table.tableName, table.provider, "EXTERNAL", table.location, str(e)[0:200]))
      print(f"could not create new table {destination_catalog}.{table.database}.{table.tableName}: " + str(e)[0:200])

create_table_results = spark.createDataFrame(SeriesAppend, ['database', 'tableName', 'provider', 'type', 'location', 'error'])

# COMMAND ----------

display(create_table_results)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Managed to UC Managed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deep clone Managed tables into UC Managed

# COMMAND ----------

#clone table list
table_clone_list = table_df.filter("type='MANAGED'").select("database", "tableName").distinct().collect()
#create a list to hold output
SeriesAppend = []

for table in table_clone_list:
    try:
      #print(f"Converting table {table.database}.{table.tableName}...")
      spark.sql(f"CREATE OR REPLACE TABLE {destination_catalog}.{table.database}.{table.tableName} DEEP CLONE {source_catalog}.{table.database}.{table.tableName}")
      #add the success info to the dataframe
      SeriesAppend.append((table.database, table.tableName, "Complete"))

    except Exception as e:
      #add the error information to the dataframe
      SeriesAppend.append((table.database, table.tableName, str(e)[0:200]))
      print(f"could not create new table {destination_catalog}.{table.database}.{table.tableName}: " + str(e)[0:200])

create_table_results = spark.createDataFrame(SeriesAppend, ['database', 'tableName', 'error']) 

# COMMAND ----------

#add table properties to the clone
##SET TBLPROPERTIES('delta.minReaderVersion' = '1', 'delta.minWriterVersion' = '2')

# COMMAND ----------

# MAGIC %md 
# MAGIC # External to UC Managed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deep clone external files into UC delta Managed - source file can be parquet, delta, or Iceberg

# COMMAND ----------

#example - https://learn.microsoft.com/en-us/azure/databricks/delta/clone-parquet
# CREATE OR REPLACE TABLE <target_table_name> CLONE parquet.`/path/to/data`;
#clone table list
table_clone_list = table_df.select("database", "tableName", "provider", "location").distinct().collect()
#create a list to hold output
SeriesAppend = []

for table in table_clone_list:
    try:
      #print(f"Converting table {table.database}.{table.tableName}...")
      spark.sql(f"""CREATE OR REPLACE TABLE {destination_catalog}.{table.database}.{table.tableName}
                DEEP CLONE {table.provider}.`{table.location}`""")
      #add the success info to the dataframe
      SeriesAppend.append((table.database, table.tableName, "Complete"))

    except Exception as e:
      #add the error information to the dataframe
      SeriesAppend.append((table.database, table.tableName, str(e)[0:200]))
      print(f"could not create new table {destination_catalog}.{table.database}.{table.tableName}: " + str(e)[0:200])

create_table_results = spark.createDataFrame(SeriesAppend, ['database', 'tableName', 'error']) 

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleanup for testing

# COMMAND ----------

# #drop the test migration databases that were created
# for database in databases:
#     spark.sql(f"DROP DATABASE {destination_catalog}.{database[0]} CASCADE")     
