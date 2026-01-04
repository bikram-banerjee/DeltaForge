# Importing Libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Parameters

# DIMENSION FOR FLIGHTS

#Catalog Name
catalog = "workspace"

# Key Cols list
key_cols = "['flight_id']"
key_cols_list = eval(key_cols)

# CDC Column
cdc_col = "modifiedDate"

# Backdated Refresh
backdated_refresh = ""

#Source Object
source_object = "silver_flights"

#Source Schema
source_schema = "silver"

#Target Schema
target_schema = "gold"

#Target Object
target_object = "DimFlights"

#Surrogate Key
surrogate_key = "DimFlightsKey"

# DIMENSION FOR AIRPORTS

Catalog Name
catalog = "workspace"

# Key Cols list
key_cols = "['airport_id']"
key_cols_list = eval(key_cols)

# CDC Column
cdc_col = "modifiedDate"

# Backdated Refresh
backdated_refresh = ""

#Source Object
source_object = "silver_airports"

#Source Schema
source_schema = "silver"

#Target Schema
target_schema = "gold"

#Target Object
target_object = "DimAirports"

#Surrogate Key
surrogate_key = "DimAirportsKey"

# DIMENSION FOR PASSENGERS

#Catalog Name
catalog = "workspace"

# Key Cols list
key_cols = "['passenger_id']"
key_cols_list = eval(key_cols)

# CDC Column
cdc_col = "modifiedDate"

# Backdated Refresh
backdated_refresh = ""

#Source Object
source_object = "silver_passengers"

#Source Schema
source_schema = "silver"

#Target Schema
target_schema = "gold"

#Target Object
target_object = "DimPassengers"

#Surrogate Key
surrogate_key = "DimPassengersKey"

# INCREMENTAL DATA INGESTION


# Last Load Date

# Check if there is no backdated refresh provided
if len(backdated_refresh) == 0:

    # If the target table already exists in the workspace catalog
    if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):

        # Get the most recent value from the CDC column
        # This represents the last successful load timestamp
        last_load = (
            spark.sql(
                f"SELECT max({cdc_col}) "
                f"FROM workspace.{target_schema}.{target_object}"
            )
            .collect()[0][0]
        )
    else:
        # If the table does not exist, initialize with a very old timestamp
        # This ensures a full load will occur
        last_load = "1900-01-01 00:00:00"
else:
    # If a backdated refresh value is provided,
    # use it instead of calculating the last load time
    last_load = backdated_refresh


#Test the last load
last_load


df_src = spark.sql(f"SELECT * FROM {source_schema}.{source_object} WHERE {cdc_col} > '{last_load}'") # "=" is kept to test upsert.


df_src.display()


# OLD vs NEW RECORDS

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):

    # Key Column String for Incremental
    key_cols_string_inc = ", ".join(key_cols_list)

    df_trg = spark.sql(f"SELECT {key_cols_string_inc}, {surrogate_key}, create_date, update_date FROM {catalog}.{target_schema}.{target_object}")

else:
    
    # Key Colunns String for Initial
    key_cols_string_init = [f"'' AS {i}" for i in key_cols_list]
    key_cols_string_init = ", ".join(key_cols_string_init)

    df_trg = spark.sql(f"""SELECT {key_cols_string_init}, CAST('0' AS INT) AS {surrogate_key}, CAST('1900-01-01 00:00:00' AS timestamp) AS create_date, CAST('1900-01-01 00:00:00' AS timestamp) AS update_date WHERE 1 = 0 """)


df_trg.display()

#JOIN CONDITION**

join_condition = ' AND '.join([f"src.{i} = trg.{i}" for i in key_cols_list])

df_src.createOrReplaceTempView("src")
df_trg.createOrReplaceTempView("trg")

df_join = spark.sql(f"""
              SELECT src.*,
                     trg.{surrogate_key},
                     trg.create_date,
                     trg.update_date
              FROM src
              LEFT JOIN trg
              ON {join_condition}
                    """)

df_join.display()

#FOR OLD RECORDS
df_old = df_join.filter(col(f'{surrogate_key}').isNotNull())

#FOR NEW RECORDS
df_new = df_join.filter(col(f'{surrogate_key}').isNull())


# ENRICHING DFS

# Preparing DF_OLD
df_old_enr = df_old.withColumn('update_date', current_timestamp())

# Preparing DF_NEW
if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
  max_surrogate_key = spark.sql(f"""
                        SELECT max({surrogate_key}) FROM {catalog}.{target_schema}.{target_object}
                      """).collect()[0][0]
  df_new_enr = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key)+lit(1)+ monotonically_increasing_id())\
                .withColumn('create_date', current_timestamp())\
                .withColumn('update_date', current_timestamp())

else:
  max_surrogate_key = 0
  df_new_enr = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key)+lit(1)+ monotonically_increasing_id())\
                .withColumn('create_date', current_timestamp())\
                .withColumn('update_date', current_timestamp())


# UNION OLD AND NEW RECORDS
df_union = df_old_enr.unionByName(df_new_enr)
df_union.display()

# UPSERT

from delta.tables import DeltaTable

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):

    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
    dlt_obj.alias("trg").merge(df_union.alias("src"), f"trg.{surrogate_key} = src.{surrogate_key}")\
                        .whenMatchedUpdateAll(condition = f"src.{cdc_col} >= trg.{cdc_col}")\
                        .whenNotMatchedInsertAll()\
                        .execute()

else:

    df_union.write.format("delta")\
            .mode("append")\
            .saveAsTable(f"{catalog}.{target_schema}.{target_object}")
    

