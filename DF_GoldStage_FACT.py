# Importing Libraries
from pyspark.sql.functions import *

# PARAMETERS

#Catalog Name
catalog = "workspace"

#Source Schema
source_schema = "silver"

#Source Object
source_object = "silver_bookings"

# CDC Column
cdc_col = "modifiedDate"

# Backdated Refresh
backdated_refresh = ""

#Source Fact Table
fact_table = f"{catalog}.{source_schema}.{source_object}"
 
#Target Schema
target_schema = "gold"

#Target Object
target_object = "FactBookings"

# Fact Key Cols List
fact_key_cols = ["DimPassengersKey", "DimFlightsKey", "DimAirportsKey", "booking_date"]

dimensions = [
    {
        "table": f"{catalog}.{target_schema}.DimPassengers",
        "alias": "DimPassengers",
        "join_keys": [("passenger_id", "passenger_id")] # (fact_col, dim_col)
    },
    {
        "table": f"{catalog}.{target_schema}.DimFlights",
        "alias": "DimFlights",
        "join_keys": [("flight_id", "flight_id")] # (fact_col, dim_col)
    },
    {
        "table": f"{catalog}.{target_schema}.DimAirports",
        "alias": "DimAirports",
        "join_keys": [("airport_id", "airport_id")] # (fact_col, dim_col)
    } 
]

#Columns we want to keep from Fact Table(besides surrogate keys)
fact_columns = ["amount", "booking_date", "modifiedDate"]

# LAST LOAD DATE

# Check if there is no backdated refresh provided
if len(backdated_refresh) == 0:

    # If the target table already exists in the workspace catalog
    if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):

        # Get the most recent value from the CDC column
        # This represents the last successful load timestamp
        last_load = spark.sql(
                          f"SELECT max({cdc_col}) "
                          f"FROM workspace.{target_schema}.{target_object}").collect()[0][0]
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


# DYNAMIC FACT QUERY [BRING KEYS]


def generate_fact_query_incremental(fact_table, dimensions, fact_columns, cdc_col, last_load):
    fact_alias = "f"

    # Base columns to select
    select_cols = [f"{fact_alias}.{col}" for col in fact_columns]

    #Build joins dynamically
    join_clauses = []

    for dim in dimensions:
        table_full = dim["table"]
        alias = dim["alias"]
        table_name = table_full.split('.')[-1]

        # Surrogate Key
        surrogate_key = f"{alias}.{table_name}Key"
        select_cols.append(surrogate_key)

        # Build ON Clause
        on_conditions = [
            f"{fact_alias}.{fk} = {alias}.{dk}" for fk, dk in dim["join_keys"]
        ]
        join_clause = f"LEFT JOIN {table_full} {alias} ON " + " AND ".join(on_conditions)
        join_clauses.append(join_clause)

    # Final SELECT and JOIN clauses
    select_clause = ",\n    ".join(select_cols)
    joins = "\n".join(join_clauses)

    # WHERE clause for incremental filtering
    where_clause = f"{fact_alias}.{cdc_col} >= TIMESTAMP('{last_load}')"

    # Final query
    query = f"""
SELECT
    {select_clause}
FROM
    {fact_table} {fact_alias}
{joins}
WHERE
    {where_clause}
""".strip()

    return query

query = generate_fact_query_incremental(fact_table, dimensions, fact_columns, cdc_col, last_load)

# DF_FACT

df_fact = spark.sql(query)

# UPSERT


# Fact Key Cols Merge Condition 
fact_key_cols_str = " AND ".join([f"src.{col} = trg.{col}" for col in fact_key_cols])


from delta.tables import DeltaTable

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):

    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
    dlt_obj.alias("trg").merge(df_fact.alias("src"), fact_key_cols_str)\
                        .whenMatchedUpdateAll(condition = f"src.{cdc_col} >= trg.{cdc_col}")\
                        .whenNotMatchedInsertAll()\
                        .execute()
else:
    
    df_fact.write.format("delta")\
            .mode("append")\
            .saveAsTable(f"{catalog}.{target_schema}.{target_object}")

%sql
SELECT * FROM workspace.gold.factbookings