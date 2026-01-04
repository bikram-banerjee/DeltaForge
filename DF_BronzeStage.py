# Initial volume creation

%sql
# BRONZE LAYER
CREATE VOLUME workspace.raw.bronze
CREATE VOLUME workspace.bronze.bronzevolume

%sql
#SILVER LAYER
CREATE VOLUME workspace.raw.silver
CREATE VOLUME workspace.silver.silvervolume


%sql
# GOLD LAYER
CREATE VOLUME workspace.raw.gold
CREATE VOLUME workspace.gold.goldvolume

#Initial Data Ingestion

dbutils.widgets.text("src","")

src_value = dbutils.widgets.get("src")

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"/Volumes/workspace/bronze/bronzevolume/{src_value}/checkpoint")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(f"/Volumes/workspace/raw/raw_volume/raw-layer/{src_value}/")
)

df.writeStream.format("delta")\
    .outputMode("append")\
    .trigger(once=True)\
    .option("checkpointLocation", f"/Volumes/workspace/bronze/bronzevolume/{src_value}/checkpoint")\
    .option("path", f"/Volumes/workspace/bronze/bronzevolume/{src_value}/data")\
    .start()

%sql
SELECT * FROM delta.`/Volumes/workspace/bronze/bronzevolume/customers/data`
