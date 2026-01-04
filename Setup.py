# Create a volume
%sql
CREATE VOLUME workspace.raw.raw_volume

%sql
# CREATE SCHEMA
CREATE SCHEMA workspace.gold

# Create a directory inside the raw_volume
dbutils.fs.mkdirs("/Volumes/workspace/raw/raw_volume/raw-layer")

# Create different folders to upload the data
#Flights
dbutils.fs.mkdirs("/Volumes/workspace/raw/raw_volume/raw-layer/flights")

#Customers
dbutils.fs.mkdirs("/Volumes/workspace/raw/raw_volume/raw-layer/customers")

#Bookings
dbutils.fs.mkdirs("/Volumes/workspace/raw/raw_volume/raw-layer/bookings")

#Airports
dbutils.fs.mkdirs("/Volumes/workspace/raw/raw_volume/raw-layer/airports")

%sql
SELECT * FROM delta.`/Volumes/workspace/bronze/bronzevolume/flights/data/`

