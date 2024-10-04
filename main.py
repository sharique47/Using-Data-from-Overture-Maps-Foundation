from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SQLContext
from itertools import islice
from pyspark.sql import *
from pyspark.sql.functions import monotonically_increasing_id, udf, col, count, hour, sum, minute, from_utc_timestamp, dayofmonth
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, col, udf, lit

from shapely.geometry import Point
from shapely.geometry import shape
from shapely.geometry.polygon import Polygon
import pandas as pd
from sqlalchemy import create_engine

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, ArrayType
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from datetime import datetime
from dateutil import tz
from h3 import h3
from pyspark.sql.types import IntegerType
import math
import json
import random
import argparse
from shapely.geometry import box
from pyspark.sql.functions import concat_ws

spark = SparkSession.builder.getOrCreate()

#Toyota, Hyundai, Kia and Nissan

category = "car_dealer"
dealer_name = "Toyota"
LATITUDE = latitude
LONGITUDE = longitude

# Reading the Overture data from my S3 bucket
path = "modify the s3 path"
#Load Overture data
Overture_df = spark.read.format("parquet").load(path)

df_places = Overture_df.filter(Overture_df["categories.primary"] == category)

df_places = df_places.select("id", "names.primary", "addresses", "geometry", "bbox")
# df_places.show()

#Define UDF for brand filtering
@udf("boolean")
def brands(name, dealer_name):
    if name and dealer_name.lower() in name.lower():
        return True
    return False

#Filter by dealer brand
df_brand = df_places.filter(brands(df_places["primary"], lit(dealer_name)))
df_brand.show()


#Define UDF for calculating centroids
@udf("string")
def lngUdf (xmin, ymin, xmax, ymax):
    bounds = (xmin, ymin, xmax, ymax)
    polygon = box(*bounds)
    return str(polygon.centroid.x)

@udf("string")
def latUdf (xmin, ymin, xmax, ymax):
    bounds = (xmin, ymin, xmax, ymax)
    polygon = box(*bounds)
    return str(polygon.centroid.y)

#Calculate centoid latitude and longitude
df_latlng = df_brand.withColumn('longitude', lngUdf(df_places.bbox.xmin, df_places.bbox.ymin, df_places.bbox.xmax, df_places.bbox.ymax))
df_latlng = df_latlng.withColumn('latitude', latUdf(df_places.bbox.xmin, df_places.bbox.ymin, df_places.bbox.xmax, df_places.bbox.ymax))


#Define UDF for distance calculation
@udf("string")
def distFrom(lat, lng):
    distance = h3.point_dist((LATITUDE, LONGITUDE), (float(lat), float(lng)), unit="km")
    return distance

#Calcuate distance from reference point
df_dist = df_latlng.withColumn('dist', distFrom(df_latlng.latitude, df_latlng.longitude))
df_dist = df_dist.filter(df_dist.dist < 30) 

df_dist.show()
print("Count")
print(df_dist.count())


df_dist = df_dist.withColumn(
    "addresses_str",
    concat_ws(
        "; ",  # Use a separator like "; " to concatenate the fields of 'addresses'
        col("addresses").getItem(0).getField("freeform"),
        col("addresses").getItem(0).getField("locality"),
        col("addresses").getItem(0).getField("postcode"),
        col("addresses").getItem(0).getField("region"),
        col("addresses").getItem(0).getField("country")
    )
)

# Connect to database using SQLAlchemy engine
username = 'USERNAME'
password = 'PASSWORD'
hostname = "HOSTNAME"
port = PORT
database = "DATABASE"
connectionProperties = {
    "user": username,
    "password": password,
    "driver": "org.postgresql.Driver"
}


jdbcUrl = f"JOBCURL"


write_df = df_dist.select(
    lit(dealer_name).alias("brand"),
    col("primary").alias("name"),
    col("addresses_str").alias("address"),
    col("latitude").cast("float"),
    col("longitude").cast("float")
)


# Write the DataFrame to the database
tableName = "TABLENAME IN SQL"
mode = "append"
write_df.write.jdbc(url=jdbcUrl, table=tableName, mode=mode, properties=connectionProperties)
print("DONE WRITE")


