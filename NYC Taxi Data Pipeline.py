# Databricks notebook source
import urllib

urls = [
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet",
]

keys = ["2024-03", "2024-02", "2024-01"]

for i, u in enumerate(urls):
    url = "yellow_tripdata_{}.parquet".format(keys[i])
    urllib.request.urlretrieve(
        u, "/Volumes/112523chen/datalake-bronze/raw/{}".format(url)
    )

# COMMAND ----------

import urllib

url = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
urllib.request.urlretrieve(
    u, "/Volumes/112523chen/datalake-bronze/raw/taxi_zone_data.csv"
)
data.write.format("parquet").save(
    "/Volumes/112523chen/datalake-bronze/raw/taxi_zone_data.parquet"
)

# COMMAND ----------

taxi_data = spark.read.parquet(
    "/Volumes/112523chen/datalake-bronze/raw/yellow_tripdata_*.parquet"
)


taxi_data.write.format("delta").mode("overwrite").saveAsTable(
    "112523chen.`datalake-silver`.yellow_taxi_trips"
)

zone_data = spark.read.parquet(
    "/Volumes/112523chen/datalake-bronze/raw/taxi_zone_data.parquet"
)

zone_data.write.format("delta").mode("overwrite").saveAsTable(
    "112523chen.`datalake-silver`.taxi_zones"
)

# COMMAND ----------

spark.read.table("112523chen.`datalake-silver`.yellow_taxi_trips").withColumnRenamed(
    "VendorID", "vendor_key"
).withColumnRenamed("tpep_pickup_datetime", "pickup_datetime").withColumnRenamed(
    "tpep_dropoff_datetime", "dropoff_datetime"
).withColumnRenamed("PULocationID", "pickup_zone_key").withColumnRenamed(
    "DOLocationID", "dropoff_zone_key"
).withColumnRenamed("RatecodeID", "rate_code_key").withColumnRenamed(
    "payment_type", "payment_type_key"
).withColumnRenamed("extra", "extra_charges").withColumnRenamed(
    "Airport_fee", "airport_fee"
).write.format("delta").mode("overwrite").saveAsTable(
    "112523chen.`datalake-gold`.fct_yellow_taxi_trips"
)

spark.read.table("112523chen.`datalake-silver`.taxi_zones").withColumnRenamed(
    "LocationID", "zone_key"
).withColumnRenamed("Borough", "borough").withColumnRenamed(
    "Zone", "zone"
).write.format("delta").mode("overwrite").saveAsTable(
    "112523chen.`datalake-gold`.dim_taxi_zones"
)

# COMMAND ----------

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructType,
    StructField,
)

dim_payment_type_table_schema = StructType(
    [
        StructField("payment_type_key", IntegerType(), True),
        StructField("payment_type", StringType(), True),
    ]
)
payment_type_table_values = [
    (1, "Credit Card"),
    (2, "Cash"),
    (3, "No Charge"),
    (4, "Dispute"),
    (5, "Unknown"),
    (6, "Voided Trip"),
]

dim_payment = spark.createDataFrame(
    payment_type_table_values,
    schema=dim_payment_type_table_schema,
)

dim_payment.write.format("delta").mode("overwrite").saveAsTable(
    "112523chen.`datalake-gold`.dim_payments"
)

# COMMAND ----------

dim_rate_code_table_schema = StructType(
    [
        StructField("rate_code_key", IntegerType(), True),
        StructField("rate_code", StringType(), True),
    ]
)

rate_code_table_values = [
    (1, "Standard rate"),
    (2, "JFK"),
    (3, "Newark"),
    (4, "Nassau or Westchester"),
    (5, "Negotiated fare"),
    (6, "Group ride"),
]

dim_rate = spark.createDataFrame(
    rate_code_table_values, schema=dim_rate_code_table_schema
)

dim_rate.write.format("delta").mode("overwrite").saveAsTable(
    "112523chen.`datalake-gold`.dim_rates"
)

# COMMAND ----------

from pyspark.sql.functions import month, avg, udf, year, col

get_month_name_from_int = udf(
    lambda x: {
        1: "January",
        2: "February",
        3: "March",
        4: "April",
        5: "May",
        6: "June",
        7: "July",
        8: "August",
        9: "September",
        10: "October",
        11: "November",
        12: "December",
    }[x],
    StringType(),
)

fct_trips = spark.read.table("112523chen.`datalake-gold`.fct_yellow_taxi_trips")
dim_rate = spark.read.table("112523chen.`datalake-gold`.dim_rates")
dim_payment = spark.read.table("112523chen.`datalake-gold`.dim_payments")

final = (
    fct_trips.join(dim_rate, on="rate_code_key")
    .join(dim_payment, on="payment_type_key")
    .withColumn("month", get_month_name_from_int(month("pickup_datetime")))
    .withColumn("year", year("pickup_datetime"))
    .filter(col("year") == 2024)
)

final.select(
    [
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        col("trip_distance").alias("trip_distance_in_miles"),
        col("total_amount").alias("trip_total_cost"),
        "rate_code",
        "payment_type",
    ]
).write.format("delta").mode("overwrite").saveAsTable(
    "112523chen.`datamart`.trip_payments"
)

display(final)
