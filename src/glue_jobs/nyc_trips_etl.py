import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, hour, dayofweek, month, year,
    when, lit, round
)
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

S3_INPUT  = "s3://nyc-trips-pipeline-ravindra/raw/yellow-trips/"
S3_OUTPUT = "s3://nyc-trips-pipeline-ravindra/processed/yellow-trips/"

# ── EXTRACT ──────────────────────────────────────────────
logger.info("Reading raw data from S3...")
df = spark.read.parquet(S3_INPUT)
logger.info(f"Raw rows loaded: {df.count()}")

# ── DATA QUALITY ─────────────────────────────────────────
logger.info("Running data quality checks...")

df_clean = df.filter(
    col("tpep_pickup_datetime").isNotNull()  &
    col("tpep_dropoff_datetime").isNotNull() &
    col("fare_amount").isNotNull()           &
    (col("fare_amount") > 0)                 &
    (col("trip_distance") > 0)               &
    (col("passenger_count") > 0)             &
    (col("passenger_count") <= 6)
)

logger.info(f"Rows after quality check: {df_clean.count()}")

# ── TRANSFORM ─────────────────────────────────────────────
logger.info("Applying transformations...")

df_transformed = df_clean \
    .withColumn("pickup_hour",
        hour(col("tpep_pickup_datetime"))) \
    .withColumn("pickup_day_of_week",
        dayofweek(col("tpep_pickup_datetime"))) \
    .withColumn("pickup_month",
        month(col("tpep_pickup_datetime"))) \
    .withColumn("pickup_year",
        year(col("tpep_pickup_datetime"))) \
    .withColumn("trip_duration_minutes",
        round(
            (col("tpep_dropoff_datetime").cast("long") -
             col("tpep_pickup_datetime").cast("long")) / 60, 2
        )) \
    .withColumn("is_peak_hour",
        when(
            col("pickup_hour").between(7, 9) |
            col("pickup_hour").between(17, 19),
            lit(1)
        ).otherwise(lit(0))) \
    .withColumn("time_of_day",
        when(col("pickup_hour").between(6,  11), "morning")
        .when(col("pickup_hour").between(12, 16), "afternoon")
        .when(col("pickup_hour").between(17, 20), "evening")
        .otherwise("night")) \
    .withColumn("tip_percentage",
        round(col("tip_amount") / col("fare_amount") * 100, 2)) \
    .withColumn("revenue_per_mile",
        round(col("total_amount") / col("trip_distance"), 2))

df_final = df_transformed.select(
    "vendorid",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "ratecodeid",
    "store_and_fwd_flag",
    "pulocationid",
    "dolocationid",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "pickup_hour",
    "pickup_day_of_week",
    "pickup_month",
    "pickup_year",
    "trip_duration_minutes",
    "is_peak_hour",
    "time_of_day",
    "tip_percentage",
    "revenue_per_mile"
)

logger.info(f"Final row count: {df_final.count()}")

# ── LOAD — S3 PROCESSED ───────────────────────────────────
logger.info("Writing to S3 processed layer...")

df_final.write \
    .mode("overwrite") \
    .partitionBy("pickup_year", "pickup_month") \
    .option("compression", "snappy") \
    .parquet(S3_OUTPUT)

logger.info("S3 write complete. Job done.")

job.commit()
logger.info("Job completed successfully.")
