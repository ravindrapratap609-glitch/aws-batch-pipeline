DROP TABLE IF EXISTS public.yellow_trips;

CREATE TABLE IF NOT EXISTS public.yellow_trips (
    vendorid                INTEGER      ENCODE AZ64,
    tpep_pickup_datetime    TIMESTAMP    ENCODE AZ64,
    tpep_dropoff_datetime   TIMESTAMP    ENCODE AZ64,
    passenger_count         FLOAT        ENCODE ZSTD,
    trip_distance           FLOAT        ENCODE ZSTD,
    ratecodeid              FLOAT        ENCODE ZSTD,
    store_and_fwd_flag      VARCHAR(3)   ENCODE BYTEDICT,
    pulocationid            INTEGER      ENCODE AZ64,
    dolocationid            INTEGER      ENCODE AZ64,
    payment_type            INTEGER      ENCODE AZ64,
    fare_amount             FLOAT        ENCODE ZSTD,
    extra                   FLOAT        ENCODE ZSTD,
    mta_tax                 FLOAT        ENCODE ZSTD,
    tip_amount              FLOAT        ENCODE ZSTD,
    tolls_amount            FLOAT        ENCODE ZSTD,
    improvement_surcharge   FLOAT        ENCODE ZSTD,
    total_amount            FLOAT        ENCODE ZSTD,
    congestion_surcharge    FLOAT        ENCODE ZSTD,
    airport_fee             FLOAT        ENCODE ZSTD,
    pickup_hour             INTEGER      ENCODE AZ64,
    pickup_day_of_week      INTEGER      ENCODE AZ64,
    pickup_month            INTEGER      ENCODE AZ64,
    pickup_year             INTEGER      ENCODE AZ64,
    trip_duration_minutes   FLOAT        ENCODE ZSTD,
    is_peak_hour            INTEGER      ENCODE AZ64,
    time_of_day             VARCHAR(20)  ENCODE BYTEDICT,
    tip_percentage          FLOAT        ENCODE ZSTD,
    revenue_per_mile        FLOAT        ENCODE ZSTD
)
DISTKEY(pulocationid)
SORTKEY(tpep_pickup_datetime);


-- COPY command — load from S3 processed layer
COPY public.yellow_trips
FROM 's3://nyc-trips-pipeline-ravindra/processed/yellow-trips/'
IAM_ROLE 'arn:aws:iam::052150906093:role/RedshiftS3Role'
FORMAT AS PARQUET;

-- Verify load
SELECT COUNT(*) FROM public.yellow_trips;
