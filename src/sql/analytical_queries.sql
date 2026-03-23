-- Query 1: Overall summary
SELECT
    COUNT(*)                             AS total_trips,
    ROUND(SUM(total_amount), 2)          AS total_revenue,
    ROUND(AVG(fare_amount), 2)           AS avg_fare,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_duration_mins
FROM public.yellow_trips;

-- Query 2: Revenue by time of day
SELECT
    time_of_day,
    COUNT(*)                        AS total_trips,
    ROUND(AVG(fare_amount), 2)      AS avg_fare,
    ROUND(AVG(tip_percentage), 2)   AS avg_tip_pct
FROM public.yellow_trips
GROUP BY time_of_day
ORDER BY total_trips DESC;

-- Query 3: Peak vs non peak
SELECT
    is_peak_hour,
    COUNT(*)                        AS total_trips,
    ROUND(AVG(fare_amount), 2)      AS avg_fare,
    ROUND(AVG(tip_percentage), 2)   AS avg_tip_pct
FROM public.yellow_trips
GROUP BY is_peak_hour;

-- Query 4: Top 10 busiest pickup zones
SELECT
    pulocationid,
    COUNT(*)                        AS total_pickups,
    ROUND(AVG(fare_amount), 2)      AS avg_fare,
    ROUND(SUM(total_amount), 2)     AS total_revenue
FROM public.yellow_trips
GROUP BY pulocationid
ORDER BY total_pickups DESC
LIMIT 10;
