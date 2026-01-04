{{ config(materialized='table') }}

WITH parent_query AS (
    SELECT
        F.amount,
        F.booking_date,
        A.airport_id,
        A.airport_name,
        A.city,
        A.country
    FROM workspace.gold.FactBookings F
    LEFT JOIN workspace.gold.DimAirports A
        ON F.DimAirportsKey = A.DimAirportsKey
)

SELECT
    airport_id,
    airport_name,
    city,
    country,
    date_trunc('month', booking_date) AS month,
    SUM(amount)  AS total_revenue,
    COUNT(*)     AS total_bookings
FROM parent_query
GROUP BY airport_id, airport_name, city, country, month
