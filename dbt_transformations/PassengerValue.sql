{{ config(materialized='table') }}

WITH parent_query AS (
    SELECT
        F.amount,
        F.booking_date,
        P.passenger_id,
        P.name       AS passenger_name,
        P.gender,
        P.nationality
    FROM workspace.gold.FactBookings F
    LEFT JOIN workspace.gold.DimPassengers P
        ON F.DimPassengersKey = P.DimPassengersKey
)

SELECT
    passenger_id,
    passenger_name,
    gender,
    nationality,
    COUNT(*) AS total_bookings,
    SUM(amount) AS total_spend,
    AVG(amount) AS avg_spend_per_booking,
    MIN(booking_date) AS first_booking_date,
    MAX(booking_date) AS last_booking_date
FROM parent_query
GROUP BY passenger_id, passenger_name, gender, nationality
