{{ config(materialized='table') }}

WITH parent_query AS (
    SELECT
        F.amount,
        F.booking_date,
        FL.origin,
        FL.destination,
        FL.airline
    FROM workspace.gold.FactBookings F
    LEFT JOIN workspace.gold.DimFlights FL
        ON F.DimFlightsKey = FL.DimFlightsKey
)

SELECT
    origin,
    destination,
    airline,
    date_trunc('month', booking_date) AS month,
    COUNT(*) AS total_bookings,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_ticket_price
FROM parent_query
GROUP BY origin, destination, airline, month
