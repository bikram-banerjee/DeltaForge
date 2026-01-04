{{ config(materialized='table') }}

WITH parent_query AS (
    SELECT
        *
    FROM workspace.gold.FactBookings
)

SELECT
    *
FROM parent_query
WHERE
    amount <= 0
    OR DimPassengersKey IS NULL
    OR DimFlightsKey IS NULL
    OR DimAirportsKey IS NULL
