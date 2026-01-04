{{ config(materialized='table') }}

WITH parent_query AS (
    SELECT
        F.booking_date,
        F.amount,

        -- Passenger
        P.passenger_id,
        P.name       AS passenger_name,
        P.gender,
        P.nationality,

        -- Flight
        FL.flight_id,
        FL.airline,
        FL.origin,
        FL.destination,
        FL.flight_date,

        -- Airport
        A.airport_id,
        A.airport_name,
        A.city,
        A.country
    FROM workspace.gold.FactBookings F
    LEFT JOIN workspace.gold.DimPassengers P
        ON F.DimPassengersKey = P.DimPassengersKey
    LEFT JOIN workspace.gold.DimFlights FL
        ON F.DimFlightsKey = FL.DimFlightsKey
    LEFT JOIN workspace.gold.DimAirports A
        ON F.DimAirportsKey = A.DimAirportsKey
)

SELECT *
FROM parent_query
