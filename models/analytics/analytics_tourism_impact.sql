{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

-- Analyse de l'impact du tourisme sur l'offre Airbnb
WITH tourism_data AS (
    SELECT
        year,
        tourists,
        tourists_change_pct,
        period_category
    FROM {{ ref('curation_tourists') }}
),

reviews_by_year AS (
    SELECT
        EXTRACT(YEAR FROM date) AS year,
        COUNT(*) AS nb_reviews,
        COUNT(DISTINCT listing_id) AS listings_reviewed
    FROM {{ source('raw_airbnb_data', 'reviews') }}
    WHERE date >= '2015-01-01'
    GROUP BY 1
),

hosts_stats AS (
    SELECT
        COUNT(*) AS total_hosts,
        COUNT(CASE WHEN is_superhost THEN 1 END) AS superhosts,
        AVG(response_rate) AS avg_response_rate
    FROM {{ ref('curation_hosts') }}
    WHERE response_rate IS NOT NULL
),

listings_stats AS (
    SELECT
        COUNT(*) AS total_listings,
        AVG(price) AS avg_price,
        COUNT(DISTINCT property_type) AS property_types,
        COUNT(DISTINCT room_type) AS room_types
    FROM {{ ref('curation_listings') }}
    WHERE price IS NOT NULL AND price > 0
),
combined_analysis AS (
    SELECT
        t.year,
        t.tourists,
        t.tourists_change_pct,
        t.period_category,
        r.nb_reviews,
        r.listings_reviewed,

        -- Ratio reviews par touriste (proxy d'activité)
        ROUND(r.nb_reviews * 1000.0 / t.tourists, 2) AS reviews_per_1000_tourists,

        -- Stats globales (répétées pour jointure)
        h.total_hosts,
        h.superhosts,
        h.avg_response_rate,
        l.total_listings,
        l.avg_price,
        l.property_types,
        l.room_types

    FROM tourism_data t
    LEFT JOIN reviews_by_year r ON t.year = r.year
    CROSS JOIN hosts_stats h
    CROSS JOIN listings_stats l
)

SELECT * FROM combined_analysis
ORDER BY year