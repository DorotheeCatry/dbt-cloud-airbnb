{{
    config(
        materialized='table',
        schema='curation'
    )
}}

-- Transformation des données de tourisme à Amsterdam
WITH tourists_raw AS (
    SELECT
        year,
        tourists
    FROM {{ ref('tourists_per_year') }}
),

tourists_formatted AS (
    SELECT
        -- Conversion en date de fin d'année
        DATE(year || '-12-31') AS year_end_date,
        year,
        tourists,

        -- Calcul des variations annuelles
        LAG(tourists) OVER (ORDER BY year) AS tourists_previous_year,
        tourists - LAG(tourists) OVER (ORDER BY year) AS tourists_change,

        -- Pourcentage de variation
        ROUND(
            (tourists - LAG(tourists) OVER (ORDER BY year)) * 100.0
            / LAG(tourists) OVER (ORDER BY year), 2
        ) AS tourists_change_pct,

        -- Catégorisation
        CASE
            WHEN year <= 2019 THEN 'Pre-COVID'
            WHEN year IN (2020, 2021) THEN 'COVID'
            ELSE 'Post-COVID'
        END AS period_category

    FROM tourists_raw
)

SELECT * FROM tourists_formatted
ORDER BY year