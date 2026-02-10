{{
    config(
        materialized='view',
        schema='curation'
    )
}}

-- Modèle des hôtes avec gestion de l'historique
WITH hosts_current AS (
    SELECT
        host_id,
        CASE WHEN LEN(host_name) = 1 THEN 'Anonyme' ELSE host_name END AS host_name,
        host_since,
        host_location,
        SPLIT_PART(host_location, ',', 1) AS host_city,
        SPLIT_PART(host_location, ',', 2) AS host_country,
        TRY_CAST(REPLACE(host_response_rate, '%', '') AS INTEGER) AS response_rate,
        host_is_superhost = 't' AS is_superhost,
        host_neighbourhood,
        host_identity_verified = 't' AS is_identity_verified,
        dbt_valid_from,
        dbt_valid_to,
        dbt_updated_at
    FROM {{ ref('hosts_snapshot') }}

    -- Filtrer pour ne garder que les enregistrements actuels
    WHERE dbt_valid_to IS NULL
      AND host_is_superhost IS NOT NULL
      AND host_neighbourhood IS NOT NULL
)

SELECT * FROM hosts_current