-- Modèle de curation pour les données des hôtes Airbnb
-- Nettoie et standardise les données raw des hôtes

WITH hosts_raw AS (
    SELECT
        host_id,
        -- Gestion des noms anonymes (1 caractère = anonyme)
        CASE
            WHEN LEN(host_name) = 1 THEN 'Anonyme'
            ELSE host_name
        END AS host_name,

        host_since,
        host_location,

        -- Extraction de la ville (première partie avant la virgule)
        SPLIT_PART(host_location, ',', 1) AS host_city,

        -- Extraction du pays (deuxième partie après la virgule)
        SPLIT_PART(host_location, ',', 2) AS host_country,

        -- Conversion du taux de réponse en entier
        TRY_CAST(REPLACE(host_response_rate, '%', '') AS INTEGER) AS response_rate,

        -- Conversion en booléen pour superhost
        host_is_superhost = 't' AS is_superhost,

        host_neighbourhood,

        -- Conversion en booléen pour identité vérifiée
        host_identity_verified = 't' AS is_identity_verified

    FROM airbnb.raw.hosts
)

SELECT * FROM hosts_raw