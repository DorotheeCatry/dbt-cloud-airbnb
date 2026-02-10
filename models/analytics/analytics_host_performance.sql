{{
    config(
        materialized='view',
        schema='analytics'
    )
}}

-- Analyse de performance des hôtes
-- Table matérialisée pour des accès rapides aux dashboards

WITH host_stats AS (
    SELECT
        h.host_id,
        h.host_name,
        h.host_city,
        h.host_country,
        h.is_superhost,
        h.response_rate,

        -- Statistiques des listings
        COUNT(l.listing_id) AS nb_listings,
        AVG(l.price) AS prix_moyen,
        MIN(l.price) AS prix_min,
        MAX(l.price) AS prix_max,

        -- Capacité totale
        SUM(l.accommodates) AS capacite_totale,
        AVG(l.accommodates) AS capacite_moyenne,

        -- Diversité des types de propriétés
        COUNT(DISTINCT l.property_type) AS nb_types_proprietes,
        COUNT(DISTINCT l.room_type) AS nb_types_chambres

    FROM airbnb.curation.curation_hosts h
    INNER JOIN airbnb.curation.curation_listings l
        ON h.host_id = l.host_id

    WHERE l.price IS NOT NULL
      AND l.price > 0

    GROUP BY 1, 2, 3, 4, 5, 6
),

performance_categories AS (
    SELECT
        *,
        -- Catégorisation des hôtes
        CASE
            WHEN nb_listings >= 10 THEN 'Professionnel'
            WHEN nb_listings >= 3 THEN 'Multi-propriétaire'
            ELSE 'Particulier'
        END AS categorie_hote,

        -- Segmentation prix
        CASE
            WHEN prix_moyen <= 75 THEN 'Budget'
            WHEN prix_moyen <= 150 THEN 'Standard'
            WHEN prix_moyen <= 300 THEN 'Premium'
            ELSE 'Luxe'
        END AS segment_prix

    FROM host_stats
)

SELECT * FROM performance_categories
ORDER BY nb_listings DESC, prix_moyen DESC