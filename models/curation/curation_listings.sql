WITH listings_raw AS (
    SELECT 
        id AS listing_id,
        listing_url,
        name,
        description,
        description IS NOT NULL AS has_description, -- Ajout du 'AS' manquant pour la clarté
        neighbourhood_overview,
        neighbourhood_overview IS NOT NULL AS has_neighrbourhood_description,
        host_id,
        latitude,
        longitude,
        property_type,
        room_type,
        accommodates,
        bathrooms,
        bedrooms,
        beds,
        amenities,
        -- Attention ici : si le prix est "$150.00", split_part(price, '$', 2) est souvent nécessaire
        TRY_CAST(REPLACE(REPLACE(price, '$', ''), ',', '') AS FLOAT) AS price,
        minimum_nights,
        maximum_nights
    FROM {{ source('raw_airbnb_data', 'listings') }} -- Un seul FROM ici !
)

SELECT *
FROM listings_raw
-- Retrait de la parenthèse fermante en trop et du point-virgule si présent