-- Modèle de curation pour les données des reviews Airbnb
-- Nettoie et standardise les données raw des reviews

WITH curation_raw AS
( SELECT listing_id, date AS review_date FROM airbnb.raw.reviews)

SELECT listing_id,  review_date , count(*) AS nb_reviews FROM curation_raw
GROUP BY ALL