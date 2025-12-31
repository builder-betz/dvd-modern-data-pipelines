DROP VIEW IF EXISTS mv_clicks_per_min_by_category_and_film;
CREATE MATERIALIZED VIEW mv_clicks_per_min_by_category_and_film
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(window_start)
ORDER BY (window_start, film_id)
AS
SELECT
    tumbleStart(toDateTime(clicks.event_ts), INTERVAL 1 MINUTE) AS window_start,
    clicks.category_name as category,
    clicks.film_id,
    ref.title,
    count(*) AS click_count
FROM dvd_clicks clicks
LEFT JOIN ref_dim_film ref
    ON clicks.film_id = ref.film_id
GROUP BY
    window_start,
    category,
    clicks.film_id,
    ref.title;