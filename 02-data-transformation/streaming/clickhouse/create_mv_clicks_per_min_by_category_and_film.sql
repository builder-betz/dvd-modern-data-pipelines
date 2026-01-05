DROP VIEW IF EXISTS mv_clicks_per_min_by_category_and_film;
CREATE MATERIALIZED VIEW mv_clicks_per_min_by_category_and_film
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(window_start)
ORDER BY (window_start, film_id)
AS
SELECT
    agg.window_start,
    agg.category_name,
    agg.film_id,
    ref.title,
    agg.click_count
FROM
(
    SELECT
        tumbleStart(toDateTime(event_ts), INTERVAL 1 MINUTE) AS window_start,
        ref.category_name,
        dvd_clicks.film_id,
        count() AS click_count
    FROM dvd_clicks
    LEFT JOIN ref_dim_film ref ON dvd_clicks.film_id = ref.film_id
    GROUP BY
        window_start,
        ref.category_name,
        dvd_clicks.film_id
) agg
LEFT JOIN ref_dim_film ref
    ON agg.film_id = ref.film_id;