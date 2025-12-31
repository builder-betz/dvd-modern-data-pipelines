-- Uses a pre-signed URL to load one-off extracted file from dim_film
-- df = spark.table("analytics.gold.dim_film")
-- 
-- df.write \
--   .mode("overwrite") \
--   .option("header", "true") \
--   .csv("s3://de-bootcamp-capstone-project/dvd-ref-data/gold/dim_film/")

CREATE OR REPLACE TABLE ref_dim_film
(
    `film_key` String,
    `film_id` Int64,
    `title` String,
    `release_year` Int16,
    `length` Int16,
    `rating` String,
    `rental_duration` Int16,
    `category_name` String
)
ENGINE = MergeTree
ORDER BY film_id;

INSERT INTO ref_dim_film
SELECT *
FROM url(
  'https://de-bootcamp-capstone-project.s3.ap-southeast-2.amazonaws.com/dvd-ref-data/gold/dim_film/dim_film.csv?<continuation_pre-signed_url>',
  'CSVWithNames'
);