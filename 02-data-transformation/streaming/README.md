# Streaming Data Transformation

ClickHouse materialized views are used to create real-time aggregations and transformations on the streaming `dvd_clicks` data. These views automatically update as new events arrive, enabling real-time analytics.


## Materialized Views

### mv_clicks_per_min_by_category_and_film

This materialized view aggregates click events by:
- 1-minute time windows (tumbling windows)
- Film category
- Film ID and title

**Purpose**: Provides real-time trending insights showing which films and categories are receiving the most clicks per minute.

**Engine**: `SummingMergeTree` - automatically merges and sums duplicate keys

**Partitioning**: By year-month for efficient querying



## SQL Scripts

### Create Materialized View

Execute the SQL script to create the materialized view:
- `create_mv_clicks_per_min_by_category_and_film.sql`

```sql
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
```



## Usage

### Query the Materialized View

```sql
-- Get trending films in the last hour
SELECT 
    window_start,
    category,
    title,
    click_count
FROM mv_clicks_per_min_by_category_and_film
WHERE window_start >= now() - INTERVAL 1 HOUR
ORDER BY click_count DESC
LIMIT 20;

-- Get clicks by category for the last 10 minutes
SELECT 
    category,
    SUM(click_count) as total_clicks
FROM mv_clicks_per_min_by_category_and_film
WHERE window_start >= now() - INTERVAL 10 MINUTE
GROUP BY category
ORDER BY total_clicks DESC;
```

### View ClickHouse Objects

The materialized view and related tables can be viewed in the ClickHouse Cloud interface:

<div align="center">

![ClickHouse Objects](/02-data-transformation/streaming/clickhouse/images/streaming-clickhouse-objects.png)

</div>



---
🔗 **Page Navigation**:  [Main](../../README.md) | [Batch](../../batch/00-data-pipelines/batch/README.md) | [Streaming](../../streaming/00-data-pipelines/batch/README.md) | [Prev](../../01-data-ingestion/streaming/README.md) | [Next](../../04-data-consumption/streaming/README.md)

