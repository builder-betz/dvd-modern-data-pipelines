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


### View ClickHouse Objects

The materialized view and related tables can be viewed in the ClickHouse Cloud interface:

<div align="center">

![ClickHouse Objects](/02-data-transformation/streaming/clickhouse/images/streaming-clickhouse-objects.png)

</div>



---
🔗 **Page Navigation**:  [Main](../../README.md) 
| [Batch](../../00-data-pipelines/batch/README.md) 
| [Streaming](../../00-data-pipelines/streaming/README.md) 
| [Prev](../../01-data-ingestion/streaming/README.md) 
| [Next](../../04-data-consumption/streaming/README.md)

🔗 **Streaming Pipeline Navigation**: 
[Data Source](../../00-data-source/streaming/README.md)
| [Data Ingestion](../../01-data-ingestion/streaming/README.md)
| Data Transformation
| [Data Consumption](../../04-data-consumption/streaming/README.md) 