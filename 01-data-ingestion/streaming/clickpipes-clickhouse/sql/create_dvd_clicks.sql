-- Note: This is automatically generated as part of datapipes
CREATE TABLE default.dvd_clicks
(
    `device` String,
    `event_id` String,
    `event_ts` DateTime64(9),
    `event_type` String,
    `film_id` Int64,
    `page` String,
    `position` Int64,
    `referrer` String,
    `session_id` String,
    `user_id` String,
    `_key` String,
    `_timestamp` DateTime64(3),
    `_partition` Int32,
    `_offset` Int64,
    `_topic` String,
    `_header_keys` Array(String),
    `_header_values` Array(String)
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY event_ts
SETTINGS index_granularity = 8192