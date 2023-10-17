CREATE TABLE useractivity (
    id VARCHAR,
    activity_type VARCHAR,
    ts TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'user_activity',
    'properties.bootstrap.servers' = 'redpanda-1:29092',
    'properties.group.id' = 'test-group',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'json'
);