CREATE TABLE useractivity_enriched WITH (
    'connector' = 'kafka',
    'topic' = 'user_activity_enriched',
    'properties.bootstrap.servers' = 'redpanda-1:29092',
    'format' = 'json'
) AS
SELECT id,
       Upper(activity_type),
       ts
FROM  useractivity;