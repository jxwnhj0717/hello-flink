CREATE TABLE kafka_table2 (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  `ts` TIMESTAMP(3) METADATA FROM 'timestamp',
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'user_behavior2',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup2',
  'properties.auto.offset.reset' = 'earliest',
  'key.format' = 'csv',
  'value.format' = 'csv'
)