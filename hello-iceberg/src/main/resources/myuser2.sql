create table myuser2 (
    id int,
    name string,
    audit boolean,
    update_time timestamp(3),
    primary key(id) NOT ENFORCED
) with (
    'connector'='iceberg',
    'catalog-name'='hadoop_test',
    'catalog-type'='hadoop',
    'warehouse'='hdfs://localhost:9000/user/iceberg',
    'format-version'='2',
    'write.distribution-mode'='hash',
    'write.metadata.delete-after-commit.enabled'='true',
    'write.metadata.previous-versions-max'='10',
    'write.upsert.enable'='true'
)