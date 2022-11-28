create table myuser (
    id int,
    name string,
    audit boolean,
    primary key(id) NOT ENFORCED
) with (
    'connector'='iceberg',
    'catalog-name'='hadoop_test',
    'catalog-type'='hadoop',
    'warehouse'='hdfs://localhost:9000/user/iceberg'
)