CREATE TABLE mysql_table (
    id int,
    name string,
    age int,
    update_time TIMESTAMP(3),
    PRIMARY KEY(id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'localhost',
    'port' = '3306',
    'username' = 'root',
    'password' = 'root123456',
    'database-name' = 'user',
    'table-name' = 'user'
)