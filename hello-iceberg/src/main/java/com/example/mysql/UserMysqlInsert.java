package com.example.mysql;

import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.UUID;


public class UserMysqlInsert {
    // mysql 5.7.26
//    create table user(
//      id int,
//      name varchar(50),
//      age int,
//      update_time timestamp,
//      primary key(id)
//    );
    public static void main(String[] args) throws Exception {
        String myDriver = "com.mysql.cj.jdbc.Driver";
        String myUrl = "jdbc:mysql://localhost/user";
        Class.forName(myDriver);
        Connection conn = DriverManager.getConnection(myUrl, "root", "root123456");
        Statement st = conn.createStatement();
        while(true) {
            int userId = RandomUtils.nextInt(0, 10);
            String name = UUID.randomUUID().toString().substring(0, 4);
            int age = RandomUtils.nextInt(10, 50);
            String sql = String.format("insert into user values(%d, '%s', %d, now())", userId, name, age);
            try {
                st.executeUpdate(sql);
            } catch (SQLIntegrityConstraintViolationException e) {
                sql = String.format("update user set name='%s', age=%d, update_time=now() where id=%d", name, age, userId);
                st.executeUpdate(sql);
            }
            System.out.println(sql);
            Thread.sleep(300);
        }
    }
}
