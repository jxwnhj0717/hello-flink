package com.example.kafka;

import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class UserBehaviorKafkaProducer2 {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        while(true) {
            int userId = RandomUtils.nextInt(0, 10);
            int itemId = RandomUtils.nextInt(1000, 10000);
            String behavior = (char)('a' + RandomUtils.nextInt(0, 10)) + "";
            String msg = String.format("%s,%s,%s", userId, itemId, behavior);
            if(RandomUtils.nextInt(0, 10) == 0) {
                msg = null;
            }
            System.out.println(msg);
            ProducerRecord<String, String> record = new ProducerRecord<>("user_behavior2", msg);
            producer.send(record);
            producer.flush();
            Thread.sleep(300);
        }
    }
}
