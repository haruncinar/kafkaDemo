package com.harun.demo.kafkaDemo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        //on the console side:
/*        Zookeeper start:
        zookeeper-server-start.sh config/zookeeper.properties

        Kafka start:
        kafka-server-start.sh config/server.properties

        Consumer start:
        kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic first_topic --group my-demo-application
        */

        String bootstrapServers = "127.0.0.1:9092";
        //Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "it's harun");

        //Send data
        producer.send(record);

        //flash data and close producer
        producer.flush();
        producer.close();
    }
}
