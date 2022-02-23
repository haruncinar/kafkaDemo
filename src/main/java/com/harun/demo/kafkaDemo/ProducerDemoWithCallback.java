package com.harun.demo.kafkaDemo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;

public class ProducerDemoWithCallback {

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
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        //Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Create a producer record
        for(int i=0; i<25; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("twitter_topic", "it's harun " + i +" "+new Date());

            //Send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata \n" +
                                record.value() + "\n" +
                                "topic: " + recordMetadata.topic() +
                                "\n offset: " + recordMetadata.offset() +
                                "\n partition: " + recordMetadata.partition() +
                                "\n timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Receiving the metadata is not successful:" + e);
                    }
                }
            });
        }
        //flash data and close producer
        producer.flush();
        producer.close();
    }
}
