package com.harun.demo.kafkaDemo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {


    public static void main(String[] args) throws ExecutionException, InterruptedException {
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
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //Create a producer record
        for(int i=0; i<10; i++) {
            String topic = "first_topic";
            String value = "value " + i;
            String key = "key " + i;

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            logger.info("Key: " + key);

            //Send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata \n" +
                                "topic: " + recordMetadata.topic() +
                                "\n offset: " + recordMetadata.offset() +
                                "\n partition: " + recordMetadata.partition() +
                                "\n timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Receiving the metadata is not successful:" + e);
                    }
                }
            }).get(); //senktron şekilde çalışması için get() koyduk. hangi key hangi pratition'a gidiyor görebilmek için. Production'da deneme!!!
        }
        //flash data and close producer
        producer.flush();
        producer.close();
    }
}
