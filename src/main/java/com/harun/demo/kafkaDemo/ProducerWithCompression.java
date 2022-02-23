package com.harun.demo.kafkaDemo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerWithCompression {

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

        //Add some properties to make producer SAFE!!
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "5");

        //high throughput producer(compression/batching, at the expence of a bit latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); //20 ms wait for messages coming.
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); //32 KB batch size. if batch become full before linger.ms time-limit, it will be sent before the end of linger.ms time.


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
