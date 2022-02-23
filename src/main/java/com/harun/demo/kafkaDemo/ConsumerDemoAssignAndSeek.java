package com.harun.demo.kafkaDemo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String offsetResetConfig = "earliest";
        String topic = "first_topic";


        //Create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetConfig);

        //Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //assign and seek mostly used to see specific data or message
        //assign
        TopicPartition partition = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(List.of(partition));

        //seek
        consumer.seek(partition, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        int numberOfMessagesThatReadSoFar = 0;
        boolean keepReading = true;

        //poll for new data
        while(keepReading)
        {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record : records)
            {
                numberOfMessagesThatReadSoFar ++;
                logger.info("key: " + record.key() + "  value: " + record.value());
                logger.info("Partition: " + record.partition() + "  Offset: " + record.offset());
                if(numberOfMessagesThatReadSoFar >= numberOfMessagesToRead)
                {
                    keepReading = false;
                    break;
                }
            }
        }
        logger.info("Exiting the program...");
    }
}
