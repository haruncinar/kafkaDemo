package com.harun.demo.kafkaDemo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoGroups {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-new-application";
        String offsetResetConfig = "earliest";
        String topic = "first_topic";

        //Bu main 2.defa çalıştırılıp, console'da re-joining group çıktıları görülebilir. 2 farklı consumer'ın console'larında partition'ların nasıl dağıtıldığı görülebilir....
        //Consumer sayıları bu şekilde arttırılıp azaltılarak producer'lardan herhangi biri çalıştırıldığında, pratition dağılımları consumer console'larında rahatlıkla gözlemlenebilir...

        //Create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetConfig);

        //Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Subscribe consumer to our topics
        consumer.subscribe(Collections.singleton(topic));

        //poll for new data
        while(true)
        {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record : records)
            {
                logger.info("key: " + record.key() + "  value: " + record.value());
                logger.info("Partition: " + record.partition() + "  Offset: " + record.offset());
            }
        }
    }
}
