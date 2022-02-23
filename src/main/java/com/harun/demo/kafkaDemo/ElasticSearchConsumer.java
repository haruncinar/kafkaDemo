package com.harun.demo.kafkaDemo;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class ElasticSearchConsumer {

    public static RestHighLevelClient createClient() {
        String hostname = "kafka-course-2345133401.eu-central-1.bonsaisearch.net";
        String user = "gcwliifbqr";
        String pass = "e01r46bwv8";


        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(user, pass));

        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);
        return client;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic)
    {
        String bootstrapServers = "127.0.0.1:9092";
        String offsetResetConfig = "earliest";
        String groupId = "my-twitter-application";


        //Create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetConfig);

        //auto-commit yerine manuel-commit yapacağız. Bu yğzden ilk olarak auto-commit property'sini false yapıyoruz.
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");


        //Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton(topic));
        return consumer;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();


        KafkaConsumer<String, String> kafkaConsumer = createConsumer("twitter_topic");
        logger.info("Consumer created!");

        while(true)
        {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

            //to send request bulkly we create BulkRequest object
            BulkRequest bulkRequest = new BulkRequest();

            for(ConsumerRecord<String, String> record : records)
            {
                //to make the message idempodent (to pretend it from dublication)
                //2 way
                //1- to use a unique id from message inside!!
                //2- to create a unique id from topic-partition-offset
                String id = record.topic() + "_" + record.partition()  + "_" + record.offset();

                String jsonString = record.value();
                //where we insert data to elastic search
                XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                        .startObject()
                        .field("tweet", jsonString)
                        .endObject();
                IndexRequest indexRequest = new IndexRequest("twitter");
                indexRequest.id(id);
                indexRequest.source(xContentBuilder);

                bulkRequest.add(indexRequest); //adding indexReq to our bulkReq

                logger.info("Response has taken, The ID : " + id);
            }
            if(records.count() > 0)
            {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Recieved records amount: " + records.count());
                logger.info("Committing offsets..");
                kafkaConsumer.commitSync();
                logger.info("Offsets have been committed..");
                Thread.sleep(2000);
            }
        }
        //to get element with id in elasticSearch : /twitter/_doc/XsMJQ34BeF8P419eJwxQ
    }
}