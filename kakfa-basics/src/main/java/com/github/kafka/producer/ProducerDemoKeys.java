/**
 * Alipay.com Inc. Copyright (c) 2004-2020 All Rights Reserved.
 */
package com.github.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author paras.chawla
 * @version $Id: ProducerDemo.java, v 0.1 2020-04-24 18:31 paras.chawla Exp $$
 */
public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        //Create a logger
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        // create Producer Properties

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Producer publishing messages in partitions in Round-Robin manner but ConsumerDemo group listening in any
        // arbitary manner becauses we didn't use any particular key
        for (int i = 0; i < 10; i++) {

            //Same key always going to same partition
            String key = "id_" + Integer.toString(i);
            logger.info("Key: " + key +"\n");
            // Create Kafka ProducerRecord
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>("topic2", key,
                    "Hello World" + Integer.toString(i));

            // Send Data - Async
            // onCompletion is called always once message is successfully sent or exception occurs
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    logger.info("Received new Metadata" + "\n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Offset :" + recordMetadata.offset() + "\n" +
                            "Partition :" + recordMetadata.partition() + "\n" +
                            "serializedKeySize :" + recordMetadata.serializedKeySize() + "\n" +
                            "serializedValueSize :" + recordMetadata.serializedValueSize() + "\n" +
                            "timestamp :" + recordMetadata.timestamp());
                }
            }).get(); // Block send to make it synchronous
        }

        // flush and close producer
        producer.flush();
        producer.close();
    }
}