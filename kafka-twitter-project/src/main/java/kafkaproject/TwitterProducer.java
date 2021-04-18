/**
 * Alipay.com Inc. Copyright (c) 2004-2020 All Rights Reserved.
 */
package kafkaproject;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author paras.chawla
 * @version $Id: TwitterProducer.java, v 0.1 2020-04-26 22:19 paras.chawla Exp $$
 */

/*
 * Reading real time TWitter feed and pushing into local kafka producer
 *
 * sh kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic twitter_tweets --partitions 6 --replication-factor 1
 * producer has max buffer of 32 MB
 */
public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    String consumerKey    = "40SeNpRwAySvpI7Fh6Kiaeo06";
    String consumerSecret = "Y9Oemf9zzaKpjRCCjLfVz0yAf6DgMq9ANYjvBo8QtRtw87xDtu";
    String token          = "167255962-HHZj75ohKmgMuZX3UXpioJPei4kzmaKYEITei17J";
    String secret         = "rYtNl7L2gevPLFUmQFvlQAVS62aGFKTp3ZUsOCRPrbJsw";

    public static void main(String[] args) throws InterruptedException {
        new TwitterProducer().run();
    }

    public void run() throws InterruptedException {

        /* Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // create Twitter client
        Client twitterClient = createTwitterClient(msgQueue);

        // Attempt to establish a connection
        twitterClient.connect();

        // Create a Kafka Producer
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application");
            twitterClient.stop();
            logger.info("Closing Producer");
            kafkaProducer.close();
            logger.info("Everything is switched off");
        }));

        // get data from Twitter
        // on a different thread, or multiple different threads....
        while (!twitterClient.isDone()) {
            String msg = msgQueue.poll(5, TimeUnit.SECONDS);
            if (msg != null) {
                logger.info(msg);
                kafkaProducer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        logger.info("Received new Metadata" + "\n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Offset :" + recordMetadata.offset() + "\n" +
                                "Partition :" + recordMetadata.partition() + "\n" +
                                "serializedKeySize :" + recordMetadata.serializedKeySize() + "\n" +
                                "serializedValueSize :" + recordMetadata.serializedValueSize() + "\n" +
                                "timestamp :" + recordMetadata.timestamp());
                    }
                });
            }
        }
        logger.info("End of application");
    }

    private KafkaProducer<String, String> createKafkaProducer() {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");

        // high througput producer(at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy"); // compression
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");// add delay of 20 ms so as to collect all messages btw that period
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));// create batch of 16 kb and send at once

        // Create Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {

        /* Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("sonal gupta");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;
    }

}