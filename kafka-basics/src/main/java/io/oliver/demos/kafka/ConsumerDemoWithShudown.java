package io.oliver.demos.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShudown {

    private static final Logger log =  LoggerFactory.getLogger(ConsumerDemoWithShudown.class);

    public static void main(String[] args) {
        log.info("I am a kafka Consumer");

        String groupId = "my-java-application";

        String topic = "demo_java";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='2kNM4QENVpqVZ5NHDHyGkD' password='eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIya05NNFFFTlZwcVZaNU5IREh5R2tEIiwib3JnYW5pemF0aW9uSWQiOjczNDU2LCJ1c2VySWQiOjg1NDE5LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJhN2Y4YmQ0NS1kMmM1LTQ2MDQtOGZkMS00MWRlOTY0MDg4MGYifX0.9Y1QKQnx3hsaJ3YtmRILjMllpN7UTJlSMW2W8QYID_w';");


        //consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);

        //properties.setProperty("auto.offset.reset", "none/earliest/latest");
        //none quer dizer que se nao tivermos um consumer group we fail
        //earliest is the same as --beginning option from kafka CLI
        properties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);


        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("detected shutdown call consumer wakeup");
                consumer.wakeup();

                //join the main thread to allow the execution of the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

        });


        try {
            //pass the topics
            consumer.subscribe(Arrays.asList(topic, "topic 2"));

            //consumers poll data from Kafka
            while (true) {
                log.info("Polling");

                //if there is data it will start fetching automatically
                //if there is no data it will w8 for 1000 miliseconds for maybe ssome data streaming
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));


                for (ConsumerRecord<String, String> record : records) {
                    log.info("key :" + record.key() + " Value :" + record.value() + " Partition :" + record.partition() + ".." + record.offset());
                }

            }
        } catch (WakeupException e) {
            log.error(e.getMessage());
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            consumer.close();
        }
    }
}
