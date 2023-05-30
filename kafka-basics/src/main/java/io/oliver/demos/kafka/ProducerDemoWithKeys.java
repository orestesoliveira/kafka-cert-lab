package io.oliver.demos.kafka;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {

    private static final Logger log =  LoggerFactory.getLogger(ProducerDemoWithKeys.class);

    public static void main(String[] args) {
        log.info("I am a kafka producer");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='2kNM4QENVpqVZ5NHDHyGkD' password='eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIya05NNFFFTlZwcVZaNU5IREh5R2tEIiwib3JnYW5pemF0aW9uSWQiOjczNDU2LCJ1c2VySWQiOjg1NDE5LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJhN2Y4YmQ0NS1kMmM1LTQ2MDQtOGZkMS00MWRlOTY0MDg4MGYifX0.9Y1QKQnx3hsaJ3YtmRILjMllpN7UTJlSMW2W8QYID_w';");


        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());


        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        log.info("example with keys");

        for (int j = 0; j < 30; j++) {
            for (int i = 0; i < 30; i++) {

                String demoJava = "demo_java";
                String key = "id_" + i;
                String value = "Hello :" + i;

                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(demoJava, key, value);

                producer.send(producerRecord, (RecordMetadata metadata, Exception exception) -> {
                    if (exception == null) {
                        log.info("key: " + key +
                                " --offset: " + metadata.offset() +
                                " --partititon :" + metadata.partition());
                    } else {
                        log.error("exception happened");
                    }

                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }


        producer.close();
    }

}
