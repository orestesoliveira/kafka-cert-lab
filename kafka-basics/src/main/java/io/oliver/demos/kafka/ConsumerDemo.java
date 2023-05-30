package io.oliver.demos.kafka;


import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log =  LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        log.info("I am a kafka Consumer");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='2kNM4QENVpqVZ5NHDHyGkD' password='eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIya05NNFFFTlZwcVZaNU5IREh5R2tEIiwib3JnYW5pemF0aW9uSWQiOjczNDU2LCJ1c2VySWQiOjg1NDE5LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJhN2Y4YmQ0NS1kMmM1LTQ2MDQtOGZkMS00MWRlOTY0MDg4MGYifX0.9Y1QKQnx3hsaJ3YtmRILjMllpN7UTJlSMW2W8QYID_w';");


        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

    }
}
