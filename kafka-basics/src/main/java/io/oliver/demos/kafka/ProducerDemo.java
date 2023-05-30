package io.oliver.demos.kafka;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log =  LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
    log.info("I am a kafka producer");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.mechanism","PLAIN");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='2kNM4QENVpqVZ5NHDHyGkD' password='eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIya05NNFFFTlZwcVZaNU5IREh5R2tEIiwib3JnYW5pemF0aW9uSWQiOjczNDU2LCJ1c2VySWQiOjg1NDE5LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJhN2Y4YmQ0NS1kMmM1LTQ2MDQtOGZkMS00MWRlOTY0MDg4MGYifX0.9Y1QKQnx3hsaJ3YtmRILjMllpN7UTJlSMW2W8QYID_w';");


        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        properties.setProperty("batch.size", String.valueOf(400));


        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        log.info("default Sticky partitioner");
        for (int i =0;i<10;i++) {
            //nesse exemplo pegamos sempre a mesma partition por causa
            //do sticky partitioner
            //kafka faz o envio por batches  mais performatico do que seria usando round robin
            //isso esta acontecendo pq nao estamos usando um partitioner definido
            //acabamos por usar o default
            //partitioner.class = class org.apache.kafka.clients.producer.internals.DefaultPartitioner
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello "+i);

            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        log.info("Received new Metadata \n" +
                                " ---" + metadata.partition());
                    } else {
                        log.error("exception happened");
                    }
                }
            });
        }


        for (int j =0;j<10;j++) {
            for (int i =0;i<30;i++) {
                //
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello "+i);

                producer.send(producerRecord, (RecordMetadata metadata, Exception exception) -> {
                        if (exception == null) {
                            log.info("Received new Metadata \n" +
                                    " ---" + metadata.topic().toString() +
                                    " ---" + metadata.partition());
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
