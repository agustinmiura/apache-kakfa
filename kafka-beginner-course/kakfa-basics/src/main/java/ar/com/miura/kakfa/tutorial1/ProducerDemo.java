package ar.com.miura.kakfa.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.stream.IntStream;

import static ar.com.miura.Utils.readPropertiesFile;

public class ProducerDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemo.class.getName());

    public void testProducer() throws IOException {
        final KafkaProducer<String, String> producer;
        Properties properties = new Properties();
        Properties fromConfig = readPropertiesFile("application.properties", this.getClass());;
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, fromConfig.getProperty("server.url"));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer= new KafkaProducer<String, String>(properties);
        try {
            IntStream stream = IntStream.iterate(0, i -> i+1).limit(10);
            stream.forEach(i -> {
                ProducerRecord<String, String> record  = new ProducerRecord<String, String>(fromConfig.getProperty("topic.name"), "Hello world" + i);
                producer.send(record, (recordMetadata, e) -> {
                    if (e==null) {
                        LOGGER.info(" Receive metadata , Topic : {} , Partition : {} , Offsets : {} , Timestamp : {} ", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                    } else {
                        LOGGER.error(" Error sending the message ", e);
                    }
                });
            });
        }catch(Exception e) {
            LOGGER.error(" Exception ", e);
        }finally{
            if (producer!=null) {
                producer.flush();
                producer.close();
            }
        }
    }
}
