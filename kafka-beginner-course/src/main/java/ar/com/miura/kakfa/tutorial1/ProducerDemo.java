package ar.com.miura.kakfa.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemo.class.getName());

    public static void main(String[] args) {
        ProducerDemo pDemo = new ProducerDemo();
        pDemo.testProducer();
    }

    public void testProducer() {
        KafkaProducer<String, String> producer = null;
        try {
            //Create producer properties
            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            //Create the producer
            producer= new KafkaProducer<String, String>(properties);

            //Create a producer record
            ProducerRecord<String, String> record  = new ProducerRecord<String, String>("first_topic", "Hello world");
            //Send data asynchronous
            producer.send(record, (recordMetadata, e) -> {
                if (e==null) {
                    LOGGER.info(" Message sent ");
                } else {
                    LOGGER.error(" Error sending the message ", e);
                }
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
