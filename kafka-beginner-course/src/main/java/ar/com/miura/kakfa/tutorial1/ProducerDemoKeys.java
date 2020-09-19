package ar.com.miura.kakfa.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.stream.IntStream;

public class ProducerDemoKeys {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemo.class.getName());

    public static void main(String[] args) throws IOException {
        ProducerDemoKeys demoKeys = new ProducerDemoKeys();
        demoKeys.testProducer();
    }

    public void testProducer() throws IOException {
        final KafkaProducer<String, String> producer;
        Properties properties = new Properties();
        Properties fromConfig = readPropertiesFile("application.properties");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, fromConfig.getProperty("server.url"));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer= new KafkaProducer<String, String>(properties);
        try {
            int index = 0;
            IntStream stream = IntStream.iterate(0, i -> i+1).limit(10);
            stream.forEach(i -> {

                String topic = fromConfig.getProperty("topic.name");
                String value = "hello_world" + i;
                String key = "id_ " + Integer.toString(i);

                LOGGER.info("Key:" + key);

                ProducerRecord<String, String> record  = new ProducerRecord<String, String>(fromConfig.getProperty("topic.name"), key, "Hello world" + i);
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

    public Properties readPropertiesFile(String fileName) throws IOException {
        Properties prop = null;
        try(InputStream is = getClass().getClassLoader().getResourceAsStream(fileName)) {
            prop = new Properties();
            prop.load(is);
        } catch(FileNotFoundException fnfe) {
            LOGGER.error(" Error ", fnfe);
        } catch(IOException ioe) {
            LOGGER.error(" IOException ", ioe);
        }
        return prop;
    }

}
