package ar.com.miura.kakfa.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemo.class.getName());

    public void testProducer() {
        KafkaProducer<String, String> producer = null;
        try {
            Properties fromConfig = readPropertiesFile("application.properties");

            //Create producer properties
            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, fromConfig.getProperty("server.url"));
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            //Create the producer
            producer= new KafkaProducer<String, String>(properties);

            //Create a producer record
            ProducerRecord<String, String> record  = new ProducerRecord<String, String>(fromConfig.getProperty("topic.name"), "Hello world");
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
