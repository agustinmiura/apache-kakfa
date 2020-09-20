package ar.com.miura.kakfa.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static ar.com.miura.Utils.readPropertiesFile;

public class ConsumerDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemo.class.getName());

    public static void main(String [] args) {

        try {
            ConsumerDemo demo = new ConsumerDemo();
            demo.testConsumer();
        }catch(Exception e) {
            LOGGER.error(" Error with the demo ", e);
        }

    }

    public void testConsumer() throws IOException {
        final KafkaProducer<String, String> producer;
        Properties properties = new Properties();
        Properties fromConfig = readPropertiesFile("application.properties", this.getClass());

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, fromConfig.getProperty("server.url"));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        /**
         * consumer.offset.reset.early=earliest
         * consumer.offset.reset.latest=latest
         * consumer.offset.reset.none=none
         */
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, fromConfig.getProperty("consumer.group.id"));
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, fromConfig.getProperty("consumer.offset.reset.early"));

        //Create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Suscribe consumer to our topic
        consumer.subscribe(Collections.singleton(fromConfig.getProperty("topic.name")));

        //poll for new data
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.iterator().forEachRemaining(eachRecord -> {
                LOGGER.info(" Key : {} , Value : {} , Partition : {} ", eachRecord.key(), eachRecord.value(), eachRecord.partition());
            });
        }
    }
}
