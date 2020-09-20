package ar.com.miura.kakfa.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static ar.com.miura.Utils.readPropertiesFile;

public class ConsumerDemoWithKeys {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemoWithKeys.class.getName());

    public static void main(String [] args) {
        try {
            ConsumerDemoWithKeys demo = new ConsumerDemoWithKeys();
            demo.testConsumer();
        }catch(Exception e) {
            LOGGER.error(" Error with the demo ", e);
        }

    }

    public void testConsumer() throws IOException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            final KafkaProducer<String, String> producer;
            Properties properties = new Properties();
            Properties fromConfig = readPropertiesFile("application.properties", this.getClass());

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, fromConfig.getProperty("server.url"));
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, fromConfig.getProperty("consumer.group.id.new"));
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, fromConfig.getProperty("consumer.offset.reset.early"));

            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Collections.singleton(fromConfig.getProperty("topic.name")));


            ConsumerRunnable consumerRunnable = new ConsumerRunnable(countDownLatch, consumer);
            Thread thread = new Thread(consumerRunnable);
            thread.start();

            Runtime.getRuntime().addShutdownHook(new Thread( () -> {
                LOGGER.info(" Caught shutdown hook ");
                consumerRunnable.shutdown();
                try {
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    LOGGER.error(" Error with await method ", e);
                }
            }));

            countDownLatch.await();
        } catch(InterruptedException e) {
            LOGGER.error(" Error app interrupted ", e);
        } finally {
          LOGGER.info(" App finished");
        }

    }
}