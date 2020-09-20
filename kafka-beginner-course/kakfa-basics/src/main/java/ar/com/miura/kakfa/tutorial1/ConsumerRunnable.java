package ar.com.miura.kakfa.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

public class ConsumerRunnable implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;

    public ConsumerRunnable(CountDownLatch latch, KafkaConsumer<String, String> consumer) {
        this.latch = latch;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        //poll for new data
        try {
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.iterator().forEachRemaining(eachRecord -> {
                    LOGGER.info(" Key : {} , Value : {} , Partition : {} ", eachRecord.key(), eachRecord.value(), eachRecord.partition());
                });
            }
        } catch(WakeupException exception) {
            LOGGER.error(" Received shutdown signal ", exception);
        } finally{
            consumer.close();
            latch.countDown();
        }
    }

    public void shutdown() {
        //Stop polling
        consumer.wakeup();
    }
}
