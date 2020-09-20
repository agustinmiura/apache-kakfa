package ar.com.miura.kakfa.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SeekAssignRunnable implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;

    public SeekAssignRunnable(CountDownLatch latch, KafkaConsumer<String, String> consumer) {
        this.latch = latch;
        this.consumer = consumer;
    }

    @Override
    public void run() {

        AtomicBoolean keepReading = new AtomicBoolean(true);
        AtomicLong messagesRead = new AtomicLong();
        long messageToRead = 5;
        try {
            while (keepReading.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (Iterator<ConsumerRecord<String, String>> it = records.iterator(); it.hasNext(); ) {
                    ConsumerRecord consumerRecord = it.next();
                    LOGGER.info(" Key : {} , Value : {} , Partition : {} ", consumerRecord.key(), consumerRecord.value(), consumerRecord.partition());
                    messagesRead.getAndIncrement();
                    if (messagesRead.get() >= messageToRead) {
                        keepReading.set(false);
                        break;
                    }
                }
            }
        } catch (WakeupException exception) {
            LOGGER.error(" Received shutdown signal ", exception);
        } finally {
            consumer.close();
            latch.countDown();
        }
    }

    public void shutdown() {
        //Stop polling
        consumer.wakeup();
    }
}
