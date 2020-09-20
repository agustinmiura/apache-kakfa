package ar.com.miura.fullexample;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static ar.com.miura.Utils.readPropertiesFile;

public class TwitterProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterProducer.class.getName());

    public static void main(String[] args) {
        try {
            new TwitterProducer().run();
        } catch (IOException e) {
            LOGGER.error(" Error ", e);
        }
    }

    public TwitterProducer() throws IOException {
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) throws IOException  {

        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("bitcoin", "usa", "politics");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Properties properties = readPropertiesFile("twitter.properties", this.getClass());
        Authentication hosebirdAuth = new OAuth1(properties.getProperty("api.key"), properties.getProperty("api.secret"), properties.getProperty("api.token"), properties.getProperty("api.token.secret"));

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

    public void run() throws IOException {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        Client client = createTwitterClient(msgQueue);
        client.connect();



        KafkaProducer<String, String> kafkaProducer =  createKakfaProducer();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info(" Shutdown of the app : ");
            client.stop();
            kafkaProducer.close();
        }));

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOGGER.error(" Error thread interrupted ", e);
                client.stop();
            }
            if (msg != null) {
                LOGGER.info(" The message is {} ", msg);
                kafkaProducer.send(new ProducerRecord<>("twitter_tweets", null, msg), (recordMetadata, e) -> {
                    if (e !=null) {
                        LOGGER.error(" Error sending message ", e);
                    }
                });
            }
        }
        LOGGER.info(" End of application ");
    }

    public KafkaProducer<String, String> createKakfaProducer() throws IOException {
        final KafkaProducer<String, String> producer;
        Properties properties = new Properties();
        Properties fromConfig = readPropertiesFile("application.properties", this.getClass());;
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, fromConfig.getProperty("server.url"));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create safer settings
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");//For Kafka 2.0 use 5 in another case use 1

        //increase throughput , increase cpu usage , increase latency.
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        return new KafkaProducer<String, String>(properties);
    }
}
