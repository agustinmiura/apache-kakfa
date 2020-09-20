package ar.com.miura.elastic;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static ar.com.miura.Utils.readPropertiesFile;

public class ElasticSearchConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    private Gson gson = (new GsonBuilder().create());

    public static void main(String[] args) {
        try {
            new ElasticSearchConsumer().run();
        } catch (IOException e) {
            LOGGER.error(" Error ", e);
        }
    }

    public ElasticSearchConsumer() {
    }

    public void run() throws IOException {
        var client = createClient();

        var consumer = createConsumer();
        //poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            LOGGER.info(" Received records : {} ", records.count());
            records.iterator().forEachRemaining(eachRecord -> {
                try {
                    String id = extractId(eachRecord.value());
                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id);
                    indexRequest.source(eachRecord.value(), XContentType.JSON);
                    IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);

                    Thread.sleep(10);
                    LOGGER.info("The id : {} ", id);
                } catch (IOException | InterruptedException ioException) {
                    LOGGER.error(" Error ", ioException);
                }
            });
            LOGGER.info(" Commiting the offsets ");
            consumer.commitSync();
            LOGGER.info(" Offsets have been commited");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public RestHighLevelClient createClient() throws IOException {

        Properties properties = readPropertiesFile("elastic.properties", this.getClass());

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(properties.getProperty("elastic.access.key"), properties.getProperty("elastic.access.secret")));

        RestClientBuilder builder = RestClient.builder(new HttpHost(properties.getProperty("elastic.url"), 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        return new RestHighLevelClient(builder);
    }

    public KafkaConsumer<String, String> createConsumer() throws IOException {
        Properties properties = new Properties();
        Properties fromConfig = readPropertiesFile("application.properties", this.getClass());

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, fromConfig.getProperty("server.url"));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, fromConfig.getProperty("consumer.group.id"));
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, fromConfig.getProperty("consumer.offset.reset.early"));
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

        //Create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //Suscribe consumer to our topic
        consumer.subscribe(Collections.singleton(fromConfig.getProperty("topic.name")));
        return consumer;
    }

    private String extractId(String tweetJson) {
        Map map =gson.fromJson(tweetJson, Map.class);
        return (String) map.get("id_str");
    }
}
