package ar.com.miura.streams;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static ar.com.miura.streams.Utils.readPropertiesFile;

public class StreamsFilterTweets {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamsFilterTweets.class.getName());

    private Gson gson = (new GsonBuilder().create());

    public static void main(String[] args) {
        try {
            (new StreamsFilterTweets()).run();
        }catch(IOException e) {
            LOGGER.error(" Error ", e);
        }

    }

    public void run() throws IOException {
        var properties = readPropertiesFile("stream.properties", this.getClass());
        var kakfaProperties = new Properties();
        kakfaProperties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("server.url"));
        kakfaProperties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, properties.getProperty("application.id.config"));
        kakfaProperties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        kakfaProperties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        var streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputTopic = streamsBuilder.stream(properties.getProperty("input.topic"));
        KStream<String, String> filteredStream = inputTopic.filter((k,jsonTweet) -> extractFollowers(jsonTweet)>2);
        filteredStream.to(properties.getProperty("destination.topic"));

        KafkaStreams kafkaStreams = new KafkaStreams(
            streamsBuilder.build(),
            kakfaProperties
        );
        kafkaStreams.start();
    }

    private double extractFollowers(String tweetJson) {
        Map map =gson.fromJson(tweetJson, Map.class);
        Object user = map.get("user");
        return (double) ((LinkedTreeMap) user).get("followers_count");
    }


}
