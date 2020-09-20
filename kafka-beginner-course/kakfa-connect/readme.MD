Kafka Connect Sample
====================

* Create the topics : ```kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic twitter_tweets --partitions 6 --replication-factor 1```
                      ```kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic twitter_tweets --partitions 6 --replication-factor 1```
* Consume the topics : ```kafka-console-consumer.sh --bootstrap-server 127.0.0.1:211 -topic twitter_status_connect --from-beginning```
* Setup the twitter.properties file like in the sample file.
* Check with the consumer : ```kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 -topic twitter_status_connect --from-beginning
```