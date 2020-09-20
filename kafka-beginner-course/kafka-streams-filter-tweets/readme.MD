Kakfa Streams
=============

* Set the values in ```stream.properties```
* Create the topics : ```kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic twitter_tweets --partitions 6 --replication-factor 1```
                      ```kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic important_tweets --create --partitions 3 --replication-factor 1```
* Run the app.
* Check the topic : ``` kafka-console-consumer.sh console-consumer --bootstrap-server 127.0.0.1:9092 --topic important_tweets --from-beginning```