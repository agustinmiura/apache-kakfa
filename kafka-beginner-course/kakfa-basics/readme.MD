Examples for Java 
=================

In the package :

* ar.com.miura.kakfa.tutorial1 : Examples for Kafka.
* ar.com.miura.kafka.fullexample: Consume tweets from the api and send them to Kafka and stored them in an Elastic Search instance .

* Start the Apache Kafka cluster.

* For the producer example execute the following code:
```
kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic first_topic --group my-third-application
```
* To start a consumer to see the messages .
```
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group my-first-application --reset-offsets --shifts-by -2 --execute --topic first_topic
```
* Describe a consumer group:
```
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group my-fourth-application --describe
```
List all topics
```
kafka-topics.sh --list --zookeeper localhost:2181
```
Create an Apache Kafka Topic with 3 partitions
```
kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic twitter_tweets --partitions 3  1
```