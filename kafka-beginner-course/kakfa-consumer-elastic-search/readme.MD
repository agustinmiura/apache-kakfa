Twitter Example
==============

* Copy ```twitter.properties.sample``` to ```twitter.properties```
* Register an app in Twitter .
* Add full permissions to the app.
* Run the example
* Create the topic : ```kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic twitter_tweets --partitions 6 --replication-factor 1```
* Create a consumer to check : ```kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic twitter_tweets```
* Create an account in ```https://app.bonsai.io/clusters/kafka-course-9094228913?hsmd=tail&hswd=900000&hstw=86400000``` to have access to an Elastic Search instance.
* Create an index named ```twitter``` in the Elastic Search instance .
* Check the status of the consumer ```kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --group kakfa-demo-elasticsearch --describe```
