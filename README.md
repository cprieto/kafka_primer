# Kafka Primer

You have no idea about this Kafka thing? well, I did a simple example using Kotlin and Spring Boot!

Because there are different ways to skin this thing, I used different methods to consume/produce data and to keep it in the same repo I am using tags, so far:

  - *v1.0* Using classic consumer/producer directly with Kafka
  - *v1.1* Using Kafka Streams directly (this is when shit starts to get awesome)
  - *v1.2* Using windowed functions and handling late events

You can check the corresponding release and tag and see the difference in code

You need to have Kafka somewhere, you could use Confluent Kafka options, just remember if using the container it will need 8GB of memspace to run (you can blame Zookeeper)
