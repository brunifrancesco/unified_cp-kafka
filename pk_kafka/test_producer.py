from pk_kafka.producer import KafkaRestProducer

# Kafka rest producer
data = [dict(element="1", ee=3) for _ in range(1000)]
KafkaRestProducer(
    rest_proxy_address="http://flash.planetek.it:8080/kafka-rest-proxy")\
    .publish_messages("test", data, parallel_processes=2, message_list_size=2)