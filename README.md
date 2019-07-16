# Unified Kafka Module

This module aims to simplify the usage of Apache Kafka via Python code.

## Consumer
    
    def handle_json_message_data(msg_value):
        # this is the function to process the message value
        print(msg_value)


    KafkaConsumerThread(
        topic="myTopic",
        broker_address='127.0.1.1',
        handle_json_message_data=True # if json messages have been passed,
        run_as_separate_thread=False # True if you want to run the consumer in a separate thread
    ).start_consumer(handle_json_message_data)
    
    KafkaConsumerRestThread(
        topic="test",
        server_address="myhost:8080/kafka-rest-proxy",
        run_as_separate_thread=True
    ).start_consumer(print)

    
## Producer

    KafkaProducer(
        broker_address='127.0.1.1' # kafka broker,
    ).publish_message('topic', <a JSON serializable object>)
    
    
    data = [dict(element="1", ee=3) for _ in range(1000)]
    KafkaRestProducer(
        rest_proxy_address="http://yourhost:8080/kafka-rest-proxy")\
        .publish_messages("test", data, parallel_processes=2, message_list_size=2)

