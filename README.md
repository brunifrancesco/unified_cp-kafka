# Planetek Kafka Module

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


    
## Producer

    KafkaProducer(
        broker_address='127.0.1.1' # kafka broker,
    ).publish_message('topic', <a JSON serializable object>)
    