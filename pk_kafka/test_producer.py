from producer import KafkaProducer


def handle_json_message_data(msg):
    print(msg)


KafkaProducer(
    broker_address='127.0.1.1',
).publish_message('myTopic', {'key': 2})
