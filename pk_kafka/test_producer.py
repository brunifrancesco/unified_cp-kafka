from pk_kafka.producer import KafkaProducer, KafkaRestProducer


def handle_json_message_data(msg):
    print(msg)


# KafkaProducer(
#     broker_address='127.0.1.1',
# ).publish_message('myTopic', {'key': 2})


result = KafkaRestProducer(
    broker_address='http://spinua.rheticus.eu:5000/kafka-rest-proxy',
    credentials=('dockermanager', 'CTGsSH4e5x')
).publish_message('removeme', {'vediamo':'sefunziona'})
print(result.text)