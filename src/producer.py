import json
from confluent_kafka import Producer
from json.decoder import JSONDecodeError

from pk_kafka.exceptions import MessageValueException


class KafkaProducer:
    """
    Start a new Producer to publish message to topic
    """

    def __init__(self, broker_address, handle_json_message_data=True):
        """
        Init the main class
        :param broker_address: the broker address
        :param handle_json_message_data: True if you want to handle json formatted data, False otherwise
        """
        self.broker_address = broker_address
        self.producer = Producer({'bootstrap.servers': self.broker_address})
        self.handle_json_message_data = handle_json_message_data

    def publish_message(self, topic, message):
        """
        Create the producer, check for provided serializable message
        and publish it to the topic
        :param topic: the topic whose message will published to
        :param message: the message to be published
        """

        def delivery_report(err, msg):
            """ Called once for each message produced to indicate delivery result.
                Triggered by poll() or flush(). """
            if err is not None:
                print('Message delivery failed: {}'.format(err))
            else:
                print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

        # Trigger any available delivery report callbacks from previous produce() calls
        self.producer.poll(0)

        # Asynchronously produce a message, the delivery report callback
        # will be triggered from poll() above, or flush() below, when the message has
        # been successfully delivered or failed permanently.
        value_to_publish = message

        if self.handle_json_message_data:
            if type(message) not in (dict, list):
                raise MessageValueException("Your message should be json serializable!")
        value_to_publish = json.dumps(value_to_publish)

        self.producer.produce(topic, value_to_publish.encode('utf8'), callback=delivery_report)

        # Wait for any outstanding messages to be delivered and delivery report
        # callbacks to be triggered.
        self.producer.flush()
