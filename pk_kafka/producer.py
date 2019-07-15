import json
from collections import Iterable
from functools import partial
from multiprocessing.pool import Pool

import requests
from confluent_kafka import Producer

from pk_kafka.exceptions import MessageValueException, RestProducerConfigException


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


class _KafkaRestProducerSessionFactory:
    @staticmethod
    def make(credentials):
        s = requests.Session()
        if credentials:
            s.auth = credentials
        s.headers.update({"Content-Type": "application/vnd.kafka.json.v2+json"})
        s.headers.update({"Accept": "application/vnd.kafka.v2+json"})
        return s


class KafkaRestProducer:

    def __init__(self, rest_proxy_address, credentials=None):
        self.rest_proxy_address = rest_proxy_address
        self.session = _KafkaRestProducerSessionFactory.make(credentials)

    @staticmethod
    def _check_for_bulk_operation(item):
        return item is not None and isinstance(item, int) and item > 1

    @staticmethod
    def _chunks(array, n):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(array), n):
            yield array[i:i + n]

    def _publish_messages_in_bulk(self, topic, messages):
        return self.session.post(
            '%s/topics/%s' % (self.rest_proxy_address, topic),
            data=json.dumps({"records": [{"value": message} for message in messages]})
        )

    def _evaluate_and_split_by_size(self, message_list_size, messages):
        messages = list(self._chunks(messages, message_list_size))
        return messages

    def publish_message(self, topic, message):
        return self.session.post(
            '%s/topics/%s' % (self.rest_proxy_address, topic),
            data=json.dumps({"records": [{"value": message}]})
        )

    def publish_messages(self, topic, messages, parallel_processes=1, message_list_size=1):
        assert isinstance(messages, Iterable)
        messages = list(messages)
        need_to_use_pool = self._check_for_bulk_operation(parallel_processes)
        need_to_send_messages_in_bulk = self._check_for_bulk_operation(message_list_size)
        if not need_to_send_messages_in_bulk and need_to_use_pool:
            raise RestProducerConfigException()
        publish_message_with_topic = partial(self.publish_message, topic)
        if need_to_send_messages_in_bulk:
            messages = self._evaluate_and_split_by_size(message_list_size, messages)
            publish_message_with_topic = partial(self._publish_messages_in_bulk, topic)
            if need_to_use_pool:
                return Pool(parallel_processes).map(publish_message_with_topic, messages)
        return [publish_message_with_topic(message) for message in messages]