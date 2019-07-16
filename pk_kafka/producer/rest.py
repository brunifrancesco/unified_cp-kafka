import json
from functools import partial
from itertools import chain, islice
from multiprocessing.pool import Pool
from typing import Iterable

import requests
from requests.auth import HTTPBasicAuth

from pk_kafka.producer.exceptions import RestProducerConfigException


class KafkaRestProducer:

    def __init__(self, rest_proxy_address, credentials=None):
        self.rest_proxy_address = rest_proxy_address
        self.session = _KafkaRestProducerSessionFactory.make(credentials)

    @staticmethod
    def _check_for_bulk_operation(item):
        return item is not None and isinstance(item, int) and item > 1

    @staticmethod
    def _evaluate_and_split_message_list_by_size(message_list_size, messages):
        """
        Split message iterable by chunks of size :param message_list_size
        :param message_list_size: the chunk size
        :param messages: the list to be splitted in chunks
        :return the chunk as generator
        """
        iterator = iter(messages)
        for first in iterator:
            yield chain([first], islice(iterator, message_list_size - 1))

    def _publish_messages_in_bulk(self, topic, messages):
        return self.session.post(
            '%s/topics/%s' % (self.rest_proxy_address, topic),
            data=json.dumps({"records": [{"value": message} for message in messages]})
        )

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
            messages = self._evaluate_and_split_message_list_by_size(message_list_size, messages)
            publish_message_with_topic = partial(self._publish_messages_in_bulk, topic)
            if need_to_use_pool:
                return Pool(parallel_processes).map(publish_message_with_topic, messages)
        return [publish_message_with_topic(message) for message in messages]


class _KafkaRestProducerSessionFactory:
    @staticmethod
    def make(credentials):
        """
        Create the Request session object.
        Given the simplicity of this method, this object will set just few headers
        as well as the basic auth credentials
        :param credentials:
        :return:
        """
        s = requests.Session()
        if credentials:
            assert isinstance(credentials, HTTPBasicAuth)
            s.auth = credentials
        s.headers.update({"Content-Type": "application/vnd.kafka.json.v2+json"})
        s.headers.update({"Accept": "application/vnd.kafka.v2+json"})
        return s