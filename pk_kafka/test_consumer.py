import unittest
from unittest import TestCase

import requests
from mock import patch, Mock, mock

from pk_kafka.consumers.rest import KafkaConsumerRestThread


def mocked_requests_get(fake):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    if fake:
        return MockResponse([{"value": dict(key="value")}], 200)
    else:
        return MockResponse({"key2": "value2"}, 200)


class KafkaConsumerRestThreadTestCase(TestCase):
    def test_create_object(self):
        kafka_consumer_rest_thread = KafkaConsumerRestThread(
            topic="topic_test",
            server_address="http://myhost:8080",
            credentials=None,
            consumer_group="consumerGroupId",
            handle_json_message_data=False
        )

        assert kafka_consumer_rest_thread
        assert kafka_consumer_rest_thread.topic == "topic_test"
        assert kafka_consumer_rest_thread.server_address == "http://myhost:8080"
        assert not kafka_consumer_rest_thread.run_as_separate_thread
        assert isinstance(kafka_consumer_rest_thread.session, requests.Session)
        assert isinstance(kafka_consumer_rest_thread.parameters, dict)
        assert not kafka_consumer_rest_thread.session.auth
        assert kafka_consumer_rest_thread.parameters == {
            "consumer_url": "%s/consumers/%s" % ("http://myhost:8080", "consumerGroupId"),
            "consumer_instance_url": "%s/consumers/%s/instances/%s_instance/subscription" % (
                "http://myhost:8080", "consumerGroupId", "consumerGroupId"),
            "consumer_instance_name": "%s_instance" % "consumerGroupId",
        }

    def test_start_consumer_thread(self):
        with patch('pk_kafka.consumers.rest.KafkaConsumerRestThread.start_consumer') as mock:
            t = KafkaConsumerRestThread.start_consumer(print)
            t.join()
            mock.assert_called_once_with(print)

    def test_run_thread_function(self):
        session = Mock()
        session.get.return_value = mocked_requests_get(True)
        session.post.return_value = mocked_requests_get(True)

        kafka_consumer_rest_thread = KafkaConsumerRestThread(
            topic="topic_test",
            server_address="http://myhost:8080",
            credentials=None,
            consumer_group="consumerGroupId",
            handle_json_message_data=False
        )
        kafka_consumer_rest_thread.session = session

        kafka_consumer_rest_thread.run_thread(lambda item: item.keys(), keep_looping=False)
        self.assertEqual(session.post.call_count, 2)
        self.assertEqual(session.get.call_count, 1)
        session.get.assert_called_with(url="http://myhost:8080")
        session.assert_has_calls([mock.call.post(
            url=kafka_consumer_rest_thread.parameters['consumer_url'],
            json={
                "name": kafka_consumer_rest_thread.parameters['consumer_instance_name'],
                "format": "json",
                "auto.offset.reset": "latest"
            }),
            mock.call.post(
                url=kafka_consumer_rest_thread.parameters['consumer_instance_url'],
                json={"topics": ["%s" % kafka_consumer_rest_thread.topic]}),
        ]
        )


if __name__ == "__main__":
    unittest.main()
