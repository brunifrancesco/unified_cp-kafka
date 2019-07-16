import threading
import uuid
from time import sleep

import requests

from pk_kafka.consumers.abstract_consumer import AbstractKafkaConsumer


class _KakfkaRestConsumerSessionFactory:
    @staticmethod
    def make(credentials):
        s = requests.Session()
        if credentials:
            s.auth = credentials
        s.headers.update({'Accept': 'application/vnd.kafka.json.v2+json'})
        s.headers.update({'Content-Type': 'application/vnd.kafka.v2+json'})
        return s


class _KakfaRestConsumerParametersFactory:
    @staticmethod
    def make(server_address, consumer_instance_name):
        return {
            "consumer_url": "%s/consumers/%s" % (server_address, consumer_instance_name),
            "consumer_instance_url": "%s/consumers/%s/instances/%s_instance/subscription" % (
                server_address, consumer_instance_name, consumer_instance_name),
            "consumer_instance_name": "%s_instance" % consumer_instance_name,
            "records_url": "%s/consumers/%s/instances/%s_instance/records" % (
                server_address, consumer_instance_name, consumer_instance_name),
        }


class KafkaConsumerRestThread(AbstractKafkaConsumer):

    def __init__(self,
                 topic,
                 server_address,
                 credentials=None,
                 handle_json_message_data=True,
                 consumer_group=str(uuid.uuid1())[0:10],
                 run_as_separate_thread=False
                 ):
        self.topic = topic
        self.server_address = server_address
        self.handle_json_message_data = handle_json_message_data
        self.consumer_group = consumer_group
        self.run_as_separate_thread = run_as_separate_thread

        self.session = _KakfkaRestConsumerSessionFactory.make(credentials)
        self.parameters = _KakfaRestConsumerParametersFactory.make(self.server_address, self.consumer_group)

    def start_consumer(self, handle_message_function):
        """
        Create the consumer, start it and begin consuming messages running a new thread
        :param handle_message_function: the function to handle consumed message
        """
        thread = threading.Thread(target=self.run_thread, args=(handle_message_function,))
        if not self.run_as_separate_thread:
            thread.daemon = False
        thread.start()
        return thread

    def run_thread(self, handle_message_value_function, keep_looping=True):
        """
        Run the thread to consume new incoming messages with the :param handle_message_value_function
        - assure consumer instance is up
        - consume new messages
        - sleep and start again

        :param handle_message_value_function: the message to consume the value with
        """
        while True:
            try:
                self._assure_subscription_is_up()

                records = self.session.get(
                    url=self.parameters['records_url'],
                )
                data = records.json()
                for element in data:
                    if 'value' in element:
                        handle_message_value_function(element['value'])
                    else:
                        print("No data to be processed")
            except Exception as e:
                print("Error getting the data..")
                print(e)
            finally:
                if keep_looping:
                    sleep(30)
                else:
                    break

    def _assure_subscription_is_up(self):
        self.session.post(
            url=self.parameters['consumer_url'],
            json={
                "name": self.parameters['consumer_instance_name'],
                "format": "json",
                "auto.offset.reset": "latest"
            }
        )
        self.session.post(
            url=self.parameters['consumer_instance_url'],
            json={"topics": ["%s" % self.topic]}
        )
