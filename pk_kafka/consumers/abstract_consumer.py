from abc import abstractmethod


class AbstractKafkaConsumer:

    @abstractmethod
    def start_consumer(self, handle_message_function):
        raise NotImplementedError("You need to implement this method before calling it!")