from abc import abstractmethod


class AbstractProducer:

    @abstractmethod
    def publish_message(self, topic, message):
        raise NotImplementedError("You need to implement the method before calling it")

    @abstractmethod
    def publish_messages(self, topic, messages):
        raise NotImplementedError("You need to implement the method before calling it")
