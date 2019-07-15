class MessageValueException(Exception):
    def __str__(self):
        return "It seems you used a non JSON serializable object"

class RestProducerConfigException(Exception):
    def __str__(self):
        return "You cannot send multiple parallel requests without sending multiple record per message!"
