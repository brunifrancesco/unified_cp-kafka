class RestProducerConfigException(Exception):
    def __str__(self):
        return "You cannot send multiple parallel requests without sending multiple record per message!"