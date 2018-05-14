from moengage.commons.connections import ConnectionUtils


class MoEKafkaProducer(object):
    def __init__(self, topic, **producer_kwargs):
        self.topic = topic
        self.kafka_client = ConnectionUtils.getKafkaClient()
        self.kafka_topic = self.kafka_client.topics[self.topic]
        self.producer = None
        self.key_serializer = None
        self.value_serializer = None
        self._init_producer(**producer_kwargs)

    def _init_producer(self, **producer_kwargs):
        producer_kwargs.setdefault('sync', True)
        producer_kwargs.setdefault('use_rdkafka', True)
        self.key_serializer = producer_kwargs.pop('key_serializer', None)
        self.value_serializer = producer_kwargs.pop('value_serializer', None)
        self.producer = self.kafka_topic.get_producer(**producer_kwargs)

    def produce(self, message, partition_key=None):
        if partition_key and self.key_serializer:
            partition_key = self.key_serializer(partition_key)
        if message is not None and self.value_serializer:
            message = self.value_serializer(message)
        return self.producer.produce(message, partition_key=partition_key)
