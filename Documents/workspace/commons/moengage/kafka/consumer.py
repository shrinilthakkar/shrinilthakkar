import json
import threading

from pykafka.exceptions import KafkaException

from moengage.commons.config.provider import CommonConfigProvider
from moengage.commons.connections import ConnectionUtils
from moengage.commons.loggers.treysor import Treysor


class MoEKafkaConsumer(threading.Thread):
    def __init__(self, topic, **consumer_kwargs):
        self.topic = topic
        self.kafka_client = ConnectionUtils.getKafkaClient()
        self.kafka_topic = self.kafka_client.topics[self.topic]
        self.consumer = None
        self._running = True
        self._decode_message = consumer_kwargs.pop('decode_message', True)
        self._init_consumer(**consumer_kwargs)
        super(MoEKafkaConsumer, self).__init__()

    def _init_consumer(self, **consumer_kwargs):
        consumer_kwargs.setdefault('consumer_group', '-'.join([self.topic, 'consumer-pykafka']))
        consumer_kwargs.setdefault('use_rdkafka', True)
        consumer_kwargs.setdefault('zookeeper_connect', CommonConfigProvider().getZookeeperConfig()['connection_url'])
        consumer_kwargs.setdefault('auto_commit_enable', True)
        self.consumer = self.kafka_topic.get_balanced_consumer(**consumer_kwargs)

    def filter_message(self, message):
        return False

    def decode_message(self, message):
        try:
            message.value = json.loads(message.value)
        except Exception:
            pass

    def process_message(self, message):
        raise NotImplementedError("SubClass should implement the process message function")

    def consume(self):
        messages_processed = 0
        messages_consumed = 0
        current_message = None
        try:
            for message in self.consumer:
                if not message:
                    continue
                current_message = message
                messages_consumed += 1
                if self._running:
                    if self._decode_message:
                        self.decode_message(current_message)
                    if not self.filter_message(current_message):
                        self.process_message(current_message)
                        messages_processed += 1
                else:
                    Treysor().error(log_tag='exit_kafka_consumer', msg='Consumer stopped externally', topic=self.topic,
                                    messages_consumed=messages_consumed, messages_processed=messages_processed,
                                    current_offset=self.consumer.held_offsets)
                    break
        except KafkaException:
            Treysor().exception(log_tag='exit_kafka_consumer', topic=self.topic, messages_consumed=messages_consumed,
                                messages_processed=messages_processed, current_offset=self.consumer.held_offsets)
            raise
        except Exception:
            Treysor().exception(log_tag='exit_kafka_consumer', topic=self.topic, messages_consumed=messages_consumed,
                                messages_processed=messages_processed, current_offset=self.consumer.held_offsets,
                                errored_payload=repr(current_message.value))
            raise
        self._running = False

    def run(self):
        self.consume()

    def stop(self):
        self._running = False
        try:
            self.consumer.stop()
        except Exception as e:
            Treysor().exception(log_tag='exit_kafka_consumer', topic=self.topic, error=repr(e))


class MoEKafkaBatchConsumer(MoEKafkaConsumer):
    def __init__(self, topic, batch_size=1000, **consumer_kwargs):
        self._batch_size = batch_size
        self.message_batch = []
        super(MoEKafkaBatchConsumer, self).__init__(topic, **consumer_kwargs)

    def process_batch(self, message_batch):
        raise NotImplementedError("Sub class should implement the process message function")

    def process_message(self, message):
        self.message_batch.append(message)
        if len(self.message_batch) >= self._batch_size:
            self.process_batch(self.message_batch)
            self.message_batch = []
