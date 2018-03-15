import json
import threading

from bson import json_util

from moengage.kafka.producer import MoEKafkaProducer
from moengage.oplog.loggers import OplogProducerTreysor
from moengage.oplog.oplog_ingestor_base import OplogIngestorBase
from moengage.oplog.utils import OperationType


class KafkaOplogIngestor(OplogIngestorBase):
    def __init__(self, topic):
        self.mutex = threading.Lock()
        self.topic = topic
        self.max_queued_messages = 5000
        self.kafka_producer = self.get_kafka_producer()

    def get_kafka_producer(self, **kwargs):
        return MoEKafkaProducer(self.topic,
                                key_serializer=str.encode,
                                use_rdkafka=kwargs.get('use_rdkafka', True),
                                min_queued_messages=10,
                                max_queued_messages=self.max_queued_messages,
                                linger_ms=5 * 1000,
                                auto_start=kwargs.get('auto_start', True),
                                max_request_size=10 * 1024 * 1024,
                                sync=False,
                                delivery_reports=False,
                                block_on_queue_full=False,
                                value_serializer=lambda m: json.dumps(m).encode('ascii'))

    def stop_kafka_producer(self):
        if self.kafka_producer and self.kafka_producer.producer:
            try:
                self.kafka_producer.producer.stop()
            except Exception as e:
                OplogProducerTreysor().exception(log_tag='exit_kafka_producer',
                                                 producer_type=str(self.kafka_producer.topic), error=repr(e))

    @classmethod
    def encode_object(cls, obj):
        return json.loads(json_util.dumps(obj))

    @classmethod
    def transform_oplog_entry(cls, shard_id, entry):
        db, collection = entry['ns'].split('.')
        document_id = str(OperationType.fromStr(entry['op']).
                          getDocumentId(entry.get('o'), entry.get('o2')))
        document_action = {
            'shard_id': shard_id,
            'db_name': db,
            'collection': collection,
            'doc_id': document_id,
            'ts': cls.encode_object(entry['ts']),
            'op': entry['op'],
            'o': cls.encode_object(entry.get('o')),
            'o2': cls.encode_object(entry.get('o2'))
        }
        return document_action

    @classmethod
    def docs_to_upsert(cls, shard_id, batch):
        for entry in batch:
            doc = cls.transform_oplog_entry(shard_id, entry)
            yield doc

    def ingest_batch(self, shard_id, batch, oplog_progress=None):
        for oplog in self.docs_to_upsert(shard_id, batch):
            doc_id = oplog.get('doc_id')
            try:
                self.kafka_producer.produce(oplog, partition_key=doc_id)
            except Exception, e:
                OplogProducerTreysor().exception(log_tag="ingest_batch_failed", error=repr(e))
