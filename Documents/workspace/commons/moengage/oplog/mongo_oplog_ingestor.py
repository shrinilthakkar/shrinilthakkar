import threading

import pymongo
from bson import json_util

from moengage.commons import CommonUtils, InfraType, ConnectionUtils, DBCategory
from moengage.commons.config import CommonConfigProvider
from moengage.oplog.loggers import OplogProducerTreysor
from moengage.oplog.oplog_ingestor_base import OplogIngestorBase


class MongoOplogIngestor(OplogIngestorBase):
    def __init__(self, database):
        self.mutex = threading.Lock()
        self.mongo_client = ConnectionUtils.getMongoConnectionForInfraType(InfraType.SEGMENTATION,
                                                                           pymongo.ReadPreference.PRIMARY_PREFERRED)[
            database]

    def add_new_db(self, db_name, oplog_progress):
        with oplog_progress as oplog_prog:
            oplog_dict = oplog_prog.get_dict()
            if 'db_names' not in oplog_dict:
                oplog_dict['db_names'] = set()
            oplog_dict['db_names'].add(db_name)

    @classmethod
    def encode_object(cls, obj):
        return json_util.dumps(obj)

    @classmethod
    def transform_oplog_entry(cls, shard_id, entry):
        db_name, collection = entry['ns'].split('.')
        document_action = {
            'shard_id': shard_id,
            'collection': collection,
            'db_name': db_name,
            'ts': entry['ts'],
            'op': entry['op'],
            'o': cls.encode_object(entry.get('o')),
            'o2': cls.encode_object(entry.get('o2'))
        }
        return document_action

    @classmethod
    def docs_to_upsert(cls, shard_id, batch):
        entries = {}
        for entry in batch:
            doc = cls.transform_oplog_entry(shard_id, entry)
            db_name = doc.pop('db_name')
            if db_name not in entries:
                entries[db_name] = []
            entries[db_name].append(doc)
        return entries

    def get_oplog_max_size_by_db_category(self, db_category):
        return CommonConfigProvider().getAppCategoryConfig()[str(db_category)]['oplog_max_size']

    def create_collection(self, db_name):
        # TODO app_category configurable
        with self.mutex:
            if db_name not in self.mongo_client.collection_names():
                db_category = CommonUtils.getDBCategory(db_name)
                if db_category and db_category != DBCategory.XSMALL:
                    max_size = self.get_oplog_max_size_by_db_category(db_category)
                    OplogProducerTreysor().info(log_tag="creating_collection", db_name=repr(db_name), max_size=max_size)
                    self.mongo_client.create_collection(db_name, capped=True, size=max_size)
                    return True
                else:
                    return False
            return True

    def ingest_batch(self, shard_id, batch, oplog_progress):
        for db_name, oplogs in self.docs_to_upsert(shard_id, batch).items():
            try:
                if db_name not in oplog_progress.get_dict().get('db_names', set()):
                    created = self.create_collection(db_name)
                    if created:
                        self.add_new_db(db_name, oplog_progress)
                    else:
                        continue
                bulk_builder = self.mongo_client[db_name].initialize_ordered_bulk_op()
                for op in oplogs:
                    bulk_builder.insert(op)
                bulk_builder.execute()
            except pymongo.errors.PyMongoError, e:
                OplogProducerTreysor().exception(log_tag="ingest_batch_failed", error=repr(e), shard_id=repr(shard_id))
                raise
