from abc import ABCMeta
from moengage.commons.config.provider import ConfigFileProvider


class OplogIngestorBase(object):
    __metaclass__ = ABCMeta

    @classmethod
    def encode_object(cls, obj):
        raise NotImplementedError("Child Class must implement encode_object function from super")

    @classmethod
    def config(cls, module, config_file_name):
        config_provider = ConfigFileProvider(config_file_name, module)
        return config_provider.config

    @classmethod
    def docs_to_upsert(cls, shard_id, batch):
        raise NotImplementedError("Child Class must implement docs_to_upsert function from super")

    @classmethod
    def transform_oplog_entry(cls, shard_id, entry):
        raise NotImplementedError("Child Class must implement transform_oplog_entry function from super")

    def ingest_batch(self, shard_id, batch, oplog_progress):
        raise NotImplementedError("Child Class must implement ingest_batch function from super")
