import json
import pickle
import zlib

import blosc
import msgpack
from bson import json_util
from enum import Enum


class SupportedEncoders(Enum):
    # pickle is not recommended as it is python only encoder
    PICKLE = 1
    MESSAGE_PACK = 2
    BSON = 3
    JSON = 4

    def __str__(self):
        return {
            SupportedEncoders.PICKLE: 'pickle',
            SupportedEncoders.MESSAGE_PACK: 'message_pack',
            SupportedEncoders.BSON: 'bson',
            SupportedEncoders.JSON: 'json'
        }.get(self)

    @classmethod
    def from_str(cls, value):
        return {
            'pickle': SupportedEncoders.PICKLE,
            'message_pack': SupportedEncoders.MESSAGE_PACK,
            'bson': SupportedEncoders.BSON
        }.get(value)

    def encode(self, value_to_encode, **kwargs):
        if self == SupportedEncoders.PICKLE:
            return pickle.dumps(value_to_encode)
        elif self == SupportedEncoders.MESSAGE_PACK:
            return msgpack.dumps(value_to_encode)
        elif self == SupportedEncoders.BSON:
            return json_util.dumps(value_to_encode)
        elif self == SupportedEncoders.JSON:
            return json.dumps(value_to_encode, indent=kwargs.get('indent', 4))

    def decode(self, value_to_decode, **kwargs):
        if self == SupportedEncoders.PICKLE:
            return pickle.loads(value_to_decode)
        elif self == SupportedEncoders.MESSAGE_PACK:
            return msgpack.loads(value_to_decode)
        elif self == SupportedEncoders.BSON:
            return json_util.loads(value_to_decode)
        elif self == SupportedEncoders.JSON:
            return json.loads(value_to_decode)


class SupportedCompressions(Enum):
    ZLIB = 1
    BLOSC = 2

    def __str__(self):
        return {
            SupportedCompressions.ZLIB: 'zlib',
            SupportedCompressions.BLOSC: 'blosc'
        }.get(self)

    @classmethod
    def from_str(cls, value):
        return {
            'zlib': SupportedCompressions.ZLIB,
            'blosc': SupportedCompressions.BLOSC
        }.get(value)

    def compress(self, value_to_compress, **kwargs):
        if self == SupportedCompressions.ZLIB:
            return zlib.compress(value_to_compress, kwargs.get('level', 6))
        elif self == SupportedCompressions.BLOSC:
            return blosc.compress(value_to_compress)

    def decompress(self, value_to_decompress, **kwargs):
        if self == SupportedCompressions.ZLIB:
            return zlib.decompress(value_to_decompress)
        elif self == SupportedCompressions.BLOSC:
            return blosc.decompress(value_to_decompress)


class CompressionUtils(object):
    @classmethod
    def encode_obj(cls, obj, encoder, **kwargs):
        encoder = SupportedEncoders.from_str(encoder) if isinstance(encoder, basestring) else encoder
        return encoder.encode(obj, **kwargs)

    @classmethod
    def decode_obj(cls, obj, encoder, **kwargs):
        encoder = SupportedEncoders.from_str(encoder) if isinstance(encoder, basestring) else encoder
        return encoder.decode(obj, **kwargs)

    @classmethod
    def compress_obj(cls, obj, compression, **kwargs):
        compression = SupportedCompressions.from_str(compression) if isinstance(compression,
                                                                                basestring) else compression
        return compression.compress(obj, **kwargs)

    @classmethod
    def decompress_obj(cls, obj, compression, **kwargs):
        compression = SupportedCompressions.from_str(compression) if isinstance(compression,
                                                                                basestring) else compression
        return compression.decompress(obj, **kwargs)
