import inspect
import json
import logging
import logging.handlers
import os
import pickle
import random
import string
import sys
import traceback
import unicodedata
from datetime import datetime, timedelta

import pkg_resources
from Crypto.Cipher import DES
from bson.objectid import ObjectId
from enum import Enum

from moengage.commons.config.provider import CommonConfigProvider
from moengage.commons.utils.db_category import DBCategory
from moengage.commons.utils.platforms import Platforms
from moengage.config_manager.util import ConfigUtils
from moengage.package.utils import PackageUtils


class CommonUtils(object):
    @staticmethod
    def getEnv():
        return PackageUtils.getExecutionEnv()

    @staticmethod
    def encodeValue(value, encoding='utf-8'):
        try:
            return value.encode(encoding) if isinstance(value, basestring) else value
        except UnicodeError:
            return value

    @staticmethod
    def decodeValue(value, encoding='utf-8'):
        try:
            return value.decode(encoding) if isinstance(value, basestring) else value
        except UnicodeError:
            return value

    @staticmethod
    def getAllPlatforms():
        return map(lambda pl: str(pl), Platforms)

    @staticmethod
    def view_traceback():
        tb = sys.exc_info()
        traceback_string = traceback.format_exc(tb[2])
        del tb
        return traceback_string

    @staticmethod
    def getFunctionCallerInfo(function_call_level):
        func = sys._getframe(function_call_level).f_code
        return {'func_call_info': func.co_name + '@' + func.co_filename + ':' + str(func.co_firstlineno)}

    @staticmethod
    def deepMergeDictionaries(dict_source, dict_to_merge):
        for key in dict_to_merge:
            if key in dict_source:
                if isinstance(dict_source[key], dict) and isinstance(dict_to_merge[key], dict):
                    dict_source[key] = CommonUtils.deepMergeDictionaries(dict_source[key], dict_to_merge[key])
                else:
                    dict_source[key] = dict_to_merge[key]
            else:
                dict_source[key] = dict_to_merge[key]
        return dict_source

    @staticmethod
    def readResourceJson(module, path):
        json_string = CommonUtils.readConfig(module, path)
        return json.loads(json_string)

    @classmethod
    def readResourceString(cls, module, path):
        return pkg_resources.resource_string(module, path)

    @classmethod
    def readPackageResourceJson(cls, module, path):
        return json.loads(cls.readResourceString(module, path))

    @classmethod
    def readConfig(cls, module, file_path):
        config_folder = os.path.join(*module.split('.'))
        config_file = os.path.join(config_folder, file_path)
        try:
            return cls.readConfigFile(config_file)
        except IOError:
            config_folder = os.path.dirname(os.path.join(*module.split('.')))
            config_file = os.path.join(config_folder, file_path)
            return cls.readConfigFile(config_file)

    @classmethod
    def readConfigFile(cls, file_path):
        with open(ConfigUtils.get_local_file_path_for_file(file_path)) as conf_file_fp:
            return conf_file_fp.read()

    @staticmethod
    def serializable(o):
        from moengage.commons import SerializableObject
        if isinstance(o, Enum):
            return str(o)
        elif isinstance(o, datetime):
            return o.isoformat()
        elif isinstance(o, ObjectId):
            return str(o)
        elif isinstance(o, timedelta):
            return str(o)
        elif hasattr(o, '__name__'):
            return {k[1:] if k.startswith('_') else k: v for k, v in o.__dict__.items() if v}
        elif isinstance(o, dict):
            return {CommonUtils.serializable(key): CommonUtils.serializable(value) for key, value in o.items()}
        elif isinstance(o, list) or isinstance(o, set):
            serialized = list()
            for item in o:
                serialized.append(CommonUtils.serializable(item))
            return serialized
        elif isinstance(o, SerializableObject):
            return o.to_dict()
        else:
            return o

    @staticmethod
    def to_json(dictionary, **kwargs):
        try:
            return json.dumps(dictionary, default=lambda o: CommonUtils.serializable(o), **kwargs)
        except TypeError:
            return CommonUtils.to_json(CommonUtils.to_serializable_dict(dictionary), **kwargs)

    @staticmethod
    def to_serializable_dict(dictionary):
        return {CommonUtils.serializable(k): CommonUtils.serializable(v) for k, v in dictionary.items()}

    @staticmethod
    def getClassNameForMethod(method):
        try:
            for cls in inspect.getmro(method.im_class):
                if method.__name__ in cls.__dict__:
                    return cls.__name__
            return method.__module__
        except Exception:
            return method.__module__

    @staticmethod
    def decryptData(cipher_text, key="nQ1&@8s#"):
        try:
            cipher_text = pickle.loads(cipher_text)
            iv = cipher_text[0:8]
            des = DES.new(key, DES.MODE_CFB, iv)
            data = des.decrypt(cipher_text[8:])
            data = data.decode('utf-8')
            return str(data)
        except EOFError:
            return cipher_text

    @staticmethod
    def generateRandomString(string_length):
        return ''.join(random.sample(string.letters * 5, string_length))

    @classmethod
    def convertToMongoDotNotation(cls, update_dict):
        return cls.convertToFlattenedDict(update_dict)

    @classmethod
    def convertToFlattenedDict(cls, source_dict, key_joiner='.'):
        flattened_dict = {}
        for key in source_dict:
            if isinstance(source_dict[key], dict):
                flattened_sub_dict = cls.convertToFlattenedDict(source_dict[key])
                for flattened_key in flattened_sub_dict:
                    flattened_dict[key + key_joiner + flattened_key] = flattened_sub_dict[flattened_key]
            else:
                flattened_dict[key] = source_dict[key]
        return flattened_dict

    @classmethod
    def getClassNameForObject(cls, obj):
        try:
            if inspect.isclass(obj):
                obj_class = obj
            else:
                obj_class = obj.__class__
            return obj_class.__module__ + '.' + obj_class.__name__
        except Exception:
            return ''

    @classmethod
    def getDBCategory(cls, db_name):
        from moengage.daos import UserDAO
        from moengage.commons.decorators import MemCached

        @MemCached('db_category_' + db_name, secs_to_refresh=1800)
        def findCategory():
            user_count = UserDAO(db_name).count()
            dbCategory = DBCategory.XSMALL
            for category, ranges in CommonConfigProvider().getAppCategoryConfig().items():
                if user_count > ranges['min']:
                    if 'max' not in ranges:
                        dbCategory = DBCategory.fromStr(category)
                    else:
                        if user_count <= ranges['max']:
                            dbCategory = DBCategory.fromStr(category)
                        else:
                            continue
            if dbCategory:
                category_info = CommonConfigProvider().getAppCategoryConfig().get(str(dbCategory))
                dbCategory.setCount(user_count)
                dbCategory.setMin(category_info.get('min', 0))
                dbCategory.setMax(category_info.get('max', 0))
                dbCategory.setPipelineThrottle(category_info.get('pipeline_throttle', 0))
            return dbCategory

        return findCategory()

    @classmethod
    def encode_replace_str(cls, str_obj, replace_val='_'):
        return unicodedata.normalize('NFKD', str_obj).encode('ascii', 'replace').replace('?', replace_val)

    @classmethod
    def setup_logger(cls, base_path, log_file_name):
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(threadName)s-%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s')
        file_handler = logging.handlers.RotatingFileHandler(os.path.join(base_path, log_file_name),
                                                            maxBytes=2000000, backupCount=5)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    @classmethod
    def create_directories(cls, base_path, folders):
        if not os.path.exists(base_path):
            os.makedirs(base_path)
        for folder in folders:
            folder_path = os.path.join(base_path, folder)
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
