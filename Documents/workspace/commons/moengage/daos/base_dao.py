import copy
from enum import IntEnum
from moengage.commons.decorators.retry import Retry

from moengage.commons.exceptions import MONGO_NETWORK_ERRORS
from moengage.commons.singleton import SingletonMetaClass
from pymongo.errors import PyMongoError
from pymongo.read_preferences import ReadPreference

from moengage.commons import ConnectionUtils
from moengage.commons.loggers.treysor import Treysor
from moengage.models import SimpleDocument


class DAOException(Exception):
    pass


class InvalidMongoQueryException(DAOException):
    pass


class MongoOperation(IntEnum):
    COUNT = 1
    FIND = 2
    FIND_ONE = 3
    FIND_AND_MODIFY = 4
    DISTINCT = 5
    INSERT = 6
    REMOVE = 7
    UPDATE = 8
    SAVE = 9
    AGGREGATE = 10


class DAOIndexCreator(object):
    __metaclass__ = SingletonMetaClass

    def __init__(self):
        self.dao_indexes = {}

    def index_creation_needed(self, dao):
        dao_name = dao.__class__.__name__
        index_exists = self.dao_indexes.setdefault(dao_name, False)
        return not index_exists

    def create(self, dao, indexes, db_connection):
        index_creation_needed = self.index_creation_needed(dao)
        if index_creation_needed:
            dao_name = dao.__class__.__name__
            indexes_created = True
            for index in indexes:
                indexes_created = bool(self.__ensure_index(index, db_connection, dao_name=dao_name))
            self.dao_indexes[dao_name] = indexes_created
        return index_creation_needed

    def __create_index(self, index, db_connection):
        index_definition = index.to_dict()
        fields = index_definition.pop('fields')
        try:
            Treysor().info(log_tag='create_mongo_index', status='started', index_name=index.name,
                           connection=str(db_connection))
            index_name = db_connection.create_index(fields, **index_definition)
            Treysor().info(log_tag='create_mongo_index', status='finished', index_name=index_name,
                           connection=str(db_connection))
            return bool(index_name)
        except PyMongoError:
            Treysor().exception(log_tag='create_mongo_index', status='failed', index_name=index.name,
                                connection=str(db_connection))
            return False

    def __ensure_index(self, index, db_connection, dao_name=None):
        def create_index_if_not_exists():
            try:
                Treysor().info(log_tag='fetch_index_info', connection=str(db_connection), dao_name=dao_name)
                index_info = db_connection.index_information()
            except PyMongoError:
                Treysor().warning(log_tag='fetch_index_info', status='failed', connection=str(db_connection),
                                  dao_name=dao_name)
                return False
            if index.name in index_info:
                return True
            return self.__create_index(index, db_connection)

        return create_index_if_not_exists()


class BaseDAO(object):
    def __init__(self, db_name, collection_name, model_class=dict, infra_type=None, indexes=None, ensure_indexes=False):
        self.__db_name = db_name
        self.__collection_name = collection_name
        self.__model_class = model_class
        self.__infra_type = infra_type
        self.__indexes = indexes
        if ensure_indexes and self.__indexes and DAOIndexCreator().index_creation_needed(self):
            DAOIndexCreator().create(self, self.__indexes, self.__get_db_connection())

    def __get_db_connection(self, read_preference=ReadPreference.PRIMARY_PREFERRED):
        # Get database connection
        if self.__infra_type:
            connection = ConnectionUtils.getMongoConnectionForInfraType(infra_type=self.__infra_type,
                                                                        read_preference=read_preference)
        else:
            connection = ConnectionUtils.getMongoConnectionForDBName(self.__db_name, read_preference=read_preference)
        database = connection[self.__db_name]
        collection = database[self.__collection_name]
        return collection

    def __perform_operation(self, collection, operation, query, **kwargs):
        # query_params - Check for projection in case of reads
        # query_params - If projection not found, check for update spec to execute update queries
        query_params = [query]
        project_or_update = kwargs.get('projection', kwargs.get('update_spec'))
        if project_or_update:
            query_params.append(project_or_update)

        query_kwargs = kwargs.get('query_args', {})
        # Hint to use any specific indexes

        # Execute operation
        if operation == MongoOperation.COUNT:
            return collection.find(*query_params, **query_kwargs).count()
        elif operation == MongoOperation.REMOVE:
            return collection.remove(*query_params, **query_kwargs)
        elif operation == MongoOperation.UPDATE:
            return collection.update(*query_params, **query_kwargs)
        elif operation == MongoOperation.SAVE:
            return collection.save(*query_params, **query_kwargs)
        elif operation == MongoOperation.INSERT:
            return collection.insert(*query_params, **query_kwargs)
        elif operation == MongoOperation.FIND_AND_MODIFY:
            return collection.find_and_modify(*query_params, **query_kwargs)
        elif operation == MongoOperation.DISTINCT:
            return collection.find(*query_params, **query_kwargs).distinct(kwargs.get('distinct', ''))
        elif operation == MongoOperation.FIND_ONE:
            return collection.find_one(*query_params, **query_kwargs)
        elif operation == MongoOperation.FIND:
            sort_params = kwargs.get('sort')
            limit_size = kwargs.get('limit')
            hint = kwargs.get('hint')
            cursor = collection.find(*query_params, **query_kwargs)
            if hint:
                cursor = cursor.hint(hint)
            if sort_params:
                cursor = cursor.sort(sort_params)
            if limit_size:
                cursor = cursor.limit(limit_size)
            return cursor
        elif operation == MongoOperation.AGGREGATE:
            return collection.aggregate(*query_params, **kwargs)

    def __parseResult(self, operation, result):
        def result_generator(cursor):
            if cursor:
                for document in cursor:
                    yield self.__model_class(**document)

        if operation == MongoOperation.FIND_ONE and isinstance(result, dict):
            return self.__model_class(**result)
        elif operation == MongoOperation.FIND_AND_MODIFY:
            if isinstance(result, dict):
                return self.__model_class(**result)
            else:
                return result_generator(result)
        elif operation == MongoOperation.FIND:
            return result_generator(result)
        else:
            return result

    def __execute(self, operation, query, **kwargs):
        if not query:
            query = {}
        connection_params = {}
        if kwargs.get('read_preference'):
            connection_params['read_preference'] = kwargs['read_preference']
        kwargs.pop('ensure_indexes', False)

        @Retry(MONGO_NETWORK_ERRORS, max_retries=5, after=30)
        def execute_mongo_operation():
            db_connection = self.__get_db_connection(**connection_params)
            cursor = self.__perform_operation(db_connection, operation, query, **kwargs)
            return self.__parseResult(operation, cursor)

        return execute_mongo_operation()

    def findOne(self, query=None, **kwargs):
        operation = MongoOperation.FIND_ONE
        return self.__execute(operation, query, **kwargs)

    def findById(self, obj_id, **kwargs):
        return self.findOne({'_id': obj_id}, **kwargs)

    def find(self, query=None, **kwargs):
        operation = MongoOperation.FIND
        return self.__execute(operation, query, **kwargs)

    def findAll(self, **kwargs):
        return self.find({}, **kwargs)

    def count(self, query=None, **kwargs):
        operation = MongoOperation.COUNT
        return self.__execute(operation, query, **kwargs)

    def distinct(self, field, query=None, **kwargs):
        operation = MongoOperation.DISTINCT
        kwargs['distinct'] = str(field)
        return self.__execute(operation, query, **kwargs)

    def remove(self, query):
        operation = MongoOperation.REMOVE
        return self.__execute(operation, query)

    def removeById(self, obj_id):
        return self.remove({'_id': obj_id})

    def aggregate(self, query=None, **kwargs):
        """
        :param query: query having pipeline operators
        :type query: list
        :param kwargs: options to the query
        :return: dict of result
        Example: db.collection.aggregate(pipeline, options)
        """
        operation = MongoOperation.AGGREGATE
        return self.__execute(operation, query, **kwargs)

    def findAndModify(self, query=None, set_spec=None, unset_spec=None, inc_spec=None, push_spec=None, **kwargs):
        operation = MongoOperation.FIND_AND_MODIFY
        query_args = dict()
        query_args['upsert'] = kwargs.pop('upsert', False)
        query_args['fields'] = kwargs.pop('fields', {})
        query_args['new'] = kwargs.pop('new', False)
        kwargs['query_args'] = query_args
        update_spec = dict()
        if set_spec:
            update_spec['$set'] = set_spec
        if unset_spec:
            update_spec['$unset'] = unset_spec
        if inc_spec:
            update_spec['$inc'] = inc_spec
        if push_spec:
            def getEachSpec(values):
                is_values_list = bool(isinstance(values, list) or isinstance(values, set))
                values_list = [values] if not is_values_list else values
                return {'$each': values_list}

            each_push_spec = {k: getEachSpec(v) for k, v in push_spec.items()}
            update_spec['$push'] = each_push_spec
        if update_spec:
            kwargs['update_spec'] = update_spec
        else:
            return self.find(query, **kwargs)
        return self.__execute(operation, query, **kwargs)

    def findByIdAndModify(self, obj_id, set_spec=None, unset_spec=None, inc_spec=None, **kwargs):
        return self.findAndModify({'_id': obj_id}, set_spec, unset_spec, inc_spec, **kwargs)

    def save(self, obj_to_save, **kwargs):
        operation = MongoOperation.SAVE
        if isinstance(obj_to_save, SimpleDocument):
            obj_to_save = obj_to_save.to_dict()
        return self.__execute(operation, obj_to_save, **kwargs)

    def insert(self, obj_to_insert, **kwargs):
        operation = MongoOperation.INSERT
        if isinstance(obj_to_insert, SimpleDocument):
            obj_to_insert = obj_to_insert.to_dict()
        return self.__execute(operation, obj_to_insert, **kwargs)

    def copy(self, **kwargs):
        clone = copy.deepcopy(self)
        for k, v in kwargs.items():
            clone.__setattr__(k, v)
        return clone

    @property
    def database_name(self):
        return self.__db_name

    @property
    def collection_name(self):
        return self.__collection_name

    @property
    def infra_type(self):
        return self.__infra_type

    @property
    def model_class(self):
        return self.__model_class

    @model_class.setter
    def model_class(self, model_class):
        self.__model_class = model_class


class SingleClientCollectionBaseDAO(BaseDAO):
    def __init__(self, client_name, database_name, collection_name, model_class=dict, infra_type=None, indexes=None,
                 ensure_indexes=False):
        super(SingleClientCollectionBaseDAO, self).__init__(database_name, collection_name, model_class=model_class,
                                                            infra_type=infra_type, indexes=indexes,
                                                            ensure_indexes=ensure_indexes)
        self.db_name = client_name

    def _getQuery(self, query, **kwargs):
        if not query:
            query = {}
        if kwargs.pop("add_client_filter", True):
            query['db_name'] = self.db_name
        return query

    def find(self, query=None, **kwargs):
        return super(SingleClientCollectionBaseDAO, self).find(self._getQuery(query, **kwargs), **kwargs)

    def distinct(self, field, query=None, **kwargs):
        return super(SingleClientCollectionBaseDAO, self).distinct(field, self._getQuery(query, **kwargs), **kwargs)

    def remove(self, query, **kwargs):
        return super(SingleClientCollectionBaseDAO, self).remove(self._getQuery(query, **kwargs))

    def findOne(self, query=None, **kwargs):
        return super(SingleClientCollectionBaseDAO, self).findOne(self._getQuery(query, **kwargs), **kwargs)

    def count(self, query=None, **kwargs):
        return super(SingleClientCollectionBaseDAO, self).count(self._getQuery(query, **kwargs), **kwargs)

    def findAndModify(self, query=None, set_spec=None, unset_spec=None, inc_spec=None, **kwargs):
        return super(SingleClientCollectionBaseDAO, self).findAndModify(query=self._getQuery(query, **kwargs),
                                                                        set_spec=set_spec, unset_spec=unset_spec,
                                                                        inc_spec=inc_spec, **kwargs)
