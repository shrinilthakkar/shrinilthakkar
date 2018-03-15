import grp
import logging
# logging init does not have handlers imported
import logging.handlers
import os
import pwd

import pymongo
from enum import Enum

from moengage.commons.connections import ConnectionUtils
from moengage.commons.loggers.context_logger import ContextLogger
from moengage.commons.utils.ssh_utils import ParamikoSshHelper


class OperationType(Enum):
    INSERT = 1
    DELETE = 2
    UPDATE = 3

    def __str__(self):
        return {
            OperationType.INSERT: "i",
            OperationType.DELETE: "d",
            OperationType.UPDATE: "u"
        }.get(self, "")

    def getDocumentId(self, o, o2=None):
        return {
            OperationType.UPDATE: o2.get('_id') if o2 else o.get('_id')
        }.get(self, o.get('_id'))

    @staticmethod
    def fromStr(value):
        return {
            "i": OperationType.INSERT,
            "d": OperationType.DELETE,
            "u": OperationType.UPDATE
        }.get(value)


class OplogUtils(object):
    @staticmethod
    def setup_logging(logging_config):
        root_logger = logging.getLogger()
        formatter = logging.Formatter(
            "%(threadName)s-%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s")
        log_levels = [
            logging.ERROR,
            logging.WARNING,
            logging.INFO,
            logging.DEBUG
        ]
        log_level = log_levels[logging_config['log_level']]
        root_logger.setLevel(log_level)
        log_dir = os.path.dirname(logging_config['filename'])
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            os.chmod(log_dir, 0o764)
            os.chown(log_dir, pwd.getpwnam("ubuntu").pw_uid, grp.getgrnam("syslog").gr_gid)
        log_out = logging.handlers.TimedRotatingFileHandler(
            logging_config['filename'],
            when=logging_config['when'],
            interval=logging_config['interval'],
            backupCount=logging_config['backupCount']
        )
        log_out.setLevel(log_level)
        log_out.setFormatter(formatter)
        root_logger.addHandler(log_out)

    @classmethod
    def execute_remote_command(cls, host_ip, command, key_filename, username='ubuntu', timeout=30):
        logger = ContextLogger().logger
        logger.debug("execute_remote_command command %r %r %r" % (command, host_ip, key_filename))
        out, err = ParamikoSshHelper().run_command(host_ip, command=command, key_filename=key_filename,
                                                   username=username, timeout=timeout)
        logger.debug("execute_remote_command command %r out %r" % (command, out))
        return out, err

    @classmethod
    def get_shard_config(cls, infra_type):
        db_connection_primary = ConnectionUtils.getMongoConnectionForInfraType(infra_type,
                                                                               pymongo.ReadPreference.PRIMARY)
        conn_type = None

        try:
            db_connection_primary.admin.command("isdbgrid")
        except pymongo.errors.OperationFailure:
            conn_type = "REPLSET"
        shard_config = {}
        if conn_type == "REPLSET":
            repl_set_status = db_connection_primary.admin.command({'replSetGetStatus': 1})
            repl_set_name = repl_set_status['set']
            member_ips = ','.join(map(lambda x: x['name'], repl_set_status['members']))
            shard_config[repl_set_name] = {
                'replica_set': repl_set_name,
                'hosts': member_ips
            }
        else:
            for shard_doc in db_connection_primary['config']['shards'].find():
                shard_id = shard_doc['_id']
                repl_set, hosts = shard_doc['host'].split('/')
                shard_config[shard_id] = {
                    'replica_set': repl_set,
                    'hosts': hosts
                }
        return shard_config
