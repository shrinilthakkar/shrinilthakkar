import os
import threading

import paramiko
from paramiko.ssh_exception import NoValidConnectionsError, SSHException

from moengage.commons.decorators.cached import MemCached
from moengage.commons.loggers.context_logger import ContextLogger
from moengage.commons.singleton import SingletonMetaClass


class ParamikoSshHelper(ContextLogger):
    key_filename = '/var/keys/pipeline_consumer.pem'
    __metaclass__ = SingletonMetaClass

    def __init__(self):
        self.ssh_connections = {}
        self.connection_lock = threading.Lock()

    def get_conn_key(self, host_ip, username):
        return MemCached.createKey(host_ip, username)

    def _create_ssh_connection(self, host_ip, key_filename=None, username='ubuntu', timeout=30):
        key_filename = key_filename or self.key_filename
        conn_key = self.get_conn_key(host_ip, username)
        with self.connection_lock:
            self.ensure_ssh_key()
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname=host_ip, username=username, key_filename=key_filename, timeout=timeout)
            self.ssh_connections[conn_key] = ssh
            self.logger.info("created ssh connection for key: %r" % conn_key)
            return ssh

    def _refresh_ssh_connection(self, host_ip, key_filename=None, username='ubuntu', timeout=30):
        ssh = self.get_connection(host_ip, key_filename=key_filename, username=username, timeout=timeout)
        try:
            ssh.close()
        except Exception, e:
            self.logger.exception("close_connection failed due to: %r" % e)
        self._create_ssh_connection(host_ip, key_filename=key_filename, username=username, timeout=timeout)

    def get_connection(self, host_ip, key_filename=None, username='ubuntu', timeout=30):
        key_filename = key_filename or self.key_filename
        conn_key = self.get_conn_key(host_ip, username)
        try:
            return self.ssh_connections[conn_key]
        except KeyError:
            return self._create_ssh_connection(host_ip, key_filename=key_filename,
                                               username=username, timeout=timeout)

    def ensure_ssh_key(self):
        path_exists = os.path.exists(self.key_filename)
        if not path_exists:
            self.logger.error('key file missing %r' % self.key_filename)
            raise OSError('key file missing %r' % self.key_filename)

    def run_command(self, host_ip, command, key_filename=None, username='ubuntu', timeout=30, retry_on_ssh_errors=True):
        try:
            ssh = self.get_connection(host_ip, key_filename=key_filename, username=username, timeout=timeout)
            _, stdout, stderr = ssh.exec_command(command, timeout=timeout)
            out = stdout.read()
            err = stderr.read()
            return out, err
        except (NoValidConnectionsError, SSHException, EOFError), e:
            self.logger.info("run_command failed due to: %r" % e)
            if retry_on_ssh_errors:
                self._refresh_ssh_connection(host_ip, key_filename=key_filename, username=username, timeout=timeout)
                return self.run_command(host_ip, command, key_filename=key_filename, username=username,
                                        timeout=timeout, retry_on_ssh_errors=False)
            raise
