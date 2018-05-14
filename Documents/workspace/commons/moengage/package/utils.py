import json
import os
import pkg_resources
import requests
import sys
from threading import Lock


class PackageUtils(object):
    INSTANCE_META = None
    EXECUTION_ENV = None
    PACKAGE_ENV = None
    DEFAULT_INSTANCE_META = {
        "devpayProductCodes": None,
        "availabilityZone": "us-east-1c",
        "instanceId": "i-dummy",
        "region": "us-east-1",
        "marketplaceProductCodes": None,
        "pendingTime": "2017-11-17T11:41:09Z",
        "privateIp": "127.0.0.1",
        "version": "2017-09-30",
        "architecture": "x86_64",
        "billingProducts": None,
        "kernelId": None,
        "ramdiskId": None,
        "imageId": "ami-dummy",
        "instanceType": "localhost",
        "accountId": "dummy_account_id"
    }
    THREAD_LOCK = Lock()

    @classmethod
    def getInstanceMeta(cls):
        if not PackageUtils.INSTANCE_META:
            try:
                response = requests.get('http://instance-data/latest/dynamic/instance-identity/document')
                cls.INSTANCE_META = json.loads(response.text)
            except Exception:
                return cls.DEFAULT_INSTANCE_META
        return cls.INSTANCE_META

    @classmethod
    def getCurrentEC2Instance(cls):
        from moengage.commons.connections import ConnectionUtils
        ec2_client = ConnectionUtils.getEC2Connection()
        instances = ec2_client.instances.filter(InstanceIds=[cls.getInstanceMeta()['instanceId']])
        instances = map(lambda x: x, instances)
        return instances[0]

    @classmethod
    def getExecutionEnv(cls):
        if not cls.EXECUTION_ENV:
            try:
                env = open(os.path.join(cls.getConfigLocalBaseFolder(), 'moe_env')).read().strip() or 'prod'
            except IOError:
                env = os.environ.get('MOE_DEPLOYMENT_ENV') or 'prod'
            cls.EXECUTION_ENV = env
        return cls.EXECUTION_ENV

    @classmethod
    def getPackageEnv(cls):
        if not cls.PACKAGE_ENV:
            try:
                env = open(os.path.join(cls.getConfigLocalBaseFolder(), 'moe_package_env')).read().strip() or 'prod'
            except IOError:
                env = os.environ.get('MOE_PACKAGE_ENV') or 'prod'
            cls.PACKAGE_ENV = env
        return cls.PACKAGE_ENV

    @classmethod
    def getPackageInstalledVersion(cls, package_name):
        return pkg_resources.get_distribution(package_name).version

    @classmethod
    def getPackageInfo(cls, package_name):
        version = cls.getPackageInstalledVersion(package_name)
        return {
            'package_name': package_name,
            'package_version': version
        }

    @classmethod
    def ensure_path(cls, file_path):
        with cls.THREAD_LOCK:
            if not os.path.exists(os.path.dirname(file_path)):
                os.makedirs(os.path.dirname(file_path))

    @classmethod
    def isVirtualEnv(cls):
        return hasattr(sys, 'real_prefix')

    @classmethod
    def isLambdaEnv(cls):
        return os.environ.get("AWS_EXECUTION_ENV")

    @classmethod
    def getConfigLocalBaseFolder(cls):
        if cls.isLambdaEnv():
            return '/var/task'
        return sys.prefix if cls.isVirtualEnv() else '/etc'
