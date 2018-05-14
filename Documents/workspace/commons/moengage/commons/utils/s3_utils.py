import os
from threading import Thread

from moengage.commons.connections import ConnectionUtils
from moengage.commons.threadsafe import ThreadPoolExecutor
from moengage.package.utils import PackageUtils
from moengage.commons.utils import CommonUtils


class S3KeyDownloader(Thread):
    def __init__(self, s3_utils, s3_key_name, local_file_path, retry_count=5):
        super(S3KeyDownloader, self).__init__()
        self.s3_utils = s3_utils
        self.s3_key_name = s3_key_name
        self.local_file_path = local_file_path
        self.retry_count = retry_count
        self.error = None

    def download_file(self, file_handler):
        from moengage.commons.decorators.retry import Retry

        @Retry(Exception, max_retries=self.retry_count)
        def s3_download():
            self.s3_utils.getKeyContentsAsFile(self.s3_key_name, file_handler)

        return s3_download()

    def run(self):
        try:
            PackageUtils.ensure_path(self.local_file_path)
            with open(self.local_file_path, 'w') as s3_file:
                self.download_file(s3_file)
        except Exception, e:
            self.error = str(e)


class S3Utils(object):
    def __init__(self, bucket_name, profile_name=None):
        self.bucket_name = bucket_name
        self.s3_connection = ConnectionUtils.getS3Connection(profile_name=profile_name)
        self.bucket = self.s3_connection.get_bucket(self.bucket_name)

    def getAllKeys(self, **kwargs):
        return self.bucket.get_all_keys(**kwargs)

    def getKey(self, key_name, **kwargs):
        return self.bucket.get_key(key_name, **kwargs)

    def createKey(self, key_name, **kwargs):
        return self.bucket.new_key(key_name, **kwargs)

    def removeKey(self, key_name, **kwargs):
        return self.bucket.delete_key(key_name, **kwargs)

    def getOrCreateKey(self, key_name, **kwargs):
        return self.getKey(key_name, **kwargs) or self.createKey(key_name, **kwargs)

    def getKeyContentsAsString(self, key_name, **kwargs):
        return self.getKey(key_name, **kwargs).get_contents_as_string()

    def setKeyContentsFromString(self, key_name, string_contents, **kwargs):
        return self.getOrCreateKey(key_name, **kwargs).set_contents_from_string(string_contents)

    def setKeyContentsFromFile(self, key_name, file_handler, **kwargs):
        return self.getOrCreateKey(key_name, **kwargs).set_contents_from_file(file_handler)

    def getKeyContentsAsFile(self, key_name, file_handler, **kwargs):
        return self.getKey(key_name, **kwargs).get_contents_to_file(file_handler)

    def getKeysWithPrefix(self, prefix, strip_prefix=False):
        key_names = []
        for key in self.bucket.list(prefix=prefix):
            key_name = key.name.split(prefix)[1] if strip_prefix else key.name
            key_names.append(key_name)
        return key_names

    def __getLocalPathForKey(self, local_folder_path, key_name, prefix=None):
        if prefix:
            key_name = key_name.split(prefix)[1]
        return os.path.join(local_folder_path, key_name.strip('/'))

    def synchronizeRemoteFolder(self, remote_key_prefix, local_folder_path):
        keys_to_sync = self.getKeysWithPrefix(remote_key_prefix)
        executor_params = map(lambda key: [self, key, self.__getLocalPathForKey(local_folder_path, key,
                                                                                remote_key_prefix)],
                              keys_to_sync)
        executor = ThreadPoolExecutor(10, S3KeyDownloader, executor_params)
        for thread in executor.start():
            yield thread.s3_key_name, thread.error


class Boto3S3Utils(S3Utils):
    def __init__(self, bucket_name, profile_name=None):
        super(Boto3S3Utils, self).__init__(bucket_name)
        self.s3_connection = ConnectionUtils.getBoto3S3Connection(profile_name=profile_name)
        self.bucket = self.s3_connection.Bucket(bucket_name)
        self.s3_client = self.s3_connection.meta.client

    def getAllKeys(self, **kwargs):
        keys = self.s3_client.list_objects(Bucket=self.bucket_name)['Contents']
        return keys

    def getKey(self, key_name, **kwargs):
        results = self.s3_client.list_objects(Bucket=self.bucket_name, Prefix=key_name)
        return results['Contents'][0] if len(results['Contents']) > 0 else None

    def createKey(self, key_name, **kwargs):
        return self.s3_client.put_object(Bucket=self.bucket_name, Key=key_name)

    def removeKey(self, key_name, **kwargs):
        return self.s3_client.delete_object(Bucket=self.bucket_name, Key=key_name)

    def getOrCreateKey(self, key_name, **kwargs):
        return self.getKey(key_name, **kwargs) or self.createKey(key_name, **kwargs)

    def getKeyContentsAsString(self, key_name, **kwargs):
        return CommonUtils.to_json(self.getKey(key_name, **kwargs))

    def setKeyContentsFromFile(self, key_name, file_handler, **kwargs):
        with open(file_handler, 'rb') as data:
            return self.s3_connection.Object(self.bucket_name, key_name).put(Body=data)

    def getKeyContentsAsFile(self, key_name, file_handler, **kwargs):
        obj = self.getKey(key_name)
        with open(file_handler, 'wb') as f:
            return f.write(CommonUtils.to_json(obj))

    def setKeyContentsFromString(self, key_name, string_contents, **kwargs):
        return self.s3_connection.Object(self.bucket_name, key_name).put(Body=string_contents)

    def getKeysWithPrefix(self, prefix, strip_prefix=False):
        results = self.s3_client.list_objects(Bucket=self.bucket_name, Prefix=prefix)
        key_names = []
        for key in results['Contents']:
            key_name = key['Key'].split(prefix)[1] if strip_prefix else key['Key']
            key_names.append(key_name)
        return key_names

    def __getLocalPathForKey(self, local_folder_path, key_name, prefix=None):
        if prefix:
            key_name = key_name.split(prefix)[1]
        return os.path.join(local_folder_path, key_name.strip('/'))

    def synchronizeRemoteFolder(self, remote_key_prefix, local_folder_path):
        keys_to_sync = self.getKeysWithPrefix(remote_key_prefix)
        executor_params = map(lambda key: [self, key, self.__getLocalPathForKey(local_folder_path, key,
                                                                                remote_key_prefix)],
                              keys_to_sync)
        executor = ThreadPoolExecutor(10, S3KeyDownloader, executor_params)
        for thread in executor.start():
            yield thread.s3_key_name, thread.error
