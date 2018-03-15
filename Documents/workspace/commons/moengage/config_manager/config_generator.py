import argparse
import os
import sys

from moengage.commons.decorators import Retry
from moengage.commons.loggers import Treysor
from moengage.commons.utils.s3_utils import S3Utils
from moengage.config_manager.util import (FileTypeFactory, SUPPORTED_ENVS, SUPPORTED_AWS_ZONES,
                                          ConfigUtils, CONFIG_S3_BUCKET_NAME)
from moengage.package.alert import PackageOperationAlert, PackageOperationType, CodeAlertDelivery, CodeAlertLevel
from moengage.package.utils import PackageUtils


class ConfigGenerator(object):
    @classmethod
    def generate_unique_config_file_names(cls):
        package_path = os.path.join('moengage')
        for config_dir, folders, file_names in os.walk(package_path):
            unique_parsed_file_names = {}
            for file_name in filter(lambda x: not (x.endswith('.py') or x.endswith('.pyc')), file_names):
                config_obj = FileTypeFactory.get_file_structure_object(config_dir=config_dir, file_name=file_name)
                unique_parsed_file_names[config_obj.parsed_file_name] = config_obj
            for parsed_file_name in unique_parsed_file_names:
                yield config_dir, parsed_file_name

    @classmethod
    def write_config_file_to_local_path(cls, config_dir, parsed_file_name):
        local_path = ConfigUtils.get_local_file_path_for_file(os.path.join(config_dir, parsed_file_name.strip('/')))
        PackageUtils.ensure_path(local_path)
        env = PackageUtils.getExecutionEnv()
        aws_zone = PackageUtils.getInstanceMeta()['region']
        file_name = parsed_file_name + '.' + env + '.' + aws_zone
        config_obj = FileTypeFactory.get_file_structure_object(config_dir=config_dir, file_name=file_name)
        print 'Writing config for env %r aws %r %r' % (env, aws_zone, local_path)

        with open(local_path, 'w') as local_file:
            local_file.write(config_obj.load_config_contents_as_string())

    @classmethod
    @Retry(Exception, max_retries=3)
    def upload_file_to_s3(cls, s3_key, file_content):
        s3_utils = S3Utils(CONFIG_S3_BUCKET_NAME, profile_name='devops')
        s3_utils.setKeyContentsFromString(s3_key, file_content)

    @classmethod
    def write_config_file_to_s3(cls, config_dir, parsed_file_name, package_name):
        for env in SUPPORTED_ENVS:
            for aws_zone in SUPPORTED_AWS_ZONES:
                file_name = parsed_file_name + '.' + env + '.' + aws_zone
                config_obj = FileTypeFactory.get_file_structure_object(config_dir=config_dir, file_name=file_name)
                package_version = \
                    PackageUtils.getPackageInstalledVersion(package_name + '-' + PackageUtils.getPackageEnv())
                config_s3_key_prefix = ConfigUtils.get_remote_key_prefix(package_name=package_name,
                                                                         package_version=package_version,
                                                                         environment=env, region_name=aws_zone)
                config_s3_key = os.path.join(config_s3_key_prefix, config_dir, parsed_file_name).strip('/')
                print 'Uploading config to s3 key %r env %r aws %r' % (config_s3_key, env, aws_zone)
                cls.upload_file_to_s3(config_s3_key, config_obj.load_config_contents_as_string())

    @classmethod
    def generate_config(cls, package_name, upload_to_s3=False):
        try:
            for config_dir, parsed_file_name in cls.generate_unique_config_file_names():
                print config_dir, parsed_file_name
                cls.write_config_file_to_local_path(config_dir, parsed_file_name)
                if upload_to_s3:
                    cls.write_config_file_to_s3(config_dir, parsed_file_name, package_name)
        except Exception, e:
            alert_dict = dict(package_name=package_name, upload_to_s3=upload_to_s3,
                              error_reason=repr(e))
            Treysor().exception(log_tag='upload_config_failed', **alert_dict)
            PackageOperationAlert(PackageOperationType.CONFIG_DOWNLOAD_FAILED,
                                  alert_level=CodeAlertLevel.ERROR,
                                  alert_delivery=CodeAlertDelivery.SLACK).send(**alert_dict)
            raise


def main():
    parser = argparse.ArgumentParser(description='Config generator')

    parser.add_argument('--package-name', action="store", dest='package_name', type=str)
    parser.add_argument('--upload-to-s3', action="store_true", dest='upload_to_s3', default=False)

    args = parser.parse_args(sys.argv[1:])
    ConfigGenerator.generate_config(args.package_name, upload_to_s3=args.upload_to_s3)
