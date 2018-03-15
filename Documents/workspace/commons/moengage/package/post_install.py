import json
from distutils.errors import DistutilsFileError

from moengage.config_manager.util import ConfigUtils, CONFIG_S3_BUCKET_NAME
from moengage.package.utils import PackageUtils


class PostInstall(object):
    def __init__(self, install_obj):
        self.package_installed_name = install_obj.distribution.get_name()
        self.package_name, self.env = self.package_installed_name.rsplit('-', 1)
        self.is_pip_install = bool(install_obj.single_version_externally_managed)
        self.package_info = self.get_package_info()
        self.instance_meta = self.get_instance_meta()
        self.local_folder = ConfigUtils.get_local_config_folder()

    def get_package_info(self):
        execution_env = PackageUtils.getExecutionEnv()
        package_env = PackageUtils.getPackageEnv()
        package_installed_version = PackageUtils.getPackageInstalledVersion(self.package_installed_name)
        return dict(package_name=self.package_name, package_version=package_installed_version,
                    execution_env=execution_env, package_env=package_env)

    def copy_configs_from_s3_to_local_path(self):
        remote_key_prefix = ConfigUtils.get_remote_key_prefix(package_name=self.package_info['package_name'],
                                                              package_version=self.package_info['package_version'])
        config_sync = {
            'sync_source': 's3',
            'bucket': CONFIG_S3_BUCKET_NAME,
            'key_prefix': remote_key_prefix,
            'local_folder': self.local_folder
        }
        print "CONFIG SYNC START: " + json.dumps(config_sync, indent=4)

        from moengage.commons.utils.s3_utils import S3Utils
        s3_utils = S3Utils(CONFIG_S3_BUCKET_NAME, profile_name='devops')
        errored_keys = []
        for key_name, error in s3_utils.synchronizeRemoteFolder(remote_key_prefix, self.local_folder):
            if error:
                errored_keys.append(key_name)
                print "Failed to download remote key: %r due to exception: %r" % (key_name, error)
            else:
                print "Successfully downloaded remote key: %r" % key_name
        if errored_keys:
            if self.env == 'prod':
                alert_dict = dict(instance_meta=self.instance_meta, package_meta=self.package_info,
                                  config_sync_meta=config_sync, errored_keys=errored_keys)
                from moengage.package.alert import PackageOperationAlert, PackageOperationType, CodeAlertDelivery, \
                    CodeAlertLevel
                PackageOperationAlert(PackageOperationType.CONFIG_DOWNLOAD_FAILED,
                                      alert_level=CodeAlertLevel.ERROR,
                                      alert_delivery=CodeAlertDelivery.SLACK).send(**alert_dict)

            raise DistutilsFileError("S3 keys failed to download: %r" % errored_keys)

    def generate_configs_to_local_path(self):
        from moengage.config_manager.config_generator import ConfigGenerator

        config_sync = {
            'sync_source': 'local',
            'local_folder': self.local_folder
        }
        print "CONFIG SYNC START: " + json.dumps(config_sync, indent=4)
        ConfigGenerator.generate_config(package_name=self.package_name, upload_to_s3=False)

    @classmethod
    def get_instance_meta(cls):
        return PackageUtils.getInstanceMeta()

    def run(self):
        print "Package Post Install: %r" % self.package_installed_name

        print "INSTANCE META: " + json.dumps(self.instance_meta, indent=4)

        print "PACKAGE META: " + json.dumps(self.package_info, indent=4)

        if self.is_pip_install:
            self.copy_configs_from_s3_to_local_path()
        else:
            self.generate_configs_to_local_path()
