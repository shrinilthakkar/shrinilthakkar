import os

from moengage.package.utils import PackageUtils

SUPPORTED_AWS_ZONES = ('us-east-1', 'eu-central-1')
SUPPORTED_ENVS = ('prod', 'pp', 'dev')
FILE_TYPES = ('json',)

CONFIG_S3_BUCKET_NAME = 'moe-configs-master'
CONFIG_DATA_FOLDER = 'package-data'
CONFIG_S3_BASE_PATH = CONFIG_DATA_FOLDER

CONFIG_LOCAL_BASE_PATH = os.path.join(PackageUtils.getConfigLocalBaseFolder(), CONFIG_DATA_FOLDER)


class ConfigUtils(object):
    @classmethod
    def get_remote_key_prefix(cls, package_name, package_version,
                              region_name=None, environment=None):
        region = region_name or PackageUtils.getInstanceMeta()['region']
        environment = environment or PackageUtils.getExecutionEnv()
        path_structure = "{base_path}/{environment}/{region}/{package_name}/{package_version}/"
        path_args = {
            'base_path': CONFIG_S3_BASE_PATH,
            'environment': environment,
            'region': region,
            'package_name': package_name,
            'package_version': package_version
        }
        return path_structure.format(**path_args)

    @classmethod
    def get_local_config_folder(cls, environment=None):
        environment = environment or PackageUtils.getExecutionEnv()
        return "{base_path}/{environment}".format(base_path=CONFIG_LOCAL_BASE_PATH, environment=environment)

    @classmethod
    def get_path_for_file(cls, base_path, file_path):
        return os.path.join(base_path, file_path.strip('/'))

    @classmethod
    def get_local_file_path_for_file(cls, file_path, environment=None):
        return cls.get_path_for_file(cls.get_local_config_folder(environment=environment), file_path)


class FileTypeFactory(object):
    @classmethod
    def get_file_structure_object(cls, file_name, config_dir):
        from moengage.config_manager.structure import JSONConfigFile, BaseConfigFile

        aws_zone, parsed_file_name = cls.extract_aws_zone_from_file_name(file_name)
        env, parsed_file_name = cls.extract_env_from_file_name(parsed_file_name)
        file_type, parsed_file_name = cls.extract_file_type_from_file_name(parsed_file_name)
        file_structure_class = {'json': JSONConfigFile}.get(file_type, BaseConfigFile)
        return file_structure_class(file_type=file_type,
                                    aws_zone=aws_zone,
                                    env=env,
                                    config_dir=config_dir,
                                    parsed_file_name=parsed_file_name,
                                    file_name=file_name)

    @classmethod
    def extract_env_from_file_name(cls, file_name):
        matched_env = None
        for env in SUPPORTED_ENVS:
            if file_name.endswith(env):
                matched_env = env
                file_name = file_name.rsplit('.', 1)[0]
                break
        return matched_env, file_name

    @classmethod
    def extract_aws_zone_from_file_name(cls, file_name):
        matched_aws_zone = None
        for aws_zone in SUPPORTED_AWS_ZONES:
            if file_name.endswith(aws_zone):
                matched_aws_zone = aws_zone
                file_name = file_name.rsplit('.', 1)[0]
                break
        return matched_aws_zone, file_name

    @classmethod
    def extract_file_type_from_file_name(cls, file_name):
        matched_file_type = None
        for file_type in FILE_TYPES:
            if file_name.endswith(file_type):
                matched_file_type = file_type
                break
        return matched_file_type, file_name
