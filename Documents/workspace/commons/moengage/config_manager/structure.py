import json
import os
from copy import deepcopy

from moengage.commons.utils import CommonUtils
from moengage.config_manager.exceptions import ConfigStructureException
from moengage.models.base import SimpleDocument


class BaseConfigFile(SimpleDocument):
    def __init__(self, **kwargs):
        self.aws_zone = None
        self.env = None
        self.config_dir = None
        self.file_name = None
        self.parsed_file_name = None
        self.file_type = None
        super(BaseConfigFile, self).__init__(**kwargs)
        if not self.config_dir:
            raise ConfigStructureException('Invalid config dir %r' % self.config_dir)
        if not self.file_name:
            raise ConfigStructureException('Invalid file_name %r' % self.file_name)
        if not self.parsed_file_name:
            self.parsed_file_name = self.file_name

    @classmethod
    def parse_config_file_content(cls, config_content):
        return config_content

    @classmethod
    def get_config_file_content(cls, config_dir, file_name):
        file_path = os.path.join(config_dir, file_name)

        # @MemCached(file_path, cache_type=CacheType.PERMANENT, skip_none=False)
        def load_config():
            config_content = ''
            with open(file_path, mode='r') as f:
                for line in f:
                    config_content += line
            return config_content

        return deepcopy(load_config())

    @classmethod
    def load_config_from_file(cls, config_dir, file_name, skip_if_file_not_exists=False):
        if os.path.exists(os.path.join(config_dir, file_name)):
            return cls.parse_config_file_content(cls.get_config_file_content(config_dir, file_name))
        else:
            if not skip_if_file_not_exists:
                raise ConfigStructureException('Config file %r not found in dir %r' % (file_name, config_dir))
            return {}

    def get_env_config_content(self):
        if self.env:
            env_file_name = self.parsed_file_name + '.' + self.env
            return self.load_config_from_file(self.config_dir, env_file_name, skip_if_file_not_exists=True)
        return {}

    def get_aws_zone_config_content(self):
        if self.aws_zone:
            aws_zone_file_name = self.parsed_file_name + '.' + self.aws_zone
            return self.load_config_from_file(self.config_dir, aws_zone_file_name, skip_if_file_not_exists=True)
        return {}

    def get_env_aws_zone_config_content(self):
        if self.env and self.aws_zone:
            env_aws_zone_file_name = self.parsed_file_name + '.' + self.env + '.' + self.aws_zone
            return self.load_config_from_file(self.config_dir, env_aws_zone_file_name, skip_if_file_not_exists=True)
        return {}

    def get_base_config(self):
        return self.load_config_from_file(self.config_dir, self.parsed_file_name, skip_if_file_not_exists=False)

    def load_config_content(self):
        base_config_content = self.get_base_config()

        env_aws_zone_config_content = self.get_env_aws_zone_config_content()
        if env_aws_zone_config_content:
            return env_aws_zone_config_content
        aws_zone_config_content = self.get_aws_zone_config_content()
        if aws_zone_config_content:
            return aws_zone_config_content
        env_config_content = self.get_env_config_content()
        if env_config_content:
            return env_config_content
        return base_config_content

    def load_config_contents_as_string(self):
        config_content = self.load_config_content()
        return config_content


class JSONConfigFile(BaseConfigFile):
    def __init__(self, **kwargs):
        super(JSONConfigFile, self).__init__(**kwargs)

    @classmethod
    def get_config_file_content(cls, config_dir, file_name):
        file_path = os.path.join(config_dir, file_name)

        # @MemCached(file_path, cache_type=CacheType.PERMANENT, skip_none=False)
        def load_config():
            with open(file_path, mode='r') as f:
                config_content = json.load(f)
            return config_content

        return deepcopy(load_config())

    def load_config_content(self):
        base_config_content = self.get_base_config()

        env_config_content = self.get_env_config_content()
        config_content = CommonUtils.deepMergeDictionaries(base_config_content, env_config_content)

        aws_zone_config_content = self.get_aws_zone_config_content()
        config_content = CommonUtils.deepMergeDictionaries(config_content, aws_zone_config_content)

        env_aws_zone_config_content = self.get_env_aws_zone_config_content()
        config_content = CommonUtils.deepMergeDictionaries(config_content, env_aws_zone_config_content)
        return config_content

    def load_config_contents_as_string(self):
        config_content = self.load_config_content()
        return json.dumps(config_content, indent=4)
