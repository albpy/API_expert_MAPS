from collections import namedtuple
import yaml

config_path_local = "core/config/config.local.yaml"
config_path_local = "core/config/config.yaml"

class SimpleConfig:
    """Manage a simple YAML config file"""

    def __new__(cls, path: str = "core/config/config.yaml"):
        if not hasattr(cls, "instance"):
            cls.instance = super(SimpleConfig, cls).__new__(cls)
            cls.instance.init(path)
        return cls.instance

    def init(self, path: str):
        """Load the config"""
        with open(path, encoding="utf-8") as file:
            self.config = yaml.load(file, Loader=yaml.FullLoader)

    def get(self, key: str):
        """Get top-level config value"""
        return self.config[key]

    def config_obj(self):
        """Create an Object.Properties version of the config"""
        return namedtuple("ConfigObj", self.config.keys())(*self.config.values())


api_settings_config = SimpleConfig(path="core/config/config.yaml").config_obj()