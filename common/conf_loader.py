from pyhocon import ConfigFactory

def load_config(path: str):
    return ConfigFactory.parse_file(path)
