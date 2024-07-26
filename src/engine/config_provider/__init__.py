from .yml_config_provider import YmlConfigProvider


def get_default_config_provider():
    try:
        config = YmlConfigProvider("./config.yml")
        return config
    except Exception as e:
        print(f"Error loading config: {e}")
