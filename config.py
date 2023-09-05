from pathlib import Path

from yaml import safe_load


def get_config(config_path) -> dict:
    """
        Returns the YAML config file as a dict 
        Args:
            config_path: str - path to the config
        Return:
            config: dict - YAML config as a dict
    """
    config_path = Path(config_path)
    assert config_path.exists()
    assert config_path.suffix == '.yml'
    with open(config_path) as f:
        config = safe_load(f).copy()
    return config
