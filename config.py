from pathlib import Path

from yaml import safe_load


def get_config(config_path) -> dict:
    config_path = Path(config_path)
    assert config_path.exists()
    assert config_path.suffix == '.yml'
    with open(config_path) as f:
        config = safe_load(f).copy()
    return config
