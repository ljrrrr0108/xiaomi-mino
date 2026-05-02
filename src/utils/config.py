"""Configuration management utility."""

import os
from typing import Any

import yaml


def load_config(config_path: str = None) -> dict:
    """Load YAML configuration file.

    Args:
        config_path: Path to the YAML config file. Defaults to
            ``config/config.yaml`` relative to the project root.

    Returns:
        Configuration dictionary.

    Raises:
        FileNotFoundError: If the configuration file does not exist.
    """
    if config_path is None:
        # Default: config/config.yaml relative to repo root
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        config_path = os.path.join(base_dir, "config", "config.yaml")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    return config or {}


def get_nested(config: dict, *keys: str, default: Any = None) -> Any:
    """Safely retrieve a nested value from a configuration dictionary.

    Args:
        config: The configuration dictionary.
        *keys: Sequence of keys forming the path to the desired value.
        default: Value returned when a key is missing.

    Returns:
        The value at the specified path, or *default*.
    """
    current = config
    for key in keys:
        if not isinstance(current, dict):
            return default
        if key not in current:
            return default
        current = current[key]
    return current
