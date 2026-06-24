"""Config loading and path resolution. Config paths are relative to the project root."""
from __future__ import annotations

import os
from pathlib import Path

import yaml

# Project root = two levels up from this file:  src/utils/config.py -> project/
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"


def load_config(config_path: str | os.PathLike | None = None) -> dict:
    """Read config.yaml into a dict."""
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def resolve(path_str: str) -> Path:
    """Resolve a config path (relative to project root) to an absolute Path."""
    p = Path(path_str)
    return p if p.is_absolute() else (PROJECT_ROOT / p).resolve()


def zone_path(config: dict, zone: str) -> Path:
    """Absolute path of a zone: 'landing' | 'formatted' | 'exploitation'."""
    return resolve(config["zones"][zone])


def source_path(config: dict, *parts: str) -> Path:
    """Absolute path inside the provided source datasets folder."""
    return resolve(config["source_datasets_dir"]).joinpath(*parts)
