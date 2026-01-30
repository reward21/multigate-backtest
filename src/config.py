from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Optional
import os

import yaml


# Repo root is parent of /src
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = ROOT / "config.yaml"


@dataclass(frozen=True)
class Paths:
    root: Path
    db_path: Path
    data_path: Path
    gates_dir: Path
    artifacts_dir: Path


@dataclass(frozen=True)
class AlpacaEnv:
    key_id: Optional[str]
    secret_key: Optional[str]


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"config.yaml not found: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise TypeError(f"config.yaml must parse to a dict, got {type(data)}")
    return data


@lru_cache(maxsize=4)
def get_config(config_path: str | Path | None = None) -> Dict[str, Any]:
    """
    Loads config.yaml once and caches it.
    Call get_config.cache_clear() if you want to reload at runtime.
    """
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    cfg = _load_yaml(path)

    # Ensure expected top-level sections exist
    cfg.setdefault("paths", {})
    cfg.setdefault("runtime", {})
    cfg.setdefault("gates", {})
    cfg.setdefault("api", {})
    cfg.setdefault("data_features", {})

    return cfg


def paths_from_config(cfg: Optional[Dict[str, Any]] = None) -> Paths:
    cfg = cfg or get_config()
    p = cfg.get("paths", {}) or {}

    db_path = ROOT / p.get("db_path", "runs/backtests.sqlite")
    data_path = ROOT / p.get("data_path", "data/spy_5m.parquet")
    gates_dir = ROOT / p.get("gates_dir", "gates")
    artifacts_dir = ROOT / p.get("artifacts_dir", "runs/artifacts")

    return Paths(
        root=ROOT,
        db_path=db_path,
        data_path=data_path,
        gates_dir=gates_dir,
        artifacts_dir=artifacts_dir,
    )


def alpaca_env_from_config(cfg: Optional[Dict[str, Any]] = None) -> AlpacaEnv:
    cfg = cfg or get_config()
    alp = (cfg.get("api", {}) or {}).get("alpaca", {}) or {}

    key_env = alp.get("key_id_env", "ALPACA_KEY_ID")
    sec_env = alp.get("secret_key_env", "ALPACA_SECRET_KEY")

    return AlpacaEnv(
        key_id=os.getenv(key_env),
        secret_key=os.getenv(sec_env),
    )