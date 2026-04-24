"""Load ``.env`` next to this folder and resolve path to ``bunny_stream_hls_merge_to_mp4.py``."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict


def deploy_dir() -> Path:
    return Path(__file__).resolve().parent


def load_env_file(path: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not path.is_file():
        raise FileNotFoundError(f"Missing env file: {path} (copy .env.example to .env)")
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key, val = key.strip(), val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
            val = val[1:-1]
        out[key] = val
    for k, v in out.items():
        os.environ.setdefault(k, v)
    return out


def merge_script_path() -> Path:
    p = deploy_dir() / "bunny_stream_hls_merge_to_mp4.py"
    if not p.is_file():
        raise FileNotFoundError(f"Missing merge script: {p}")
    return p


def pick_python_exe() -> Path:
    venv_py = deploy_dir() / ".venv" / "bin" / "python3"
    if venv_py.is_file():
        return venv_py
    venv_py2 = deploy_dir() / ".venv" / "bin" / "python"
    if venv_py2.is_file():
        return venv_py2
    import sys

    return Path(sys.executable)


def require_keys(env: Dict[str, str], *keys: str, hint: str = "") -> None:
    missing = [k for k in keys if not (env.get(k) or os.environ.get(k, "")).strip()]
    if missing:
        msg = f".env missing required keys: {', '.join(missing)}"
        if hint:
            msg += f". {hint}"
        raise SystemExit(msg)
