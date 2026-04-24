#!/usr/bin/env python3
"""
One DRM migration job via **worker/next**, merge with **HLS → ffmpeg** (default worker path).

Equivalent to ``drm_hls_migration_worker.py`` with ``DRM_MIGRATION_MERGE_METHOD=hls``.
"""

from __future__ import annotations

import os

from _env_util import deploy_dir, load_env_file


def main() -> None:
    d = deploy_dir()
    load_env_file(d / ".env")
    os.environ["DRM_MIGRATION_MERGE_METHOD"] = "hls"
    from drm_hls_migration_worker import main as worker_main

    worker_main()


if __name__ == "__main__":
    main()
