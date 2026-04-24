#!/usr/bin/env python3
"""
One DRM migration job: **POST …/worker/next** → merge → S3 → **worker/report**.

Merge mode from ``DRM_MIGRATION_MERGE_METHOD`` or ``BUNNY_MERGE_METHOD`` in ``.env``
(``hls`` or ``zip``). Default **hls**. Bunny Stream settings come from the API job, not from
``BUNNY_STREAM_*`` env vars.

For a fixed mode without editing ``.env``, use ``run_hls_merge.py`` or ``run_zip_merge.py``.
"""

from __future__ import annotations

import os

from _env_util import deploy_dir, load_env_file


def main() -> None:
    d = deploy_dir()
    load_env_file(d / ".env")
    method = (
        os.environ.get("DRM_MIGRATION_MERGE_METHOD") or os.environ.get("BUNNY_MERGE_METHOD") or "hls"
    ).strip().lower()
    if method not in ("hls", "zip"):
        raise SystemExit("Merge method must be 'hls' or 'zip' (DRM_MIGRATION_MERGE_METHOD or BUNNY_MERGE_METHOD)")
    os.environ["DRM_MIGRATION_MERGE_METHOD"] = method
    from drm_hls_migration_worker import main as worker_main

    worker_main()


if __name__ == "__main__":
    main()
