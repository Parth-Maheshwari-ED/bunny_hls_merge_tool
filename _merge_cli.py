"""Build argv for ``bunny_stream_hls_merge_to_mp4.py`` (HLS path only). ZIP uses ``run_zip_merge.py`` + API."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Optional

from _env_util import deploy_dir, load_env_file, merge_script_path, pick_python_exe, require_keys


def build_merge_argv(env: dict, d: Path, method: str) -> list[str]:
    method = (method or "hls").strip().lower()
    if method not in ("zip", "hls"):
        raise SystemExit("BUNNY_MERGE_METHOD must be 'zip' or 'hls'")

    if method == "zip":
        raise SystemExit(
            "ZIP merge is not available through _merge_cli (Storage auth is not read from env). "
            "Use run_zip_merge.py — bunny.drm_bunny_access_key comes from worker/next. "
            "For ad-hoc library ZIP, call bunny_stream_hls_merge_to_mp4.py with --storage-access-key."
        )

    keys = [
        "BUNNY_STREAM_LIBRARY_ID",
        "BUNNY_STREAM_ACCESS_KEY",
        "BUNNY_STREAM_CDN_BASE",
        "BUNNY_VIDEO_GUID",
    ]
    require_keys(env, *keys)

    raw_out = ((env.get("BUNNY_OUTPUT_DIR_HLS")) or ".").strip() or "."
    out = Path(raw_out).expanduser()
    out_dir = out.resolve() if out.is_absolute() else (d / out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    prog_name = ".merge_progress_hls.json"
    prog = out_dir / prog_name

    py = str(pick_python_exe())
    script = str(merge_script_path())
    lid = str(int(env["BUNNY_STREAM_LIBRARY_ID"]))
    ak = env["BUNNY_STREAM_ACCESS_KEY"]
    cdn = env["BUNNY_STREAM_CDN_BASE"]
    guid = env["BUNNY_VIDEO_GUID"].strip()
    tail = [
        "--output-dir",
        str(out_dir),
        "--progress-file",
        str(prog),
        "-v",
    ]
    cmd = [
        py,
        script,
        "--library-id",
        lid,
        "--access-key",
        ak,
        "--cdn-base",
        cdn,
        "--video-guid",
        guid,
        "--no-use-zip-method",
        *tail,
    ]
    return cmd


def main(forced_method: Optional[str] = None) -> int:
    d = deploy_dir()
    env = load_env_file(d / ".env")
    if forced_method is not None:
        method = forced_method.strip().lower()
    else:
        method = (env.get("BUNNY_MERGE_METHOD") or "hls").strip().lower()
    argv = build_merge_argv(env, d, method)
    print("Running merge method:", method, flush=True)
    print("Running:", " ".join(argv[:6]), "... --access-key *** ...", flush=True)
    return subprocess.call(argv)


if __name__ == "__main__":
    raise SystemExit(main(None))
