#!/usr/bin/env python3
"""
Merge Bunny Stream output into a single MP4 using stream copy (no re-encode).

Aligned with Edmingle Bunny usage (Stream API + CDN URLs):
  - List videos: GET https://video.bunnycdn.com/library/{libraryId}/videos
  - Auth header: AccessKey (same pattern as DrmVideoUtils / teachingmaterial flows)

Requires: Python 3.9+, ffmpeg in PATH, aiohttp (pip install aiohttp)

**ZIP method (default on):** If ``--cdn-base`` looks like ``https://vz-….b-cdn.net``,
  the storage zone name ``vz-…`` is derived and we try **one GET** (not HEAD) to Bunny
  Storage ``…/{zone}/{videoGuid}/?accessKey=…&download`` (streamed to disk as a ZIP).
  Note: some Bunny endpoints return **401 to HEAD** while **GET** works—do not use
  ``curl -I`` alone to debug. After
  extract, the best ``.mp4`` / ``.ts`` / ``.m4s`` file is chosen and copied or
  remuxed to MP4. This only works when your Stream library is backed by that
  storage zone; otherwise you get 404 and we **fall back** to HLS remux.

**HLS fallback:** One ffmpeg pass on the best variant ``.m3u8`` (handles AES-128,
  Bunny ``.dts`` segment names via demuxer flags).

Environment (optional; CLI overrides):
  BUNNY_STREAM_LIBRARY_ID
  BUNNY_STREAM_ACCESS_KEY
  BUNNY_STREAM_CDN_BASE   e.g. https://vz-abcdef123.b-cdn.net  (no trailing slash)
  BUNNY_VIDEO_GUID  optional; single-video mode if ``--video-guid`` is omitted
  BUNNY_MERGE_METHOD  used by ``run_merge.py``: ``zip`` (default) or ``hls``

  Storage ZIP auth (standalone CLI only): use ``--storage-access-key``; Edmingle DRM uses
  ``bunny.drm_bunny_access_key`` from ``worker/next`` (see ``drm_hls_migration_worker.py``).

  Optional S3 (after each successful merge): set ``BUNNY_S3_BUCKET`` plus standard
  ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` (and ``AWS_REGION``). See ``.env.example``.

On startup, if a ``.env`` file exists next to this script (same layout as ``run_*.py``),
it is loaded automatically so you can run this file directly without exporting variables.
"""

from __future__ import annotations

import argparse
import asyncio
import functools
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, List, Optional, Tuple
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlparse, urlunparse

try:
    import aiohttp
except ImportError as exc:  # pragma: no cover
    print("Install aiohttp: pip install aiohttp", file=sys.stderr)
    raise SystemExit(1) from exc

from _s3_upload import load_s3_upload_config, upload_local_mp4

LOG = logging.getLogger("bunny_hls_merge")

# Bunny Stream API (manage videos)
STREAM_API_BASE = "https://video.bunnycdn.com"
STORAGE_API_BASE = "https://storage.bunnycdn.com"
VIDEO_FINISHED_STATUS = 4

VIDEO_SOURCE_EXTENSIONS = frozenset({".mp4", ".ts", ".m4s"})
ZIP_PROGRESS_LOG_BYTES = 50 * 1024 * 1024


def parse_storage_zone_from_cdn_base(cdn_base: str) -> Optional[str]:
    """
    Derive Bunny storage / pull zone label from Stream CDN host, e.g.
    ``https://vz-6c2fc224-1bf.b-cdn.net`` -> ``vz-6c2fc224-1bf``.
    """
    raw = (cdn_base or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    host = (parsed.netloc or "").strip().lower().rstrip(".")
    if not host:
        return None
    if host.endswith(".b-cdn.net"):
        return host[: -len(".b-cdn.net")]
    # Custom hostname: use leftmost label as best-effort guess
    return host.split(".")[0] if host else None


def _height_hint_from_relative_path(rel: str) -> int:
    """Prefer 1080p paths, then 720p, etc., for ZIP contents."""
    s = rel.lower().replace("\\", "/")
    best = 0
    for m in re.finditer(r"/(\d{3,4})p/", s):
        best = max(best, int(m.group(1)))
    if best == 0:
        for m in re.finditer(r"(\d{3,4})p", s):
            best = max(best, int(m.group(1)))
    return best


def find_best_video_file_in_tree(root: Path) -> Optional[Path]:
    """
    Pick the best candidate under ``root``: highest resolution hint in path,
    then largest file size. Extensions: .mp4, .ts, .m4s.
    """
    scored: list[tuple[int, int, Path]] = []
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        suf = p.suffix.lower()
        if suf not in VIDEO_SOURCE_EXTENSIONS:
            continue
        try:
            rel = str(p.relative_to(root))
            sz = p.stat().st_size
        except OSError:
            continue
        h = _height_hint_from_relative_path(rel)
        scored.append((h, sz, p))
    if not scored:
        return None
    scored.sort(key=lambda t: (t[0], t[1]))
    h, sz, best = scored[-1]
    LOG.info("Selected video file: %s (hint_h=%s, size=%s bytes)", best, h, sz)
    return best


def extract_zip_logged(zip_path: Path, dest_dir: Path) -> int:
    """Extract ZIP to ``dest_dir``; return member count. Raises on corrupt archive."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
        if not names:
            raise zipfile.BadZipFile("ZIP archive has no entries")
        corrupt = zf.testzip()
        if corrupt is not None:
            raise zipfile.BadZipFile(f"Corrupt ZIP member: {corrupt}")
        LOG.info("Extracting ZIP: %s file(s) -> %s", len(names), dest_dir)
        t0 = time.perf_counter()
        zf.extractall(dest_dir)
        elapsed = time.perf_counter() - t0
        LOG.info("Extraction finished in %.2fs", elapsed)
    return len(names)


def ffmpeg_container_stream_copy_to_mp4(input_path: Path, output_mp4: Path) -> None:
    """Remux or copy elementary/container to MP4 with stream copy + faststart."""
    output_mp4.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        str(input_path.resolve()),
        "-c",
        "copy",
        "-movflags",
        "+faststart",
        str(output_mp4),
    ]
    LOG.info("Running ffmpeg container remux: %s", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"ffmpeg container remux failed ({proc.returncode}): {proc.stderr or proc.stdout}"
        )


def _storage_zip_url_with_query(base: str, access_key: str, *, download_bare: bool) -> str:
    """
    Build full Storage URL with query string (no extra encoding of ``/`` in base).

    Bunny dashboard / browser often uses a bare ``download`` flag::
      ?accessKey=…&download

    Some clients use ``download=true`` instead; toggle via ``download_bare``.
    """
    key_q = quote(access_key, safe="-")
    if download_bare:
        return f"{base}?accessKey={key_q}&download"
    return f"{base}?accessKey={key_q}&download=true"


def _storage_zip_url_download_only(base: str, *, download_bare: bool) -> str:
    if download_bare:
        return f"{base}?download"
    return f"{base}?download=true"


async def download_video_as_zip(
    session: aiohttp.ClientSession,
    *,
    video_guid: str,
    storage_zone_name: str,
    access_key: str,
    zip_path: Path,
    timeout: aiohttp.ClientTimeout,
    download_bare: bool = True,
) -> Tuple[int, float]:
    """
    Download folder ``/{storage_zone_name}/{video_guid}/`` as one ZIP (streamed to disk).

    Primary request matches browser-style Storage ZIP links::
      GET {base}?accessKey=…&download

    On 401/403, retries with ``AccessKey`` header and ``?download`` (or ``?download=true``).
    Returns ``(bytes_written, seconds_elapsed)``.
    """
    base = f"{STORAGE_API_BASE.rstrip('/')}/{storage_zone_name.strip('/')}/{video_guid.strip()}/"
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    zip_path.unlink(missing_ok=True)

    async def _stream_body_to_file(resp: aiohttp.ClientResponse, dest: Path) -> int:
        total = 0
        last_log = 0
        chunk_size = 1024 * 1024
        with open(dest, "wb") as out_f:
            while True:
                chunk = await resp.content.read(chunk_size)
                if not chunk:
                    break
                out_f.write(chunk)
                total += len(chunk)
                if total - last_log >= ZIP_PROGRESS_LOG_BYTES:
                    LOG.info("ZIP download progress: %.1f MiB", total / (1024 * 1024))
                    last_log = total
        return total

    url1 = _storage_zip_url_with_query(base, access_key, download_bare=download_bare)
    LOG.debug(
        "ZIP GET URL shape: %s?accessKey=***&%s",
        base.rstrip("/"),
        "download" if download_bare else "download=true",
    )

    t0 = time.perf_counter()
    header_retry = False
    async with session.get(url1, allow_redirects=True, timeout=timeout) as resp:
        if resp.status in (401, 403):
            LOG.info("ZIP GET returned HTTP %s; retrying with AccessKey header", resp.status)
            await resp.read()
            header_retry = True
        elif resp.status >= 400:
            raise RuntimeError(f"Storage ZIP download failed: HTTP {resp.status} for {base!r}")
        else:
            n = await _stream_body_to_file(resp, zip_path)
            elapsed = time.perf_counter() - t0
            LOG.info(
                "ZIP download complete: %s bytes in %.2fs (avg %.2f MiB/s)",
                n,
                elapsed,
                (n / max(elapsed, 1e-6)) / (1024 * 1024),
            )
            return n, elapsed

    if not header_retry:
        raise RuntimeError("Storage ZIP download: unexpected state")

    zip_path.unlink(missing_ok=True)
    url2 = _storage_zip_url_download_only(base, download_bare=download_bare)
    t0 = time.perf_counter()
    async with session.get(
        url2,
        headers={"AccessKey": access_key},
        allow_redirects=True,
        timeout=timeout,
    ) as resp2:
        if resp2.status >= 400:
            raise RuntimeError(f"Storage ZIP download failed: HTTP {resp2.status} for {base!r}")
        n = await _stream_body_to_file(resp2, zip_path)
        elapsed = time.perf_counter() - t0
        LOG.info(
            "ZIP download complete: %s bytes in %.2fs (avg %.2f MiB/s)",
            n,
            elapsed,
            (n / max(elapsed, 1e-6)) / (1024 * 1024),
        )
        return n, elapsed


async def _attempt_zip_to_mp4(
    session: aiohttp.ClientSession,
    *,
    video_guid: str,
    access_key: str,
    storage_zone: str,
    work_dir: Path,
    out_path: Path,
    timeout: aiohttp.ClientTimeout,
    download_bare: bool,
) -> Path:
    """Download Storage ZIP, extract, pick best media file, remux to ``out_path``. Raises on failure."""
    zip_path = work_dir / f"{video_guid}.zip"
    await download_video_as_zip(
        session,
        video_guid=video_guid,
        storage_zone_name=storage_zone,
        access_key=access_key,
        zip_path=zip_path,
        timeout=timeout,
        download_bare=download_bare,
    )
    sz = zip_path.stat().st_size
    if sz < 100:
        raise RuntimeError(f"ZIP too small ({sz} bytes); likely not a folder archive")
    if not zipfile.is_zipfile(zip_path):
        raise RuntimeError("Downloaded file is not a valid ZIP archive")
    extract_dir = work_dir / "extracted"
    extract_zip_logged(zip_path, extract_dir)
    try:
        n_zip = zip_path.stat().st_size
        zip_path.unlink()
        LOG.info("Removed Storage ZIP after extract (%s bytes) to reduce peak disk use", n_zip)
    except OSError as exc:
        LOG.warning("Could not remove ZIP after extract (non-fatal): %s", exc)
    best = find_best_video_file_in_tree(extract_dir)
    if best is None:
        raise RuntimeError("No .mp4, .ts, or .m4s files found under extracted ZIP tree")
    suf = best.suffix.lower()
    if suf == ".mp4":
        LOG.info("Source is MP4; remuxing with stream copy + faststart")
    elif suf in (".ts", ".m4s"):
        LOG.info("Source is %s; remuxing to MP4 (stream copy)", suf)
    ffmpeg_container_stream_copy_to_mp4(best, out_path)
    return out_path


# ---------------------------------------------------------------------------
# FFmpeg: single-input HLS remux (clear or AES-128)
# ---------------------------------------------------------------------------
@functools.lru_cache(maxsize=1)
def _ffmpeg_hls_demuxer_help_text() -> str:
    """Raw stdout+stderr from ``ffmpeg -h demuxer=hls`` (empty if probe failed)."""
    try:
        proc = subprocess.run(
            ["ffmpeg", "-h", "demuxer=hls"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        return (proc.stdout or "") + (proc.stderr or "")
    except (OSError, subprocess.TimeoutExpired, ValueError):
        return ""


def _ffmpeg_hls_relaxed_extension_args() -> List[str]:
    """
    HLS demuxer flags that relax segment filename extensions (e.g. Bunny ``.dts``).

    Each flag exists only from a certain FFmpeg version onward. Passing an unsupported
    global-style option makes ffmpeg exit with "Unrecognized option '…'", so we only
    append options that appear in this host's ``ffmpeg -h demuxer=hls`` output.
    """
    blob = _ffmpeg_hls_demuxer_help_text()
    out: List[str] = []
    if "-extension_picky" in blob:
        out.extend(["-extension_picky", "0"])
    if "-allowed_segment_extensions" in blob:
        out.extend(["-allowed_segment_extensions", "ALL"])
    if "-allowed_extensions" in blob:
        out.extend(["-allowed_extensions", "ALL"])
    return out


def ffmpeg_hls_remux_to_mp4(variant_playlist_url: str, output_mp4: Path) -> None:
    """
    Remux HLS (clear or AES-128) to MP4 in one ffmpeg pass.

    Bunny Stream sometimes lists TS segments as ``*.dts``; newer ffmpeg can relax
    extension checks via HLS demuxer options. Older binaries omit those options; we
    probe once and only pass flags this build actually supports.

    When ``-i`` is a **local** ``.m3u8`` (rewritten signed playlist), the HLS demuxer
    defaults to a tight protocol whitelist (often ``file,crypto,data`` only). Segment
    lines then use ``https://`` and ffmpeg errors unless ``https`` / ``tcp`` / ``tls``
    are whitelisted — see ``-protocol_whitelist`` below.
    """
    output_mp4.parent.mkdir(parents=True, exist_ok=True)
    head = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        # Local playlist + absolute https segment URLs (signed Bunny Storage).
        "-protocol_whitelist",
        "file,http,https,tcp,tls,crypto,data",
    ]
    head.extend(_ffmpeg_hls_relaxed_extension_args())
    head.extend(
        [
            "-i",
            variant_playlist_url,
            "-c",
            "copy",
            "-movflags",
            "+faststart",
        ]
    )
    cmd_aac = head + ["-bsf:a", "aac_adtstoasc", str(output_mp4)]
    LOG.info("Running ffmpeg HLS remux: %s", " ".join(cmd_aac))
    LOG.info(
        "ffmpeg is pulling the entire HLS over the network (-c copy); long sources can take "
        "many minutes. With -loglevel error there is no progress output until ffmpeg finishes or fails."
    )
    proc = subprocess.run(cmd_aac, capture_output=True, text=True)
    if proc.returncode == 0:
        return
    err = (proc.stderr or proc.stdout or "").lower()
    if any(
        x in err
        for x in (
            "aac_adtstoasc",
            "bitstream filter",
            "codec 'dts'",
            "not supported by the bitstream filter",
        )
    ):
        cmd_plain = head + [str(output_mp4)]
        LOG.warning("AAC bitstream filter failed; retrying remux without -bsf:a aac_adtstoasc")
        proc2 = subprocess.run(cmd_plain, capture_output=True, text=True)
        if proc2.returncode == 0:
            return
        raise RuntimeError(
            f"ffmpeg HLS remux failed ({proc2.returncode}): {proc2.stderr or proc2.stdout}"
        )
    raise RuntimeError(f"ffmpeg HLS remux failed ({proc.returncode}): {proc.stderr or proc.stdout}")


# ---------------------------------------------------------------------------
# HLS parsing (master + variant)
# ---------------------------------------------------------------------------
def _bandwidth_from_stream_inf(line: str) -> int:
    m = re.search(r"BANDWIDTH=(\d+)", line, re.I)
    return int(m.group(1)) if m else 0


def _height_from_stream_inf(line: str) -> int:
    m = re.search(r"RESOLUTION=\d+x(\d+)", line, re.I)
    return int(m.group(1)) if m else 0


_URI_IN_TAG = re.compile(r'URI="([^"]+)"')


def hls_url_inherit_master_query(master_url: str, resolved_url: str) -> str:
    """
    Re-attach the master playlist's query string (e.g. ``accessKey=``) to a resolved URL.

    ``urljoin(master, '1920x1080/video.m3u8')`` drops ``?accessKey=…`` from the master,
    which breaks Bunny Edge Storage (401 on the variant). Child query params still win
    on duplicate keys.
    """
    qm = urlparse(master_url).query.strip()
    if not qm:
        return resolved_url
    pu = urlparse(resolved_url)
    merged = dict(parse_qsl(qm, keep_blank_values=True))
    merged.update(dict(parse_qsl(pu.query, keep_blank_values=True)))
    new_q = urlencode(merged)
    return urlunparse(pu._replace(query=new_q))


def rewrite_hls_media_playlist_signed_query(playlist_url_with_query: str, body: str) -> str:
    """
    Re-resolve every segment and ``URI="…"`` in a **media** playlist so signed query params
    (e.g. Bunny Storage ``accessKey``) are present on **each** URL.

    ffmpeg resolves ``video0.ts`` against the variant URL but **drops** the ``?accessKey``
    query, which yields 401 or invalid TS bytes (ffmpeg error 183).
    """
    base = playlist_url_with_query.strip()
    if not urlparse(base).query.strip():
        return body

    out_lines: list[str] = []
    for raw in body.splitlines():
        line = raw.rstrip("\r")
        stripped = line.strip()
        if not stripped:
            out_lines.append(line)
            continue
        if stripped.startswith("#"):
            if 'URI="' in line:

                def _sub_uri(m: re.Match) -> str:
                    inner = m.group(1)
                    abs_u = urljoin(base, inner)
                    fixed = hls_url_inherit_master_query(base, abs_u)
                    return f'URI="{fixed}"'

                line = _URI_IN_TAG.sub(_sub_uri, line)
            out_lines.append(line)
            continue
        if ".m3u8" in stripped.lower():
            raise ValueError(
                "Nested .m3u8 in signed storage variant (not supported here): "
                f"{stripped!r}"
            )
        abs_u = urljoin(base, stripped)
        fixed = hls_url_inherit_master_query(base, abs_u)
        out_lines.append(fixed)
    trailing = "\n" if body.endswith("\n") or not body else ""
    return "\n".join(out_lines) + trailing


def hls_variant_url_or_local_playlist_for_ffmpeg(
    variant_url: str, variant_text: str, staging_dir: Path
) -> str:
    """
    Return the string to pass as ffmpeg's ``-i`` input for an HLS **media** variant.

    When ``variant_url`` has a query string (e.g. Bunny Storage ``accessKey``), ffmpeg
    does not apply it to relative segment lines, so segment GETs fail (often exit 183).
    In that case we write ``rewrite_hls_media_playlist_signed_query(...)`` under
    ``staging_dir`` and return that file path; otherwise return ``variant_url`` unchanged.
    """
    if not urlparse(variant_url.strip()).query.strip():
        return variant_url.strip()
    body = rewrite_hls_media_playlist_signed_query(variant_url, variant_text)
    staging_dir.mkdir(parents=True, exist_ok=True)
    local = staging_dir / "variant_ffmpeg.m3u8"
    local.write_text(body, encoding="utf-8")
    return str(local.resolve())


def pick_best_variant_url(master_text: str, master_url: str) -> str:
    """
    From a master playlist, choose the variant with highest BANDWIDTH
    (tie-breaker: taller RESOLUTION).
    """
    lines = [ln.strip() for ln in master_text.splitlines()]
    candidates: list[tuple[int, int, str]] = []
    i = 0
    while i < len(lines):
        if lines[i].startswith("#EXT-X-STREAM-INF"):
            bw = _bandwidth_from_stream_inf(lines[i])
            h = _height_from_stream_inf(lines[i])
            if i + 1 < len(lines) and lines[i + 1] and not lines[i + 1].startswith("#"):
                uri = urljoin(master_url, lines[i + 1])
                uri = hls_url_inherit_master_query(master_url, uri)
                candidates.append((bw, h, uri))
                i += 2
                continue
        i += 1
    if not candidates:
        # Already a media playlist (single rendition), not a master
        LOG.info("No variants in master; using playlist URL as media: %s", master_url)
        return master_url
    candidates.sort(key=lambda t: (t[0], t[1]))
    _bw, _h, best = candidates[-1]
    LOG.info("Selected variant (bw=%s res_h=%s): %s", _bw, _h, best)
    return best


def parse_variant_segment_urls(variant_text: str, variant_url: str) -> Tuple[List[str], bool]:
    """
    Return ordered absolute segment URLs and whether any #EXT-X-KEY was seen.
    Handles #EXT-X-MAP for fMP4 (init segment first).
    """
    base = variant_url.rsplit("/", 1)[0] + "/"
    lines = [ln.strip() for ln in variant_text.splitlines()]
    has_key = any(ln.startswith("#EXT-X-KEY") for ln in lines)
    map_uri: Optional[str] = None
    ordered: list[str] = []
    for ln in lines:
        if ln.startswith("#EXT-X-MAP:"):
            m = re.search(r'URI="([^"]+)"', ln)
            if m:
                map_uri = urljoin(base, m.group(1))
        elif ln.startswith("#EXT-X-I-FRAME-STREAM-INF"):
            continue
        elif ln and not ln.startswith("#"):
            # media segment or sub-playlist
            if ".m3u8" in ln.lower():
                raise ValueError(
                    "Nested media playlist in variant; fetch sub-playlist first "
                    f"(line={ln!r})"
                )
            ordered.append(urljoin(base, ln))
    if map_uri:
        ordered = [map_uri] + ordered
    return ordered, has_key


# ---------------------------------------------------------------------------
# Bunny REST (sync pagination helper + async downloads)
# ---------------------------------------------------------------------------
@dataclass
class ProgressTracker:
    path: Path
    completed: set[str] = field(default_factory=set)
    failed: dict[str, str] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> ProgressTracker:
        if not path.exists():
            return cls(path=path)
        data = json.loads(path.read_text(encoding="utf-8"))
        return cls(
            path=path,
            completed=set(data.get("completed", [])),
            failed=dict(data.get("failed", {})),
        )

    def save(self) -> None:
        payload = {
            "version": 1,
            "completed": sorted(self.completed),
            "failed": self.failed,
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        tmp.replace(self.path)


def list_all_video_guids(
    library_id: int, access_key: str, items_per_page: int = 100
) -> list[dict[str, Any]]:
    """Synchronous pagination over GET /library/{id}/videos (video.bunnycdn.com)."""
    import urllib.error
    import urllib.request

    guids: list[dict[str, Any]] = []
    page = 1
    while True:
        url = (
            f"{STREAM_API_BASE}/library/{library_id}/videos"
            f"?page={page}&itemsPerPage={items_per_page}"
        )
        req = urllib.request.Request(
            url,
            headers={
                "AccessKey": access_key,
                "Accept": "application/json",
            },
            method="GET",
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                body = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"List videos HTTP {e.code}: {e.read()!r}") from e

        items = body.get("items") or []
        for it in items:
            guids.append(it)
        total = int(body.get("totalItems") or 0)
        fetched = page * items_per_page
        LOG.debug("Listed page %s: +%s videos (totalItems=%s)", page, len(items), total)
        if fetched >= total or not items:
            break
        page += 1
    return guids


def get_one_video(library_id: int, access_key: str, video_guid: str) -> dict[str, Any]:
    """GET /library/{libraryId}/videos/{videoId} — single video for targeted POC runs."""
    import urllib.error
    import urllib.request

    url = f"{STREAM_API_BASE}/library/{library_id}/videos/{video_guid}"
    req = urllib.request.Request(
        url,
        headers={"AccessKey": access_key, "Accept": "application/json"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Get video HTTP {e.code}: {e.read()!r}") from e


async def fetch_text(session: aiohttp.ClientSession, url: str) -> str:
    async with session.get(url, allow_redirects=True) as resp:
        resp.raise_for_status()
        return await resp.text()


def best_variant_from_api_resolution_paths(
    playlist_resolutions: Iterable[dict[str, Any]],
    cdn_base: str,
    video_guid: str,
) -> Optional[str]:
    """
    Use Bunny /videos/{guid}/resolutions playlistResolutions[].path when present.
    Paths look like play_1080p.m3u8 — pick highest numeric *p height.
    """
    best_h = -1
    best_path: Optional[str] = None
    for pr in playlist_resolutions:
        path = (pr or {}).get("path") or ""
        m = re.search(r"(\d+)p", path)
        if not m:
            continue
        h = int(m.group(1))
        if h > best_h:
            best_h = h
            best_path = path
    if not best_path:
        return None
    # Bunny /resolutions often returns HLS folders only, e.g. "1080p/" — variant is video.m3u8 inside.
    rel = (best_path or "").strip()
    if rel.endswith("/"):
        rel = rel + "video.m3u8"
    elif not rel.lower().endswith(".m3u8"):
        rel = rel.rstrip("/") + "/video.m3u8"
    return urljoin(f"{cdn_base.rstrip('/')}/{video_guid}/", rel)


async def resolve_variant_playlist_url(
    session: aiohttp.ClientSession,
    *,
    library_id: int,
    access_key: str,
    cdn_base: str,
    video_guid: str,
    master_playlist_url: str,
    timeout: aiohttp.ClientTimeout,
) -> str:
    """Prefer Stream API resolutions info; fall back to parsing master playlist.m3u8."""
    res_url = f"{STREAM_API_BASE}/library/{library_id}/videos/{video_guid}/resolutions"
    try:
        async with session.get(
            res_url,
            headers={"AccessKey": access_key, "Accept": "application/json"},
            timeout=timeout,
        ) as resp:
            if resp.status == 200:
                payload = await resp.json()
                data = (payload or {}).get("data") or {}
                prs = data.get("playlistResolutions") or []
                picked = best_variant_from_api_resolution_paths(prs, cdn_base, video_guid)
                if picked:
                    LOG.info("Using resolutions API playlist: %s", picked)
                    return picked
    except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as exc:
        LOG.warning("Resolutions API unavailable (%s); using master playlist", exc)

    master_text = await fetch_text(session, master_playlist_url)
    return pick_best_variant_url(master_text, master_playlist_url)


# ---------------------------------------------------------------------------
# Per-video pipeline
# ---------------------------------------------------------------------------
async def process_one_video(
    *,
    library_id: int,
    access_key: str,
    cdn_base: str,
    video: dict[str, Any],
    output_dir: Path,
    http_timeout_s: float,
    dry_run: bool,
    use_zip_method: bool = True,
    storage_zone_override: Optional[str] = None,
    work_dir_parent: Optional[str] = None,
    storage_access_key: Optional[str] = None,
    storage_zip_download_bare: bool = True,
    zip_only: bool = False,
) -> Optional[Path]:
    guid = video.get("guid") or ""
    title = video.get("title") or guid
    status = video.get("status")
    if status != VIDEO_FINISHED_STATUS:
        LOG.warning("Skipping %s (%s): status=%s (need Finished=%s)", title, guid, status, VIDEO_FINISHED_STATUS)
        return None

    master_url = f"{cdn_base.rstrip('/')}/{guid}/playlist.m3u8"
    LOG.info("--- Video: %s | guid=%s ---", title, guid)

    storage_zone = (storage_zone_override or "").strip() or parse_storage_zone_from_cdn_base(cdn_base)
    if zip_only and (not use_zip_method or not storage_zone):
        raise RuntimeError(
            "--zip-only requires a storage zone (from --cdn-base hostname or --storage-zone) "
            "and ZIP enabled; pass --storage-access-key for Storage auth (or omit if same as --access-key)."
        )
    if use_zip_method and storage_zone:
        q = "accessKey=***&download" if storage_zip_download_bare else "accessKey=***&download=true"
        LOG.info(
            "ZIP method enabled: storage zone %r (GET %s/%s/%s/?%s)",
            storage_zone,
            STORAGE_API_BASE,
            storage_zone,
            guid,
            q,
        )
    elif use_zip_method and not storage_zone:
        LOG.warning(
            "ZIP method requested but could not parse storage zone from --cdn-base %r; "
            "use --storage-zone to set it explicitly.",
            cdn_base,
        )

    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=http_timeout_s)
    connector = aiohttp.TCPConnector(limit=32)

    work_dir: Optional[Path] = None
    try:
        if not dry_run and use_zip_method and storage_zone:
            wd_arg = (work_dir_parent or "").strip()
            parent: Optional[Path] = Path(wd_arg).resolve() if wd_arg else None
            if parent is not None:
                if not parent.is_dir():
                    raise ValueError(f"--work-dir is not a directory: {parent}")
                work_dir = Path(tempfile.mkdtemp(prefix="bunny_zip_", dir=str(parent)))
            else:
                work_dir = Path(tempfile.mkdtemp(prefix="bunny_zip_"))

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            if dry_run:
                if use_zip_method and storage_zone:
                    LOG.info("Dry-run: would try Storage ZIP%s", " only (--zip-only)" if zip_only else " before HLS")
                if zip_only:
                    return None
                variant_url = await resolve_variant_playlist_url(
                    session,
                    library_id=library_id,
                    access_key=access_key,
                    cdn_base=cdn_base,
                    video_guid=guid,
                    master_playlist_url=master_url,
                    timeout=timeout,
                )
                variant_text = await fetch_text(session, variant_url)
                segment_urls, has_key = parse_variant_segment_urls(variant_text, variant_url)
                if segment_urls:
                    LOG.info(
                        "Dry-run HLS fallback: variant=%s segments=%s has_key=%s",
                        variant_url,
                        len(segment_urls),
                        has_key,
                    )
                    LOG.info("Dry-run: first segment URL: %s", segment_urls[0])
                else:
                    LOG.warning("Dry-run: HLS variant has no segments parsed")
                return None

            safe_name = re.sub(r"[^\w.\-]+", "_", title)[:180] or guid
            out_path = output_dir / f"{safe_name}_{guid}.mp4"

            if use_zip_method and storage_zone and work_dir is not None:
                zip_key = (storage_access_key or access_key).strip()
                try:
                    await _attempt_zip_to_mp4(
                        session,
                        video_guid=guid,
                        access_key=zip_key,
                        storage_zone=storage_zone,
                        work_dir=work_dir,
                        out_path=out_path,
                        timeout=timeout,
                        download_bare=storage_zip_download_bare,
                    )
                    LOG.info("Wrote %s (via Storage ZIP)", out_path)
                    return out_path
                except (RuntimeError, aiohttp.ClientError, asyncio.TimeoutError, OSError, zipfile.BadZipFile) as zip_exc:
                    if zip_only:
                        LOG.error("ZIP-only mode: %s — not falling back to HLS.", zip_exc)
                        raise
                    LOG.warning(
                        "ZIP storage method failed (%s: %s); falling back to HLS remux.",
                        type(zip_exc).__name__,
                        zip_exc,
                    )

            if zip_only:
                raise RuntimeError(
                    "ZIP-only mode: ZIP path was not attempted (missing work dir / storage zone) or failed above."
                )

            variant_url = await resolve_variant_playlist_url(
                session,
                library_id=library_id,
                access_key=access_key,
                cdn_base=cdn_base,
                video_guid=guid,
                master_playlist_url=master_url,
                timeout=timeout,
            )
            variant_text = await fetch_text(session, variant_url)
            segment_urls, has_key = parse_variant_segment_urls(variant_text, variant_url)
            if not segment_urls:
                raise RuntimeError(f"No media segments parsed for {guid}")

            if has_key:
                LOG.info("%s: AES-128 HLS — ffmpeg will pull keys and segments from the variant URL.", guid)
            LOG.info(
                "%s: HLS fallback — remuxing variant with ffmpeg (%s segments; has_key=%s)",
                guid,
                len(segment_urls),
                has_key,
            )
            staging = work_dir if work_dir is not None else out_path.parent
            ffmpeg_in = hls_variant_url_or_local_playlist_for_ffmpeg(
                variant_url, variant_text, staging
            )
            try:
                ffmpeg_hls_remux_to_mp4(ffmpeg_in, out_path)
            finally:
                if ffmpeg_in != variant_url:
                    Path(ffmpeg_in).unlink(missing_ok=True)
            LOG.info("Wrote %s (via HLS)", out_path)
            return out_path
    finally:
        if work_dir is not None and work_dir.exists():
            shutil.rmtree(work_dir, ignore_errors=True)


async def run_async(args: argparse.Namespace) -> int:
    library_id = int(args.library_id or os.environ.get("BUNNY_STREAM_LIBRARY_ID", "0"))
    access_key = args.access_key or os.environ.get("BUNNY_STREAM_ACCESS_KEY", "")
    storage_zip_key = (args.storage_access_key or "").strip() or None
    cdn_base = args.cdn_base or os.environ.get("BUNNY_STREAM_CDN_BASE", "")
    if not library_id or not access_key or not cdn_base:
        LOG.error("Set library_id, access_key, and cdn_base (CLI or env).")
        LOG.error(
            "Edmingle DRM worker/next flow: run run_hls_merge.py or drm_hls_migration_worker.py "
            "(they call worker/next and merge from the job's HLS URL — not this standalone entrypoint)."
        )
        return 2

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    progress = ProgressTracker.load(Path(args.progress_file).resolve())
    LOG.info(
        "Resume: %s completed, %s failed (progress file=%s)",
        len(progress.completed),
        len(progress.failed),
        progress.path,
    )

    video_guid = (args.video_guid or os.environ.get("BUNNY_VIDEO_GUID") or "").strip() or None
    if video_guid:
        videos = [get_one_video(library_id, access_key, video_guid)]
        LOG.info("Single-video mode: %s", video_guid)
    else:
        videos = list_all_video_guids(
            library_id, access_key, items_per_page=args.items_per_page
        )
    if args.max_videos:
        videos = videos[: args.max_videos]

    s3_cfg = load_s3_upload_config()
    if s3_cfg:
        LOG.info(
            "S3 upload enabled: bucket=%r prefix=%r region=%r (local file removed after upload)",
            s3_cfg.bucket,
            s3_cfg.key_prefix or "(none)",
            s3_cfg.region,
        )

    for video in videos:
        guid = video.get("guid", "")
        if not guid or guid in progress.completed:
            continue
        try:
            result = await process_one_video(
                library_id=library_id,
                access_key=access_key,
                cdn_base=cdn_base,
                video=video,
                output_dir=output_dir,
                http_timeout_s=args.http_timeout,
                dry_run=args.dry_run,
                use_zip_method=args.use_zip_method,
                storage_zone_override=args.storage_zone or None,
                work_dir_parent=args.work_dir or None,
                storage_access_key=storage_zip_key,
                storage_zip_download_bare=args.storage_zip_download_bare,
                zip_only=args.zip_only,
            )
            # Do not mark skipped (non-finished) or dry-run as completed
            if not args.dry_run and result is not None:
                if s3_cfg is not None:
                    try:
                        uri = await asyncio.to_thread(upload_local_mp4, s3_cfg, result)
                        LOG.info("Uploaded to %s", uri)
                    except Exception:
                        LOG.exception("S3 upload failed for %s (local file kept)", result)
                        raise
                progress.completed.add(guid)
                if guid in progress.failed:
                    del progress.failed[guid]
                progress.save()
        except Exception as exc:  # pylint: disable=broad-exception-caught
            LOG.exception("Failed video %s: %s", guid, exc)
            progress.failed[guid] = str(exc)
            progress.save()

    LOG.info("Done. completed=%s failed=%s", len(progress.completed), len(progress.failed))
    return 0 if not progress.failed else 1


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--library-id", type=int, default=None, help="Bunny Stream library id (numeric)")
    p.add_argument("--access-key", default=None, help="Stream library AccessKey (secret)")
    p.add_argument(
        "--storage-access-key",
        default=None,
        help="Bunny Storage password for ZIP download (default: same as --access-key when omitted).",
    )
    p.add_argument(
        "--cdn-base",
        default=None,
        help="CDN root for files, e.g. https://vz-xxxx.b-cdn.net (see Bunny storage structure docs)",
    )
    p.add_argument("--output-dir", default="./bunny_mp4_output", help="Final MP4 directory")
    p.add_argument(
        "--use-zip-method",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Try Bunny Storage ZIP download first, then HLS remux on failure (default: on).",
    )
    p.add_argument(
        "--storage-zone",
        default="",
        help="Storage zone name for ZIP API (default: parsed from --cdn-base hostname before .b-cdn.net).",
    )
    p.add_argument(
        "--storage-zip-download-bare",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="ZIP URL uses …&download (Bunny UI style, default). Use --no-storage-zip-download-bare for …&download=true.",
    )
    p.add_argument(
        "--zip-only",
        action="store_true",
        help="Only use Storage ZIP; on failure exit with error (no HLS remux).",
    )
    p.add_argument(
        "--work-dir",
        default="",
        help="Optional parent directory for temporary ZIP/extract folders (default: system temp).",
    )
    p.add_argument("--progress-file", default="./bunny_merge_progress.json")
    p.add_argument(
        "--concurrency",
        type=int,
        default=32,
        help="Deprecated: ignored (ffmpeg HLS demuxer fetches segments sequentially).",
    )
    p.add_argument(
        "--retries",
        type=int,
        default=6,
        help="Deprecated: ignored.",
    )
    p.add_argument("--http-timeout", type=float, default=300.0, help="Per-read timeout (seconds)")
    p.add_argument("--items-per-page", type=int, default=100)
    p.add_argument("--max-videos", type=int, default=0, help="Stop after N videos (0 = no limit)")
    p.add_argument(
        "--video-guid",
        default=None,
        help="Process only this video GUID (skips list API); best for POC demos",
    )
    p.add_argument("--dry-run", action="store_true", help="Resolve playlists only; no download/ffmpeg")
    p.add_argument("-v", "--verbose", action="store_true")
    return p


def _try_load_dotenv() -> None:
    """Load ``.env`` from the project directory (same file as ``run_zip_merge.py`` uses)."""
    try:
        from _env_util import deploy_dir, load_env_file
    except ImportError:
        return
    try:
        load_env_file(deploy_dir() / ".env")
    except FileNotFoundError:
        pass


def main() -> None:
    _try_load_dotenv()
    args = build_arg_parser().parse_args()
    if args.zip_only and not args.use_zip_method:
        print("Cannot use --zip-only with --no-use-zip-method.", file=sys.stderr)
        raise SystemExit(2)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    try:
        rc = asyncio.run(run_async(args))
    except KeyboardInterrupt:
        LOG.error("Interrupted")
        rc = 130
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
