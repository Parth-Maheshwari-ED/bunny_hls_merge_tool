#!/usr/bin/env python3
"""
Edmingle DRM HLS→MP4 bridge worker (HTTP only — no direct DB, no Bunny IDs in env).

Polls **POST /drmvideo/migration/worker/next**. The job supplies ``migration_row``,
``bunny`` (storage zone, library keys, …), optional ``hls_playlist_url``, and ``s3``.
**No** ``BUNNY_STREAM_*`` / ``BUNNY_VIDEO_GUID`` in env — library id, API keys, zone, and
``drm_id`` all come from that JSON.

Merge pipeline (env ``DRM_MIGRATION_MERGE_METHOD`` or ``BUNNY_MERGE_METHOD``, default ``hls``):

- **hls** — non-empty ``job.hls_playlist_url``; else Stream CDN
  ``https://{zone}.b-cdn.net/{drm_id}/playlist.m3u8``, then ffmpeg remux.
- **zip** — Bunny Storage ZIP download for ``/{zone}/{drm_id}/``. Auth is **only**
  ``bunny.drm_bunny_access_key`` from ``worker/next`` (Bunny Storage / Edge password).
  No Bunny storage or Stream keys are read from the environment for merge.

HTTP: same shape as browser **curl --form** — multipart field ``JSONString``, plus headers
``APIKEY`` and ``ORGID`` (configurable). Base URL: ``EDMINGLE_WORKER_API_BASE`` **or**
``EDMINGLE_API_PROTOCOL`` + ``EDMINGLE_API_HOST`` + ``EDMINGLE_API_PATH_PREFIX``.

``job.s3.bucket`` may be empty; falls back to env ``drm_migration_s3_bucket_name``.

Merge scratch (ZIP extract, ``merged.mp4``) uses the **OS temp directory**, not
``DRM_MIGRATION_LOCAL_OUTPUT_DIR``. That env path is only for the **no-S3-IAM** fallback
(``drm_migration_s3_access_key`` / ``drm_migration_s3_secret_key`` unset): the MP4 is moved
there and the worker reports failure so the job can be retried after fixing credentials.

Runs in a **loop**: after each job, **POST …/worker/report** (``outcome`` ``success`` or
``failure`` with ``error_message``), wait **DRM_MIGRATION_JOB_GAP_SEC** (default ``1.5``),
then **POST …/worker/next** again (after a short pause, default **1.5s**, before each next
call except the first). Merge, S3, or report errors on one migration do **not**
stop the worker; only ``worker/next`` transport or envelope errors exit the process.
Unexpected crashes are still **reported** as ``failure`` when ``migration_id`` is known.

**Throughput (why one EC2 does not “use 50 Gbps”)**

- This process does **one** ``worker/next`` job at a time, then merge, then S3, then ``worker/report``.
  Throughput is the sum of **Bunny → EC2** (ZIP or HLS), **ffmpeg**, and **EC2 → S3**, plus **Edmingle** round-trips.
  Your instance’s “up to N Gbps” NIC does not apply to Bunny’s edge build/ZIP speed or to single-stream ffmpeg.
- **ZIP:** Bunny builds and streams the archive; downloads in your logs are often **~10–15 MiB/s** — that is
  usually **Bunny or the path to Bunny**, not a broken EC2 NIC.
- **HLS:** ffmpeg pulls segments **sequentially**; wall time is dominated by remux + network, not local RAM.
- **Idle gap:** ``DRM_MIGRATION_JOB_GAP_SEC`` (default ``1.5``) sleeps between jobs; set to ``0`` if the API tolerates it.
- **Scale out:** run **multiple** worker processes on **separate** hosts (or one host if Edmingle hands out distinct
  migration rows per worker without locking); one Python loop will never saturate “50 Gbps”.
- **Tuning:** ``DRM_MIGRATION_ZIP_READ_CHUNK_BYTES``, ``DRM_MIGRATION_S3_UPLOAD_MAX_CONCURRENCY``,
  ``DRM_MIGRATION_S3_MULTIPART_CHUNKSIZE_BYTES`` (see ``bunny_stream_hls_merge_to_mp4`` / ``_drm_migration_s3``).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote

import aiohttp

import bunny_stream_hls_merge_to_mp4 as bunny
from _drm_migration_s3 import (
    drm_s3_client_from_keys,
    head_object_nonzero_size,
    is_s3_auth_or_config_failure,
    put_mp4_private_then_retry_no_acl,
    verify_object_nonzero_after_put,
)
from _env_util import deploy_dir, load_env_file

LOG = logging.getLogger("drm_migration_worker")

ERROR_MSG_MAX = 65000


def _trunc_err(msg: str) -> str:
    if len(msg) <= ERROR_MSG_MAX:
        return msg
    return msg[: ERROR_MSG_MAX - 40] + "\n...[truncated]"


def _api_base() -> str:
    """
    Full API root, e.g. ``http://localhost/nuSource/api/v1`` (no trailing slash).

    Set ``EDMINGLE_WORKER_API_BASE`` **or** build from protocol + host + path prefix.
    """
    full = (os.environ.get("EDMINGLE_WORKER_API_BASE") or "").strip().rstrip("/")
    if full:
        return full
    protocol = (
        os.environ.get("EDMINGLE_API_PROTOCOL") or os.environ.get("PROTOCOL") or "http"
    ).strip().rstrip(":")
    host = (os.environ.get("EDMINGLE_API_HOST") or os.environ.get("DOMAIN") or "localhost").strip()
    path = (
        os.environ.get("EDMINGLE_API_PATH_PREFIX")
        or os.environ.get("BASE_PATH")
        or "/nuSource/api/v1"
    ).strip()
    if not path.startswith("/"):
        path = "/" + path
    path = path.rstrip("/")
    if not host:
        raise SystemExit(
            "Set EDMINGLE_WORKER_API_BASE (recommended) or EDMINGLE_API_HOST + EDMINGLE_API_PATH_PREFIX."
        )
    return f"{protocol}://{host}{path}"


def _org_id() -> str:
    org_id = (os.environ.get("ORGID") or os.environ.get("EDMINGLE_ORG_ID") or "").strip()
    if not org_id:
        raise SystemExit("Set ORGID (or EDMINGLE_ORG_ID) — same as browser ORGID header.")
    return org_id


def _api_key() -> str:
    api_key = (os.environ.get("APIKEY") or os.environ.get("EDMINGLE_API_KEY") or "").strip()
    if not api_key:
        raise SystemExit("Set APIKEY (or EDMINGLE_API_KEY) — same as browser APIKEY header.")
    return api_key


def _default_session_headers() -> Dict[str, str]:
    return {
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "APIKEY": _api_key(),
        "ORGID": _org_id(),
    }


def _institution_id_for_next() -> int:
    raw = (os.environ.get("DRM_MIGRATION_INSTITUTION_ID") or "0").strip()
    try:
        return int(raw)
    except ValueError:
        return 0


def _job_gap_sec() -> float:
    raw = (os.environ.get("DRM_MIGRATION_JOB_GAP_SEC") or "1.5").strip() or "1.5"
    try:
        v = float(raw)
    except ValueError:
        return 1.5
    return max(0.0, v)


def _merge_method() -> str:
    raw = (
        os.environ.get("DRM_MIGRATION_MERGE_METHOD") or os.environ.get("BUNNY_MERGE_METHOD") or "hls"
    ).strip().lower()
    if raw not in ("hls", "zip"):
        raise SystemExit(
            "DRM_MIGRATION_MERGE_METHOD (or BUNNY_MERGE_METHOD) must be 'hls' or 'zip'"
        )
    return raw


def _bunny_access_key_for_job(job: Dict[str, Any]) -> str:
    """Bunny Storage / Edge password from ``worker/next`` ``bunny`` object only."""
    bn = _bunny(job)
    v = _pick(bn, "drm_bunny_access_key", "drmBunnyAccessKey")
    if v is None:
        return ""
    return str(v).strip()


def _storage_zip_access_key_for_job(job: Dict[str, Any]) -> str:
    """Password for ``GET https://storage.bunnycdn.com/{zone}/{drm_id}/?...`` ZIP download."""
    key = _bunny_access_key_for_job(job)
    if not key:
        raise RuntimeError(
            "ZIP merge needs a non-empty bunny.drm_bunny_access_key on the worker/next job "
            "(Bunny Storage API password for the zone). Bunny credentials are not read from the environment."
        )
    return key


def _zip_job_inputs(job: Dict[str, Any]) -> Tuple[str, str, str]:
    """
    Returns ``(storage_zone_name, drm_id_as_guid, storage_access_key)``.

    Zone, ``drm_id``, and Storage auth all come from ``worker/next`` (``bunny.drm_bunny_access_key``).
    """
    row = _migration_row(job)
    bn = _bunny(job)
    drm_id = _pick(row, "drm_id", "drmId")
    zone = _pick(
        bn,
        "drm_bunny_storagezonename",
        "drmBunnyStoragezonename",
        "storage_zone",
        "storageZone",
    )
    if not drm_id or not zone:
        raise RuntimeError(
            "ZIP merge needs migration_row.drm_id and bunny.drm_bunny_storagezonename from worker/next"
        )
    storage_key = _storage_zip_access_key_for_job(job)
    return (str(zone).strip(), str(drm_id).strip(), storage_key)


async def _merge_zip_job_to_mp4(
    session: aiohttp.ClientSession,
    job: Dict[str, Any],
    work_dir: Path,
    out_mp4: Path,
    timeout: aiohttp.ClientTimeout,
) -> None:
    zone, guid, storage_key = _zip_job_inputs(job)
    LOG.info("ZIP merge: storage_zone=%r drm_id=%r (auth from job bunny.drm_bunny_access_key)", zone, guid)
    await bunny._attempt_zip_to_mp4(
        session,
        video_guid=guid,
        access_key=storage_key,
        storage_zone=zone,
        work_dir=work_dir,
        out_path=out_mp4,
        timeout=timeout,
        download_bare=True,
    )


def _s3_credentials_from_env() -> Optional[Tuple[str, str]]:
    ak = (os.environ.get("drm_migration_s3_access_key") or "").strip()
    sk = (os.environ.get("drm_migration_s3_secret_key") or "").strip()
    if not ak or not sk:
        return None
    return (ak, sk)


def _next_url() -> str:
    return f"{_api_base()}/drmvideo/migration/worker/next"


def _report_url() -> str:
    return f"{_api_base()}/drmvideo/migration/worker/report"


async def _post_json_string(
    session: aiohttp.ClientSession,
    url: str,
    inner: Dict[str, Any],
    timeout: aiohttp.ClientTimeout,
) -> Dict[str, Any]:
    """POST multipart form (``curl --form``) with field ``JSONString`` = JSON object string."""
    form = aiohttp.FormData()
    form.add_field("JSONString", json.dumps(inner, separators=(",", ":")))
    async with session.post(url, data=form, timeout=timeout) as resp:
        text = await resp.text()
        if resp.status >= 400:
            raise RuntimeError(f"HTTP {resp.status} from {url!r}: {text[:1200]!r}")
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Non-JSON response from {url!r}: {text[:500]!r}") from exc
    return payload


def _inner_next_payload() -> Dict[str, Any]:
    """Matches Edmingle ``worker/next`` JSONString (``institution_id``)."""
    return {"institution_id": _institution_id_for_next()}


def _inner_report_payload(
    migration_id: int, outcome: str, error_message: Optional[str]
) -> Dict[str, Any]:
    inner: Dict[str, Any] = {
        "migration_id": int(migration_id),
        "outcome": outcome.strip().lower(),
    }
    if inner["outcome"] == "failure":
        inner["error_message"] = _trunc_err((error_message or "unknown error").strip())
    return inner


async def _call_next(
    session: aiohttp.ClientSession, timeout: aiohttp.ClientTimeout
) -> Dict[str, Any]:
    return await _post_json_string(session, _next_url(), _inner_next_payload(), timeout)


async def _call_report(
    session: aiohttp.ClientSession,
    *,
    migration_id: int,
    outcome: str,
    error_message: Optional[str],
    timeout: aiohttp.ClientTimeout,
) -> Dict[str, Any]:
    return await _post_json_string(
        session,
        _report_url(),
        _inner_report_payload(migration_id, outcome, error_message),
        timeout,
    )


def _unwrap_envelope(payload: Dict[str, Any], *, context: str) -> Dict[str, Any]:
    code = payload.get("code")
    try:
        code_int = int(code) if code is not None else -1
    except (TypeError, ValueError):
        code_int = -1
    if code_int != 200:
        raise RuntimeError(f"{context}: API code={code!r} message={payload.get('message')!r}")
    return (payload.get("data") or {}) if isinstance(payload.get("data"), dict) else {}


def _parse_next_job(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    hj = data.get("has_job")
    if hj not in (True, "true", "True", 1, "1"):
        return None
    job = data.get("job")
    if not isinstance(job, dict):
        return None
    return job


def _migration_id(job: Dict[str, Any]) -> int:
    row = job.get("migration_row")
    if not isinstance(row, dict) or row.get("id") is None:
        raise RuntimeError("job.migration_row.id missing in next response")
    return int(row["id"])


def _pick(d: Optional[Dict[str, Any]], *keys: str) -> Any:
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k not in d:
            continue
        v = d.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return v
    return None


def _migration_row(job: Dict[str, Any]) -> Dict[str, Any]:
    row = job.get("migration_row")
    if not isinstance(row, dict):
        raise RuntimeError("job.migration_row missing or not an object")
    return row


def _bunny(job: Dict[str, Any]) -> Dict[str, Any]:
    b = job.get("bunny")
    return b if isinstance(b, dict) else {}


def cdn_base_from_storage_zone(zone: str) -> str:
    """
    Bunny Stream CDN host from pull/storage zone label, e.g. ``vz-6c2fc224-1bf`` →
    ``https://vz-6c2fc224-1bf.b-cdn.net`` (same pattern as ``BUNNY_STREAM_CDN_BASE`` in env-based flows).
    """
    z = (zone or "").strip().strip("/")
    if not z:
        raise ValueError("empty drm_bunny_storagezonename for CDN base")
    low = z.lower()
    if low.startswith("http://") or low.startswith("https://"):
        return z.rstrip("/")
    return f"https://{z}.b-cdn.net"


def _edge_storage_base() -> str:
    return (os.environ.get("BUNNY_EDGE_STORAGE_BASE_URL") or "").strip().rstrip("/") or bunny.STORAGE_API_BASE


def _edge_storage_master_playlist_url(zone: str, drm_id: str, password: str) -> str:
    z = (zone or "").strip().strip("/")
    d = (drm_id or "").strip()
    key_q = quote(str(password), safe="")
    base = f"{_edge_storage_base().rstrip('/')}/{z}/{d}/playlist.m3u8"
    return f"{base}?accessKey={key_q}"


def _stream_cdn_master_playlist_url(zone: str, drm_id: str) -> str:
    """Public Stream master on pull zone CDN (no storage accessKey query param)."""
    base = cdn_base_from_storage_zone(zone)
    return f"{base.rstrip('/')}/{str(drm_id).strip()}/playlist.m3u8"


def _resolve_hls_master_url(job: Dict[str, Any]) -> str:
    """
    HLS master URL for ffmpeg.

    Rules:
    - If the API already provides ``job.hls_playlist_url``, use it.
    - Otherwise, always use the **Stream CDN** master on ``*.b-cdn.net``.

    IMPORTANT: ``bunny.drm_bunny_access_key`` is a **Storage** password and is used for the
    **ZIP** flow only. Do not route HLS through Edge Storage ``storage.bunnycdn.com``.
    """
    u = _pick(job, "hls_playlist_url", "hlsPlaylistUrl")
    if u is not None:
        return str(u).strip()

    row = _migration_row(job)
    drm_id = _pick(row, "drm_id", "drmId")
    if drm_id is None:
        raise RuntimeError("migration_row.drm_id missing and hls_playlist_url empty")

    bn = _bunny(job)
    zone = _pick(
        bn,
        "drm_bunny_storagezonename",
        "drmBunnyStoragezonename",
        "storage_zone",
        "storageZone",
    )
    if not zone:
        raise RuntimeError("hls_playlist_url empty and drm_bunny_storagezonename missing")
    return _stream_cdn_master_playlist_url(str(zone), str(drm_id))


def _log_job_bunny_context(job: Dict[str, Any]) -> None:
    """Log non-secret context from API (CDN base from zone, ids)."""
    try:
        row = _migration_row(job)
        drm_id = _pick(row, "drm_id", "drmId")
        bn = _bunny(job)
        zone = _pick(bn, "drm_bunny_storagezonename", "drmBunnyStoragezonename")
        lib = _pick(bn, "drm_bunny_libraryid", "drmBunnyLibraryid", "drm_bunny_library_id")
        if zone:
            try:
                cdn = cdn_base_from_storage_zone(str(zone))
            except ValueError:
                cdn = "(invalid zone)"
        else:
            cdn = "(no zone)"
        LOG.info(
            "Job context: drm_id=%r library_id=%r derived_stream_cdn_base=%s",
            drm_id,
            lib,
            cdn,
        )
    except Exception as exc:
        LOG.debug("Could not log job context: %s", exc)


def _s3_from_job(job: Dict[str, Any]) -> Tuple[str, str, str]:
    s3 = job.get("s3")
    if not isinstance(s3, dict):
        raise RuntimeError("job.s3 missing in next response")
    bucket = (s3.get("bucket") or "").strip() or (os.environ.get("drm_migration_s3_bucket_name") or "").strip()
    region = (s3.get("region") or "us-east-1").strip()
    key = (s3.get("object_key") or s3.get("objectKey") or "").strip()
    if not key:
        raise RuntimeError("job.s3.object_key missing in next response")
    if not bucket:
        raise RuntimeError(
            "job.s3.bucket is empty and drm_migration_s3_bucket_name is not set in env"
        )
    return bucket, region, key


async def _merge_hls_master_to_mp4(
    session: aiohttp.ClientSession,
    master_url: str,
    out_mp4: Path,
    work_dir: Path,
    timeout: aiohttp.ClientTimeout,
) -> None:
    async with session.get(master_url, allow_redirects=True, timeout=timeout) as resp:
        resp.raise_for_status()
        master_text = await resp.text()
    variant_url = bunny.pick_best_variant_url(master_text, master_url)
    async with session.get(variant_url, allow_redirects=True, timeout=timeout) as resp:
        resp.raise_for_status()
        variant_text = await resp.text()
    ffmpeg_in = bunny.hls_variant_url_or_local_playlist_for_ffmpeg(
        variant_url, variant_text, work_dir
    )
    if ffmpeg_in.rstrip("/") != variant_url.strip().rstrip("/"):
        LOG.info(
            "HLS signed variant: using rewritten local playlist for ffmpeg (-i %s)",
            ffmpeg_in,
        )
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, bunny.ffmpeg_hls_remux_to_mp4, ffmpeg_in, out_mp4)


async def _report_safe(
    session: aiohttp.ClientSession,
    *,
    migration_id: int,
    outcome: str,
    error_message: Optional[str],
    timeout: aiohttp.ClientTimeout,
) -> None:
    """POST worker/report; log and swallow errors so the worker loop can continue."""
    try:
        await _call_report(
            session,
            migration_id=migration_id,
            outcome=outcome,
            error_message=error_message,
            timeout=timeout,
        )
        LOG.info("worker/report ok migration_id=%s outcome=%s", migration_id, outcome)
    except Exception as exc:
        LOG.error(
            "worker/report failed migration_id=%s outcome=%s: %s",
            migration_id,
            outcome,
            exc,
            exc_info=True,
        )


async def _process_migration_job(
    session: aiohttp.ClientSession,
    job: Dict[str, Any],
    local_out: Path,
    timeout: aiohttp.ClientTimeout,
) -> bool:
    """
    Merge → S3 (or local fallback) → worker/report success.

    Returns True only if S3 upload succeeded and ``worker/report`` returned 200.
    On any failure, sends ``worker/report`` with ``outcome=failure`` and ``error_message``,
    then returns False.

    The per-job ``work_dir`` under the system temp directory is always removed in a
    ``finally`` block (merge/HLS/S3/report failures, success, and interrupt) so ZIPs,
    extracted trees, local HLS playlists, and ``merged.mp4`` do not accumulate on disk.
    """
    migration_id = _migration_id(job)
    LOG.info("Picked migration_id=%s", migration_id)
    _log_job_bunny_context(job)

    merge_method = _merge_method()
    LOG.info("Merge method: %s", merge_method)

    work_dir = Path(tempfile.mkdtemp(prefix="drm_migration_", dir=None))
    out_mp4 = work_dir / "merged.mp4"

    try:
        try:
            if merge_method == "zip":
                await _merge_zip_job_to_mp4(session, job, work_dir, out_mp4, timeout)
            else:
                master_url = _resolve_hls_master_url(job)
                redacted = (
                    master_url.split("?", 1)[0] + "?accessKey=***"
                    if "?" in master_url
                    else master_url
                )
                LOG.info("HLS master: %s", redacted)
                await _merge_hls_master_to_mp4(session, master_url, out_mp4, work_dir, timeout)
        except Exception as exc:
            LOG.exception("Merge failed migration_id=%s", migration_id)
            err = f"merge failed: {exc}"
            low = str(exc).lower()
            if "401" in str(exc) or "unauthorized" in low:
                err += (
                    " — Bunny/Storage authorization failed: check bunny.drm_bunny_access_key "
                    "and paths from worker/next (wrong API data yields 401 on playlist or segments)."
                )
            if "errno 28" in low or "no space left on device" in low:
                err += " — Disk full under temp (often /tmp): free space, enlarge volume, or set TMPDIR to a larger mount."
            await _report_safe(
                session,
                migration_id=migration_id,
                outcome="failure",
                error_message=err,
                timeout=timeout,
            )
            return False

        if not out_mp4.is_file() or out_mp4.stat().st_size <= 0:
            await _report_safe(
                session,
                migration_id=migration_id,
                outcome="failure",
                error_message="merge produced empty MP4",
                timeout=timeout,
            )
            return False

        try:
            bucket, region, object_key = _s3_from_job(job)
        except Exception as exc:
            await _report_safe(
                session,
                migration_id=migration_id,
                outcome="failure",
                error_message=str(exc),
                timeout=timeout,
            )
            return False

        creds = _s3_credentials_from_env()
        if creds is None:
            local_out.mkdir(parents=True, exist_ok=True)
            dest = local_out / f"{migration_id}_merged.mp4"
            shutil.move(str(out_mp4), str(dest))
            msg = (
                "LOCAL_OUTPUT_NO_S3: drm_migration_s3_access_key / drm_migration_s3_secret_key "
                f"not set; MP4 saved at {dest}"
            )
            LOG.warning(msg)
            await _report_safe(
                session,
                migration_id=migration_id,
                outcome="failure",
                error_message=msg,
                timeout=timeout,
            )
            return False

        ak, sk = creds
        client = drm_s3_client_from_keys(region, ak, sk)
        try:
            if head_object_nonzero_size(client, bucket, object_key):
                LOG.info("S3 object already present: s3://%s/%s", bucket, object_key)
            else:
                put_mp4_private_then_retry_no_acl(client, bucket, object_key, out_mp4)
                verify_object_nonzero_after_put(client, bucket, object_key)
                LOG.info("Uploaded s3://%s/%s", bucket, object_key)
        except Exception as exc:
            LOG.exception("S3 failed migration_id=%s", migration_id)
            if is_s3_auth_or_config_failure(exc):
                msg = f"S3 authentication or configuration failure: {exc}"
            else:
                msg = f"S3 upload/verify failed: {exc}"
            await _report_safe(
                session,
                migration_id=migration_id,
                outcome="failure",
                error_message=msg,
                timeout=timeout,
            )
            return False

        try:
            raw_rep = await _call_report(
                session,
                migration_id=migration_id,
                outcome="success",
                error_message=None,
                timeout=timeout,
            )
            rep_data = _unwrap_envelope(raw_rep, context="worker/report")
            LOG.info(
                "Report success: row_updated=%s merge_status=%s",
                rep_data.get("row_updated"),
                rep_data.get("merge_status"),
            )
        except Exception as exc:
            LOG.error(
                "worker/report failed after successful S3 upload migration_id=%s: %s",
                migration_id,
                exc,
                exc_info=True,
            )
            return False

        return True
    finally:
        if work_dir.is_dir():
            shutil.rmtree(work_dir, ignore_errors=True)


async def _run_async() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    d = deploy_dir()
    load_env_file(d / ".env")
    _api_base()
    _api_key()
    _org_id()

    local_out = Path(
        (os.environ.get("DRM_MIGRATION_LOCAL_OUTPUT_DIR") or "./drm_migration_output").strip()
        or "./drm_migration_output"
    )
    if not local_out.is_absolute():
        local_out = (d / local_out).resolve()
    # Created on demand when saving MP4 without S3 IAM keys (LOCAL_OUTPUT_NO_S3 path).
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=600)
    connector = aiohttp.TCPConnector(limit=8)
    headers = _default_session_headers()

    had_job_failure = False
    gap = _job_gap_sec()
    first_next = True

    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        while True:
            if not first_next and gap > 0:
                LOG.info("Waiting %.2fs before worker/next", gap)
                await asyncio.sleep(gap)
            first_next = False

            try:
                raw_next = await _call_next(session, timeout)
            except Exception as exc:
                LOG.error("worker/next request failed: %s", exc, exc_info=True)
                print(f"FATAL: worker/next failed: {exc}", file=sys.stderr)
                return 2

            try:
                data_next = _unwrap_envelope(raw_next, context="worker/next")
            except Exception as exc:
                LOG.error("worker/next bad envelope: %s raw=%s", exc, raw_next)
                print(f"FATAL: {exc}", file=sys.stderr)
                return 2

            job = _parse_next_job(data_next)
            if not job:
                LOG.info("No job (has_job=false). Idle exit.")
                break

            job_ok = False
            try:
                job_ok = await _process_migration_job(session, job, local_out, timeout)
            except Exception as exc:
                LOG.exception("Unhandled error while processing job (reporting failure)")
                try:
                    mid = _migration_id(job)
                    await _report_safe(
                        session,
                        migration_id=mid,
                        outcome="failure",
                        error_message=_trunc_err(str(exc)),
                        timeout=timeout,
                    )
                except Exception as report_exc:
                    LOG.error(
                        "Could not report failure for job (migration_id unknown or report failed): %s",
                        report_exc,
                        exc_info=True,
                    )
                had_job_failure = True
            else:
                if not job_ok:
                    had_job_failure = True
                    LOG.info("Job finished with failure; next worker/next after gap.")

    return 1 if had_job_failure else 0


def main() -> None:
    try:
        rc = asyncio.run(_run_async())
    except KeyboardInterrupt:
        rc = 130
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
