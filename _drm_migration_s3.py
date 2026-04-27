"""Edmingle DRM migration bridge bucket: HeadObject idempotency, PutObject with optional ACL."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

LOG = logging.getLogger("drm_migration.s3")


@dataclass(frozen=True)
class DrmMigrationS3Config:
    bucket: str
    region: str
    access_key: str
    secret_key: str


def load_drm_migration_s3_config() -> Optional[DrmMigrationS3Config]:
    """Same env names as Edmingle PHP ``S3Util`` / ``EDMINGLE_CONFIG_PROPERTIES``."""
    bucket = (os.environ.get("drm_migration_s3_bucket_name") or "").strip()
    ak = (os.environ.get("drm_migration_s3_access_key") or "").strip()
    sk = (os.environ.get("drm_migration_s3_secret_key") or "").strip()
    if not bucket and not ak and not sk:
        return None
    if not (bucket and ak and sk):
        raise ValueError(
            "Incomplete DRM migration S3 env: set drm_migration_s3_bucket_name, "
            "drm_migration_s3_access_key, and drm_migration_s3_secret_key together, "
            "or leave all three unset for local-output-only mode."
        )
    region = (os.environ.get("drm_migration_s3_region") or "us-east-1").strip()
    return DrmMigrationS3Config(bucket=bucket, region=region, access_key=ak, secret_key=sk)


def drm_s3_client(cfg: DrmMigrationS3Config):
    return drm_s3_client_from_keys(cfg.region, cfg.access_key, cfg.secret_key)


def drm_s3_client_from_keys(region: str, access_key: str, secret_key: str):
    """S3 client for bridge upload; ``bucket`` is supplied per request (from API job)."""
    try:
        import boto3
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("Install boto3: pip install boto3") from exc
    return boto3.client(
        "s3",
        region_name=(region or "us-east-1").strip(),
        aws_access_key_id=access_key.strip(),
        aws_secret_access_key=secret_key.strip(),
    )


def is_s3_auth_or_config_failure(exc: BaseException) -> bool:
    """True when retrying other rows will not help (bad keys, signature, denied)."""
    try:
        from botocore.exceptions import ClientError
    except ImportError:
        return False
    if isinstance(exc, ClientError):
        code = (exc.response.get("Error") or {}).get("Code") or ""
        if code in (
            "InvalidAccessKeyId",
            "SignatureDoesNotMatch",
            "InvalidToken",
            "AccessDenied",
            "ExpiredToken",
        ):
            return True
        if exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode") in (401, 403):
            return True
    low = str(exc).lower()
    if "accessdenied" in low or "signaturedoesnotmatch" in low or "invalidaccesskeyid" in low:
        return True
    return False


def head_object_nonzero_size(client: Any, bucket: str, key: str) -> bool:
    from botocore.exceptions import ClientError

    try:
        r = client.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        err = e.response.get("Error", {}) or {}
        if err.get("Code") in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
    size = int(r.get("ContentLength") or 0)
    return size > 0


def _s3_upload_transfer_config() -> Any:
    """
    Optional multipart tuning (larger MP4s). Env:
    ``DRM_MIGRATION_S3_UPLOAD_MAX_CONCURRENCY`` (default 16, max 32),
    ``DRM_MIGRATION_S3_MULTIPART_CHUNKSIZE_BYTES`` (default 16 MiB, max 256 MiB).
    """
    try:
        from boto3.s3.transfer import TransferConfig
    except ImportError:  # pragma: no cover
        return None
    try:
        conc = int((os.environ.get("DRM_MIGRATION_S3_UPLOAD_MAX_CONCURRENCY") or "16").strip())
    except ValueError:
        conc = 16
    try:
        chunk = int(
            (os.environ.get("DRM_MIGRATION_S3_MULTIPART_CHUNKSIZE_BYTES") or str(16 * 1024 * 1024)).strip()
        )
    except ValueError:
        chunk = 16 * 1024 * 1024
    conc = max(1, min(conc, 32))
    chunk = max(5 * 1024 * 1024, min(chunk, 256 * 1024 * 1024))
    return TransferConfig(
        multipart_threshold=8 * 1024 * 1024,
        multipart_chunksize=chunk,
        max_concurrency=conc,
        use_threads=True,
    )


def put_mp4_private_then_retry_no_acl(client: Any, bucket: str, key: str, local_path: Path) -> None:
    """
    PutObject stream copy from disk; ``Content-Type: video/mp4``.
    Try ``ACL: private`` first; if bucket rejects ACL, retry without ACL (Edmingle pattern).
    """
    path = str(local_path.resolve())
    extra_with_acl = {"ContentType": "video/mp4", "ACL": "private"}
    extra_plain = {"ContentType": "video/mp4"}
    xfer = _s3_upload_transfer_config()
    xfer_kw: dict[str, Any] = {}
    if xfer is not None:
        xfer_kw["Config"] = xfer
    try:
        client.upload_file(path, bucket, key, ExtraArgs=extra_with_acl, **xfer_kw)
        LOG.debug("PutObject succeeded with ACL private: s3://%s/%s", bucket, key)
        return
    except Exception as exc:  # pylint: disable=broad-exception-caught
        low = str(exc).lower()
        if "acl" in low or "access control" in low or "cannedacl" in low or "not supported" in low:
            LOG.warning("PutObject with ACL private failed (%s); retrying without ACL", exc)
            client.upload_file(path, bucket, key, ExtraArgs=extra_plain, **xfer_kw)
            LOG.info("PutObject succeeded without ACL: s3://%s/%s", bucket, key)
            return
        raise


def verify_object_nonzero_after_put(client: Any, bucket: str, key: str) -> int:
    r = client.head_object(Bucket=bucket, Key=key)
    n = int(r.get("ContentLength") or 0)
    if n <= 0:
        raise RuntimeError(f"S3 HeadObject after Put reports empty object: s3://{bucket}/{key}")
    return n


def build_migration_object_key(s3_path_prefix: str, s3_file_name: str) -> str:
    p = (s3_path_prefix or "").strip().rstrip("/")
    f = (s3_file_name or "").strip().lstrip("/")
    if p and f:
        return f"{p}/{f}"
    if f:
        return f
    raise ValueError("empty s3_file_name for object key")
