"""Optional S3 upload after merge. Configure with env (see ``.env.example``)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class S3UploadConfig:
    bucket: str
    key_prefix: str  # normalized: "" or "folder/sub/"
    region: str
    endpoint_url: Optional[str]


def load_s3_upload_config() -> Optional[S3UploadConfig]:
    """Return config if ``BUNNY_S3_BUCKET`` is set; otherwise uploads are disabled."""
    bucket = (os.environ.get("BUNNY_S3_BUCKET") or "").strip()
    if not bucket:
        return None
    raw_prefix = (os.environ.get("BUNNY_S3_PREFIX") or "").strip().strip("/")
    key_prefix = f"{raw_prefix}/" if raw_prefix else ""
    region = (
        os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
    ).strip()
    # Not needed for normal AWS S3; only for S3-compatible APIs (MinIO, etc.).
    endpoint_url = (os.environ.get("BUNNY_S3_ENDPOINT_URL") or "").strip() or None
    return S3UploadConfig(
        bucket=bucket,
        key_prefix=key_prefix,
        region=region,
        endpoint_url=endpoint_url,
    )


def upload_local_mp4(cfg: S3UploadConfig, local_path: Path) -> str:
    """
    Upload ``local_path`` to ``s3://bucket/{key_prefix}{filename}``.

    Uses standard boto3 credential env vars (``AWS_ACCESS_KEY_ID``,
    ``AWS_SECRET_ACCESS_KEY``, and optionally ``AWS_SESSION_TOKEN`` for STS).

    After a successful upload, the local file is always removed.

    Returns the ``s3://`` URI of the object.
    """
    try:
        import boto3
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("Install boto3 for S3 upload: pip install boto3") from exc

    if not local_path.is_file():
        raise FileNotFoundError(f"Cannot upload missing file: {local_path}")

    key = f"{cfg.key_prefix}{local_path.name}"
    session = boto3.session.Session(region_name=cfg.region)
    client_kwargs: dict = {}
    if cfg.endpoint_url:
        client_kwargs["endpoint_url"] = cfg.endpoint_url
    client = session.client("s3", **client_kwargs)
    extra = {"ContentType": "video/mp4"}
    client.upload_file(str(local_path.resolve()), cfg.bucket, key, ExtraArgs=extra)
    uri = f"s3://{cfg.bucket}/{key}"
    local_path.unlink(missing_ok=True)
    return uri
