# Bunny HLS merge tool

Merge [Bunny.net Stream](https://bunny.net/stream) output into a single **MP4** using **stream copy** (no re-encode): either by downloading the video folder as a **Storage ZIP**, or by **remuxing HLS** with ffmpeg.

## Requirements

- **Python 3.9+**
- **ffmpeg** on your `PATH`
- Python packages: **aiohttp**, **boto3** (S3 / DRM bridge upload)

## Quick setup

```bash
cd bunny_hls_merge_tool
python3 -m venv .venv
./.venv/bin/pip install --upgrade pip
./.venv/bin/pip install "aiohttp>=3.9.0" "boto3>=1.34.0"
```

The `run_*.py` helpers prefer `./.venv/bin/python3` when a `.venv` exists.

### Optional: EC2 / Linux bootstrap

From this directory:

```bash
chmod +x setup_ec2.sh
./setup_ec2.sh
```

This installs OS packages (where `apt` or `yum` apply), creates `.venv`, installs Python deps (`aiohttp`, `boto3`), and prints next steps.

## Configuration

```bash
cp .env.example .env
```

**`.env.example`** lists only what **`drm_hls_migration_worker.py`** needs (Edmingle API base, headers, optional Edge Storage override, DRM bridge S3 IAM keys, local output dir). It does **not** include `BUNNY_STREAM_CDN_BASE`, library id, or video GUID: those come from **`worker/next`** (`migration_row`, `bunny`, optional `hls_playlist_url`).

### Edmingle DRM worker env (see `.env.example`)

| Variable | Purpose |
|----------|---------|
| `EDMINGLE_WORKER_API_BASE` | Optional full API root, e.g. `http://localhost/nuSource/api/v1` (overrides split vars) |
| `EDMINGLE_API_PROTOCOL` / `EDMINGLE_API_HOST` / `EDMINGLE_API_PATH_PREFIX` | Build base when `EDMINGLE_WORKER_API_BASE` unset (default `http` + `localhost` + `/nuSource/api/v1`) |
| `APIKEY` (or `EDMINGLE_API_KEY`) | Same as browser `APIKEY` header |
| `ORGID` (or `EDMINGLE_ORG_ID`) | Same as browser `ORGID` header |
| `DRM_MIGRATION_INSTITUTION_ID` | Sent as `institution_id` in `JSONString` for `worker/next` |
| `DRM_MIGRATION_MERGE_METHOD` | `hls` (default) or `zip` — same meaning as legacy `BUNNY_MERGE_METHOD` |
| `DRM_MIGRATION_LOCAL_OUTPUT_DIR` | Where to put the MP4 **only** when S3 IAM keys are unset (fallback save + `LOCAL_OUTPUT_NO_S3`); merge scratch uses OS temp |
| `DRM_MIGRATION_JOB_GAP_SEC` | Seconds to wait before each **`worker/next`** after the first (default `1.5`) |
| `drm_migration_s3_bucket_name` | Fallback when `job.s3.bucket` is empty |
| `drm_migration_s3_access_key` / `drm_migration_s3_secret_key` | IAM for S3 upload |

Requests use **multipart** form data with field **`JSONString`** (like `curl --form`), plus headers **`X-Requested-With`**, **`Accept`**, **`APIKEY`**, **`ORGID`**.

**Merge (after `worker/next`)** — mode from `DRM_MIGRATION_MERGE_METHOD` or `BUNNY_MERGE_METHOD` (`hls` default):

- **`hls`**: non-empty **`hls_playlist_url`**; else **Stream CDN** `https://{zone}.b-cdn.net/{drm_id}/playlist.m3u8`, then ffmpeg remux. No Bunny auth key is used for HLS.
- **`zip`**: download Storage ZIP for `/{drm_bunny_storagezonename}/{drm_id}/` using **`bunny.drm_bunny_access_key`** from **`worker/next`** only (Bunny Storage / Edge password). Not read from `.env`.

### `run_merge.py` / `run_zip_merge.py` / `run_hls_merge.py`

These are thin wrappers around **`drm_hls_migration_worker.py`**: they load `.env`, set merge mode (`run_zip_merge` → `zip`, `run_hls_merge` → `hls`, `run_merge` → from env), then run the same **worker/next → merge → S3 → worker/report** loop. They do **not** require `BUNNY_STREAM_*` or `BUNNY_VIDEO_GUID`. **ZIP** and **HLS** Edge paths use **`bunny.drm_bunny_access_key`** from the job only.

**Legacy:** **`_merge_cli.py`** builds an HLS-only argv from **`BUNNY_STREAM_*`** / **`BUNNY_VIDEO_GUID`**. **`bunny_stream_hls_merge_to_mp4.py`** can also be run by hand; ZIP there uses **`--storage-access-key`** (not `BUNNY_STORAGE_ACCESS_KEY`, which is unused).

### Optional: upload to S3 after merge

If **`BUNNY_S3_BUCKET`** is set, each successful merge is uploaded to  
`s3://{BUNNY_S3_BUCKET}/{BUNNY_S3_PREFIX}{filename}.mp4`,  
then the **local MP4 is always deleted** (only after a successful upload).

Required in `.env` for normal AWS S3:

| Variable | Purpose |
|----------|---------|
| `BUNNY_S3_BUCKET` | Bucket name (leave empty to disable S3) |
| `BUNNY_S3_PREFIX` | Optional object key prefix (no leading `/`) |
| `AWS_ACCESS_KEY_ID` | IAM access key |
| `AWS_SECRET_ACCESS_KEY` | IAM secret |
| `AWS_REGION` | Region (defaults to `us-east-1` if unset) |

You do **not** need `AWS_SESSION_TOKEN`, `BUNNY_S3_ENDPOINT_URL`, or any “delete local” flag for the usual case. If you use temporary STS credentials, add `AWS_SESSION_TOKEN` to `.env` yourself—boto3 reads it automatically. For MinIO or another S3-compatible endpoint, you can add `BUNNY_S3_ENDPOINT_URL` to `.env` (not listed in `.env.example`).

If S3 upload fails, the local MP4 is kept and the video is recorded as **failed** in the progress file (not completed), so you can fix credentials or bucket policy and retry.

Requires **`pip install boto3`** (included in `setup_ec2.sh`).

### `DRM_MIGRATION_MERGE_METHOD` (ZIP vs HLS, worker + `run_merge.py`)

| Variable | Default | Description |
|----------|---------|-------------|
| `DRM_MIGRATION_MERGE_METHOD` | `hls` | `drm_hls_migration_worker.py` / `run_merge.py`: `hls` or `zip` (credentials from `worker/next`). |
| `BUNNY_MERGE_METHOD` | — | Optional alias for the same setting (legacy name). |

`run_zip_merge.py` / `run_hls_merge.py` force `zip` / `hls` regardless of env.

### Edmingle DRM migration worker (`drm_hls_migration_worker.py`)

**No database access in the worker.** Edmingle exposes two HTTP routes; the worker only calls those.

1. **POST** `{api_base}/drmvideo/migration/worker/next` — multipart field **`JSONString`** (same as `curl --form`). Body includes **`institution_id`**. Headers **`APIKEY`**, **`ORGID`**, **`X-Requested-With`**, **`Accept`** match browser calls.

   Success (`code` 200): if `data.has_job` is false, the worker **stops** (idle). If true, `data.job` matches your API shape (`migration_row`, `bunny`, `hls_playlist_url`, `s3`, …).

2. **Merge**: if **`DRM_MIGRATION_MERGE_METHOD`** (or **`BUNNY_MERGE_METHOD`**) is **`zip`**, download the Storage ZIP for the job’s zone + `drm_id` using **`bunny.drm_bunny_access_key`** from **`worker/next`** (not env). If **`hls`** (default), resolve an HLS master URL and remux with ffmpeg.

3. **Upload** to `s3://{bucket}/{object_key}` — **`object_key`** from job; **`bucket`** from job or env **`drm_migration_s3_bucket_name`** when API returns empty bucket. IAM keys from env.

4. **POST** `{api_base}/drmvideo/migration/worker/report` — same multipart **`JSONString`** pattern with `migration_id`, `outcome` (`success` or `failure`), and `error_message` on failure.

5. **Loop**: wait **`DRM_MIGRATION_JOB_GAP_SEC`** (default **1.5s**), then call **`worker/next`** again. A failed merge, S3 step, or missing IAM still **reports failure** with **`error_message`**, then the worker **continues**. Only **`worker/next`** transport or non-200 envelope errors exit the process immediately. Uncaught exceptions still trigger **`worker/report`** when **`migration_id`** is available.

**Exit codes:** **0** — idle (no job) or all jobs in the run succeeded; **1** — at least one job failed (after reporting); **2** — could not call or parse **`worker/next`**.

Having **`job.s3.bucket`** (or env bucket) is not enough: upload needs **`drm_migration_s3_access_key`** / **`drm_migration_s3_secret_key`**. If those IAM keys are **not** set, the MP4 is moved under **`DRM_MIGRATION_LOCAL_OUTPUT_DIR`** and the worker **reports failure** with `LOCAL_OUTPUT_NO_S3:…` so Edmingle can mark the row failed.

## How to run

### Edmingle worker (recommended)

```bash
./.venv/bin/python3 drm_hls_migration_worker.py
```

Same **worker/next → merge → S3 → worker/report** flow via helpers (merge mode from env, or forced):

```bash
./.venv/bin/python3 run_merge.py          # DRM_MIGRATION_MERGE_METHOD or BUNNY_MERGE_METHOD (default hls)
./.venv/bin/python3 run_zip_merge.py      # always zip (zone + drm_id + drm_bunny_access_key from job)
./.venv/bin/python3 run_hls_merge.py      # always hls
```

While merging, work files live under the **OS temp directory**; after a successful S3 upload the scratch dir is removed. **`DRM_MIGRATION_LOCAL_OUTPUT_DIR`** is not used on the happy path.

### Run the core script directly (legacy, no Edmingle)

`bunny_stream_hls_merge_to_mp4.py` is still available for **library batch** or ad-hoc CLI; pass **library / Stream key / CDN / video GUID** on the command line (optional env overrides exist for that script only). The Edmingle wrappers above do **not** use that path.

For full control (batch library, dry-run, custom paths), run it yourself:

```bash
./.venv/bin/python3 bunny_stream_hls_merge_to_mp4.py --help
```

Examples:

```bash
# Single video, default ZIP then HLS fallback
./.venv/bin/python3 bunny_stream_hls_merge_to_mp4.py \
  --library-id YOUR_ID --access-key YOUR_STREAM_KEY \
  --cdn-base https://vz-xxxx.b-cdn.net \
  --video-guid YOUR_GUID --output-dir ./out --progress-file ./progress.json -v

# ZIP only (needs storage key)
./.venv/bin/python3 bunny_stream_hls_merge_to_mp4.py \
  ... --storage-access-key YOUR_STORAGE_KEY --zip-only
```

## Project layout

| File | Purpose |
|------|---------|
| `bunny_stream_hls_merge_to_mp4.py` | Main merge logic: Stream API, optional Storage ZIP download, HLS parsing, ffmpeg remux, optional S3 upload, progress JSON |
| `_s3_upload.py` | Reads S3-related env vars and uploads the finished MP4 with boto3 |
| `_drm_migration_s3.py` | DRM bridge S3: HeadObject idempotency, PutObject + ACL fallback |
| `drm_hls_migration_worker.py` | Edmingle DRM worker: loop `worker/next` → merge → S3 → `worker/report` (failure still reports then continues) |
| `_merge_cli.py` | Legacy: builds HLS argv for `bunny_stream_hls_merge_to_mp4.py` from env (`BUNNY_MERGE_METHOD=zip` exits with a pointer to `run_zip_merge.py`) |
| `run_merge.py` | Same worker; merge mode from `DRM_MIGRATION_MERGE_METHOD` / `BUNNY_MERGE_METHOD` (default `hls`) |
| `run_zip_merge.py` | Same worker; forces **zip** (Storage ZIP; `bunny.drm_bunny_access_key` on the job) |
| `run_hls_merge.py` | Same worker; forces **hls** |
| `_env_util.py` | Loads `.env`, resolves paths, picks Python executable (venv vs current) |
| `.env.example` | Template for `.env` |
| `setup_ec2.sh` | Installs system deps + venv + aiohttp on a fresh Linux host |

Generated / local-only (see `.gitignore`): `.env`, `.venv/`, `output_zip/`, `output_hls/`, progress JSON / MP4s if you write them there.

### Re-running the same video

The merge script records completed GUIDs in the progress file. If you see `Resume: N completed` and it exits immediately, that video was skipped. Remove its GUID from the progress JSON, delete the progress file, or point `BUNNY_OUTPUT_DIR` / `--progress-file` to a fresh path to merge again.

## Troubleshooting

- **Missing aiohttp**: `pip install aiohttp` (or use the venv from setup).
- **ffmpeg not found**: Install ffmpeg and ensure it is on `PATH`.
- **ZIP 401 (Edmingle worker)**: ensure **`bunny.drm_bunny_access_key`** on **`worker/next`** is the Bunny **Storage** password for that zone (not the Stream **`drm_bunny_libraryapikey`**).
- **Video skipped**: Only videos in **Finished** status (Bunny status `4`) are processed.

## License

Add your license here if applicable.
