# S3Util — Production-Ready S3 Utility

A comprehensive, all-in-one Python utility class for AWS S3 operations. Provides a clean interface for upload, download, delete, list, fetch, metadata, and existence checks. Unifies the capabilities from `s3_util.py` and `s3_operations.py` into a single production-ready class with full backward compatibility.

## Why Use This?

Every project using S3 ends up writing the same patterns: initialize a client, upload files, download files, list objects, delete objects, check existence, fetch content. This class wraps all of that into a clean, production-ready API so you focus on business logic, not S3 plumbing.

**Before:**
```python
import boto3
client = boto3.client("s3", region_name="ap-south-1",
    aws_access_key_id=...,
    aws_secret_access_key=...)
client.upload_file("/tmp/file.jpg", "my-bucket", "uploads/file.jpg")
client.download_file("my-bucket", "uploads/file.jpg", "/tmp/file.jpg")
client.delete_object(Bucket="my-bucket", Key="uploads/file.jpg")
# ... repeat client setup for every file, every project
```

**After:**
```python
from modules.s3 import S3Util
s3 = S3Util(bucket_name="my-bucket")
s3.upload_file("/tmp/file.jpg", folder="uploads")
s3.download_file("uploads/file.jpg", "/tmp/file.jpg")
s3.delete_file("uploads/file.jpg")
```

## Installation

```bash
pip install boto3
```

## Quick Start

```python
from modules.s3 import S3Util

# Uses env vars: S3_BUCKET_NAME (or AWS_BUCKET_NAME), AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
s3 = S3Util()

# Or specify everything explicitly
s3 = S3Util(
    bucket_name="my-app-bucket",
    region="ap-south-1",
    aws_access_key_id="AKIA...",
    aws_secret_access_key="...",
)
```

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `AWS_ACCESS_KEY_ID` | AWS access key | Yes |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | Yes |
| `AWS_REGION` | AWS region (default: `ap-south-1`) | No |
| `S3_BUCKET_NAME` | S3 bucket name | Yes (or pass to constructor) |

## Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `bucket_name` | `Optional[str]` | Env var `S3_BUCKET_NAME` | S3 bucket name |
| `region` | `Optional[str]` | Env var `AWS_REGION` or `ap-south-1` | AWS region |
| `aws_access_key_id` | `Optional[str]` | Env var `AWS_ACCESS_KEY_ID` | AWS access key |
| `aws_secret_access_key` | `Optional[str]` | Env var `AWS_SECRET_ACCESS_KEY` | AWS secret key |
| `endpoint_url` | `Optional[str]` | `None` | Custom endpoint (MinIO, DigitalOcean Spaces) |

## Features

### Upload

```python
# Upload by file path
result = s3.upload_file("/tmp/photo.jpg")
# {'success': True, 'url': 'https://...', 's3_key': 'photo.jpg', 'filename': 'photo.jpg'}

# Upload with custom S3 key
s3.upload_file("/tmp/photo.jpg", s3_key="profile_pics/user_001.jpg")

# Upload into a folder
s3.upload_file("/tmp/photo.jpg", folder="uploads")
# s3_key becomes: uploads/photo.jpg

# Upload with folder + custom key
s3.upload_file("/tmp/photo.jpg", s3_key="user_001.jpg", folder="profile_pics")
# s3_key becomes: profile_pics/user_001.jpg

# Upload a file object (BytesIO, Flask FileStorage, etc.)
from io import BytesIO
s3.upload_file(BytesIO(b"hello world"), s3_key="hello.txt", content_type="text/plain")

# Upload with extra S3 args (encryption, ACL, etc.)
s3.upload_file("/tmp/secret.pdf", s3_key="docs/secret.pdf",
    extra_args={"ServerSideEncryption": "AES256"})
```

### Download

```python
# Download to current directory (uses s3_key basename)
result = s3.download_file("uploads/photo.jpg")
# {'success': True, 'local_path': 'photo.jpg', 's3_key': 'uploads/photo.jpg'}

# Download to a specific path
s3.download_file("uploads/photo.jpg", "/tmp/downloaded_photo.jpg")

# Download to a directory (path created automatically)
s3.download_file("uploads/photo.jpg", "downloads/photos/photo.jpg")
```

### Delete

```python
# Delete a single file
result = s3.delete_file("uploads/photo.jpg")
# {'success': True, 's3_key': 'uploads/photo.jpg'}

# Bulk delete (up to 1000 keys per request)
result = s3.delete_files(["file1.jpg", "file2.jpg", "file3.jpg"])
# {'success': True, 'message': 'Deleted 3 files', 'deleted': [...], 'errors': []}
```

### List Files

```python
# List all files
result = s3.list_files()
# {'success': True, 'files': ['uploads/photo.jpg', 'docs/report.pdf', ...], 'count': 12}

# List files in a folder
result = s3.list_files(prefix="uploads/")
# {'success': True, 'files': ['uploads/photo.jpg', 'uploads/doc.pdf'], 'count': 2}
```

### File Existence & Metadata

```python
# Check if file exists
exists = s3.file_exists("uploads/photo.jpg")
# True or False

# Get file metadata (no download)
meta = s3.get_file_metadata("uploads/photo.jpg")
# {
#   'success': True,
#   'data': {
#     'content_type': 'image/jpeg',
#     'content_length': 102400,
#     'last_modified': datetime(...),
#     'etag': 'abc123...',
#     'server_side_encryption': None,
#     'metadata': {}
#   }
# }
```

### Fetch / Read File Content

```python
# Fetch as raw bytes
result = s3.get_file("data.bin", return_type="content")
content = result["data"]  # bytes
metadata = result["metadata"]

# Fetch as stream (for chunked processing)
result = s3.get_file("large_file.bin", return_type="stream")
stream = result["data"]
for chunk in iter(lambda: stream.read(8192), b''):
    process(chunk)

# Fetch metadata only
result = s3.get_file("data.bin", return_type="metadata")
print(result["data"]["content_type"])

# Fetch as text
result = s3.get_file_text("readme.txt")
text = result["data"]  # string

# Fetch and parse JSON
result = s3.get_file_json("config.json")
config = result["data"]  # dict
```

### Get Public URL

```python
url = s3.get_url("uploads/photo.jpg")
# 'https://my-bucket.s3.ap-south-1.amazonaws.com/uploads/photo.jpg'
```

### Context Manager

```python
with S3Util(bucket_name="my-bucket") as s3:
    s3.upload_file("/tmp/data.json", s3_key="data.json")
    content = s3.get_file_text("data.json")["data"]
    # client auto-closed on exit
```

## Backward-Compatible Module Functions

Both original module-level APIs are preserved for existing callers:

### From `s3_util.py` (dict-returning)

```python
from modules.s3 import upload_to_s3, delete_from_s3, delete_file_from_folder, delete_multiple_from_s3

# Upload
result = upload_to_s3("/tmp/file.jpg", folder_name="uploads", filename="photo.jpg")
# {'success': True, 'url': '...', 's3_key': 'uploads/photo.jpg', 'filename': 'photo.jpg'}

# Single delete
result = delete_from_s3("uploads/photo.jpg")

# Delete by folder + filename
result = delete_file_from_folder("photo.jpg", "uploads")

# Bulk delete
result = delete_multiple_from_s3(["file1.jpg", "file2.jpg"])
```

### From `s3_operations.py` (bool-returning)

```python
from modules.s3 import (
    upload_file_to_s3, download_file_from_s3, delete_file_from_s3,
    list_files_in_s3, fetch_file_from_s3, fetch_text_file_from_s3,
    fetch_json_file_from_s3, check_file_exists_in_s3
)

# Upload (returns bool)
success = upload_file_to_s3("/tmp/file.jpg", "uploads/file.jpg")
if success:
    print("Upload successful")

# Download (returns bool)
success = download_file_from_s3("uploads/file.jpg", "/tmp/file.jpg")

# Delete (returns bool)
success = delete_file_from_s3("uploads/file.jpg")

# List (returns list)
files = list_files_in_s3(prefix="uploads/")
for f in files:
    print(f)

# Fetch (returns dict)
result = fetch_file_from_s3("config.json", return_type="content")
if result["success"]:
    data = result["data"]

# Fetch text
result = fetch_text_file_from_s3("readme.txt")
text = result["data"]

# Fetch JSON
result = fetch_json_file_from_s3("config.json")
config = result["data"]

# Check existence (returns bool)
if check_file_exists_in_s3("uploads/file.jpg"):
    print("File exists!")
```

## Use Cases

| Use Case | Config |
|----------|--------|
| User profile photos | `bucket_name="user-uploads", folder="profiles"` |
| Document storage | `bucket_name="docs-bucket", s3_key="documents/{id}.pdf"` |
| Backup files | `bucket_name="backups", extra_args={"ServerSideEncryption": "AES256"}` |
| Static assets | `bucket_name="assets", folder="static/"` |
| Config/JSON files | `bucket_name="config-bucket"` + `get_file_json()` |
| Large file streaming | `bucket_name="data-lake"` + `get_file(s3_key, "stream")` |

## Production Notes

- `upload_file()` supports **file paths** (via `upload_file`) and **file objects** (via `upload_fileobj`) — auto-detects the type
- `delete_files()` uses a single `delete_objects` API call — **no N+1 requests**
- `list_files()` uses `list_objects_v2` with prefix filtering — **server-side**
- `get_file()` supports three return modes: `content` (bytes), `stream` (file-like), `metadata` (headers only)
- File existence uses `head_object` — **no download, header-only check**
- Client is **reused** across all operations (created once in constructor)
- All methods have full **type hints** and **docstrings**
- Context manager ensures client session is closed properly
- Backward-compatible wrappers preserve original `s3_util.py` and `s3_operations.py` APIs
- Supports custom `endpoint_url` for S3-compatible storage (MinIO, DigitalOcean Spaces, etc.)
