"""
S3Util — Production-ready S3 utility class and helper functions.
Unifies upload, download, delete, list, fetch, existence check, and metadata operations.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
except ImportError:
    raise ImportError("Install boto3: pip install boto3")


class S3Util:
    """
    Production-ready S3 utility class providing upload, download, delete, list,
    fetch, existence check, and metadata operations.

    Attributes:
        bucket_name (str): S3 bucket name.
        region (str): AWS region name.
        endpoint_url (Optional[str]): Custom S3 endpoint (for MinIO, DigitalOcean Spaces, etc.).
    """

    def __init__(
        self,
        bucket_name: Optional[str] = None,
        region: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        endpoint_url: Optional[str] = None,
    ) -> None:
        """
        Initialize S3Util instance.

        Args:
            bucket_name: S3 bucket name. Falls back to env var S3_BUCKET_NAME.
            region: AWS region. Falls back to env var AWS_REGION.
            aws_access_key_id: AWS access key. Falls back to env var AWS_ACCESS_KEY_ID.
            aws_secret_access_key: AWS secret key. Falls back to env var AWS_SECRET_ACCESS_KEY.
            endpoint_url: Custom S3 endpoint URL (e.g., for MinIO).
        """
        self.bucket_name: str = bucket_name or os.getenv("S3_BUCKET_NAME") or os.getenv("AWS_BUCKET_NAME", "")
        self.region: str = region or os.getenv("AWS_REGION", "ap-south-1")
        self.endpoint_url: Optional[str] = endpoint_url

        self._client = boto3.client(
            "s3",
            region_name=self.region,
            aws_access_key_id=aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY"),
            endpoint_url=self.endpoint_url,
        )

    # ──────────────────────────────────────────────
    # UPLOAD
    # ──────────────────────────────────────────────

    def upload_file(
        self,
        file_input: Union[str, Any],
        s3_key: Optional[str] = None,
        folder: Optional[str] = None,
        content_type: Optional[str] = None,
        extra_args: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Upload a file to S3. Supports file paths, file objects, and Flask FileStorage.

        Args:
            file_input: File path (str) or file object (with .read() method).
            s3_key: S3 object key. If None, derived from filename.
            folder: Optional folder/prefix to prepend to s3_key.
            content_type: MIME type. Auto-detected for file objects if available.
            extra_args: Additional ExtraArgs passed to upload_file/upload_fileobj.

        Returns:
            Dict with success, url, s3_key, filename on success,
            or success, error on failure.
        """
        try:
            client = self._client
            bucket = self.bucket_name

            if not bucket:
                return {"success": False, "error": "No bucket name configured. Set S3_BUCKET_NAME env var or pass bucket_name."}

            filename = ""
            is_path = isinstance(file_input, str)

            if is_path:
                path = Path(file_input)
                if not path.exists():
                    return {"success": False, "error": f"File not found: {file_input}"}
                filename = path.name
                key = s3_key or filename
                if folder:
                    key = f"{folder}/{key}"
                client.upload_file(str(path), bucket, key, ExtraArgs=extra_args or {})
            else:
                if hasattr(file_input, "filename") and file_input.filename:
                    filename = file_input.filename
                elif s3_key:
                    filename = s3_key.split("/")[-1]
                else:
                    filename = "upload"
                key = s3_key or filename
                if folder:
                    key = f"{folder}/{key}"
                args = dict(extra_args or {})
                if content_type:
                    args["ContentType"] = content_type
                elif hasattr(file_input, "content_type") and file_input.content_type:
                    args["ContentType"] = file_input.content_type
                if "ContentType" not in args:
                    args["ContentType"] = "application/octet-stream"
                client.upload_fileobj(file_input, bucket, key, ExtraArgs=args)

            url = self.get_url(key)
            return {"success": True, "url": url, "s3_key": key, "filename": filename}
        except (ClientError, NoCredentialsError) as e:
            return {"success": False, "error": str(e)}

    # ──────────────────────────────────────────────
    # DOWNLOAD
    # ──────────────────────────────────────────────

    def download_file(
        self,
        s3_key: str,
        local_path: Optional[Union[str, Path]] = None,
    ) -> Dict[str, Any]:
        """
        Download a file from S3 to a local path.

        Args:
            s3_key: S3 object key.
            local_path: Local destination path. If None, uses s3_key basename in current dir.

        Returns:
            Dict with success, local_path, s3_key on success, or success, error on failure.
        """
        try:
            dest = Path(local_path or Path(s3_key).name)
            dest.parent.mkdir(parents=True, exist_ok=True)
            self._client.download_file(self.bucket_name, s3_key, str(dest))
            return {"success": True, "local_path": str(dest), "s3_key": s3_key}
        except ClientError as e:
            code = e.response["Error"]["Code"]
            msg = f"File not found: {s3_key}" if code == "NoSuchKey" else str(e)
            return {"success": False, "error": msg}

    # ──────────────────────────────────────────────
    # DELETE
    # ──────────────────────────────────────────────

    def delete_file(self, s3_key: str) -> Dict[str, Any]:
        """
        Delete a single file from S3.

        Args:
            s3_key: S3 object key.

        Returns:
            Dict with success, s3_key on success, or success, error on failure.
        """
        try:
            self._client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            return {"success": True, "s3_key": s3_key}
        except ClientError as e:
            return {"success": False, "error": str(e)}

    def delete_files(self, s3_keys: List[str]) -> Dict[str, Any]:
        """
        Delete multiple files from S3 in a single request (max 1000 keys).

        Args:
            s3_keys: List of S3 object keys.

        Returns:
            Dict with success, message, deleted, errors.
        """
        if not s3_keys:
            return {"success": True, "message": "No keys provided", "deleted": [], "errors": []}
        try:
            objects = [{"Key": k} for k in s3_keys]
            response = self._client.delete_objects(
                Bucket=self.bucket_name,
                Delete={"Objects": objects},
            )
            deleted = [o["Key"] for o in response.get("Deleted", [])]
            errors = response.get("Errors", [])
            return {
                "success": True,
                "message": f"Deleted {len(deleted)} files",
                "deleted": deleted,
                "errors": errors,
            }
        except Exception as e:
            return {"success": False, "error": f"Bulk delete error: {str(e)}"}

    # ──────────────────────────────────────────────
    # LIST
    # ──────────────────────────────────────────────

    def list_files(self, prefix: str = "") -> Dict[str, Any]:
        """
        List files in the bucket with an optional prefix filter.

        Args:
            prefix: Filter by prefix (folder path).

        Returns:
            Dict with success, files (list of keys), and count.
        """
        try:
            response = self._client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            files = [obj["Key"] for obj in response.get("Contents", [])]
            return {"success": True, "files": files, "count": len(files)}
        except ClientError as e:
            return {"success": False, "error": str(e), "files": [], "count": 0}

    # ──────────────────────────────────────────────
    # FILE INFO
    # ──────────────────────────────────────────────

    def file_exists(self, s3_key: str) -> bool:
        """
        Check if a file exists in S3.

        Args:
            s3_key: S3 object key.

        Returns:
            True if file exists, False otherwise.
        """
        try:
            self._client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError:
            return False

    def get_file_metadata(self, s3_key: str) -> Dict[str, Any]:
        """
        Get metadata for an S3 object without downloading it.

        Args:
            s3_key: S3 object key.

        Returns:
            Dict with success and either data (metadata dict) or error.
        """
        try:
            response = self._client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return {
                "success": True,
                "data": {
                    "content_type": response.get("ContentType", "unknown"),
                    "content_length": response.get("ContentLength", 0),
                    "last_modified": response.get("LastModified"),
                    "etag": response.get("ETag", "").strip('"'),
                    "server_side_encryption": response.get("ServerSideEncryption"),
                    "metadata": response.get("Metadata", {}),
                },
            }
        except ClientError as e:
            code = e.response["Error"]["Code"]
            msg = f"File not found: {s3_key}" if code == "NoSuchKey" else str(e)
            return {"success": False, "error": msg}

    # ──────────────────────────────────────────────
    # FETCH / READ
    # ──────────────────────────────────────────────

    def get_file(
        self,
        s3_key: str,
        return_type: str = "content",
    ) -> Dict[str, Any]:
        """
        Fetch/read a file from S3.

        Args:
            s3_key: S3 object key.
            return_type: One of "content" (bytes), "stream" (file-like object),
                        or "metadata" (response metadata dict).

        Returns:
            Dict with success, data, metadata, error.
        """
        valid = {"content", "stream", "metadata"}
        if return_type not in valid:
            return {"success": False, "data": None, "metadata": {}, "error": f"Invalid return_type: {return_type}. Use one of {valid}"}
        try:
            response = self._client.get_object(Bucket=self.bucket_name, Key=s3_key)
            metadata = {
                "content_type": response.get("ContentType", "unknown"),
                "content_length": response.get("ContentLength", 0),
                "last_modified": response.get("LastModified"),
                "etag": response.get("ETag", "").strip('"'),
                "server_side_encryption": response.get("ServerSideEncryption"),
                "metadata": response.get("Metadata", {}),
            }
            if return_type == "content":
                return {"success": True, "data": response["Body"].read(), "metadata": metadata, "error": None}
            elif return_type == "stream":
                return {"success": True, "data": response["Body"], "metadata": metadata, "error": None}
            else:
                return {"success": True, "data": metadata, "metadata": metadata, "error": None}
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "NoSuchKey":
                msg = f"File not found: {s3_key}"
            elif code == "AccessDenied":
                msg = f"Access denied to file: {s3_key}"
            else:
                msg = str(e)
            return {"success": False, "data": None, "metadata": {}, "error": msg}

    def get_file_text(self, s3_key: str, encoding: str = "utf-8") -> Dict[str, Any]:
        """
        Fetch a file from S3 and decode it as text.

        Args:
            s3_key: S3 object key.
            encoding: Text encoding (default: utf-8).

        Returns:
            Dict with success, data (string), metadata, error.
        """
        result = self.get_file(s3_key, "content")
        if result["success"]:
            try:
                result["data"] = result["data"].decode(encoding)
            except UnicodeDecodeError as e:
                return {"success": False, "data": None, "metadata": {}, "error": f"Failed to decode text file: {str(e)}"}
        return result

    def get_file_json(self, s3_key: str) -> Dict[str, Any]:
        """
        Fetch a JSON file from S3 and parse it.

        Args:
            s3_key: S3 object key.

        Returns:
            Dict with success, data (parsed JSON), metadata, error.
        """
        result = self.get_file_text(s3_key)
        if result["success"]:
            try:
                result["data"] = json.loads(result["data"])
            except json.JSONDecodeError as e:
                return {"success": False, "data": None, "metadata": {}, "error": f"Failed to parse JSON file: {str(e)}"}
        return result

    def get_url(self, s3_key: str) -> str:
        """
        Get the public HTTPS URL for an S3 object.

        Args:
            s3_key: S3 object key.

        Returns:
            Public URL string.
        """
        return f"https://{self.bucket_name}.s3.{self.region}.amazonaws.com/{s3_key}"

    # ──────────────────────────────────────────────
    # LIFECYCLE
    # ──────────────────────────────────────────────

    def close(self) -> None:
        """Close the underlying S3 client session."""
        self._client.close()

    def __repr__(self) -> str:
        return f"S3Util(bucket='{self.bucket_name}', region='{self.region}')"

    def __enter__(self) -> "S3Util":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


# ──────────────────────────────────────────────
# BACKWARD-COMPATIBLE MODULE-LEVEL WRAPPERS
# ──────────────────────────────────────────────
# These functions preserve the original signatures and return formats
# from both s3_util.py and s3_operations.py for existing callers.

_DEFAULT_UTIL: Optional[S3Util] = None


def _get_default_util(
    bucket_name: Optional[str] = None,
    region: Optional[str] = None,
) -> S3Util:
    global _DEFAULT_UTIL
    if _DEFAULT_UTIL is None or bucket_name or region:
        return S3Util(bucket_name=bucket_name, region=region)
    return _DEFAULT_UTIL


def upload_to_s3(
    file_input: Union[str, Any],
    folder_name: str = "",
    filename: Optional[str] = None,
    bucket_name: Optional[str] = None,
    region: Optional[str] = None,
) -> Dict[str, Any]:
    """Backward-compatible upload — delegates to S3Util.upload_file."""
    util = _get_default_util(bucket_name, region)
    s3_key = filename if filename else None
    result = util.upload_file(file_input, s3_key=s3_key, folder=folder_name or None)
    if result["success"]:
        return {
            "success": True,
            "url": result["url"],
            "s3_key": result["s3_key"],
            "filename": result["filename"],
        }
    return result


def delete_from_s3(
    s3_key: str,
    bucket_name: Optional[str] = None,
    region: Optional[str] = None,
) -> Dict[str, Any]:
    """Backward-compatible single delete — delegates to S3Util.delete_file."""
    util = _get_default_util(bucket_name, region)
    return util.delete_file(s3_key)


def delete_file_from_folder(
    filename: str,
    folder_name: str,
    bucket_name: Optional[str] = None,
    region: Optional[str] = None,
) -> Dict[str, Any]:
    """Backward-compatible folder delete — constructs key and delegates."""
    s3_key = f"{folder_name}/{filename}" if folder_name else filename
    return delete_from_s3(s3_key, bucket_name, region)


def delete_multiple_from_s3(
    s3_keys: List[str],
    bucket_name: Optional[str] = None,
    region: Optional[str] = None,
) -> Dict[str, Any]:
    """Backward-compatible bulk delete — delegates to S3Util.delete_files."""
    util = _get_default_util(bucket_name, region)
    return util.delete_files(s3_keys)


# ──────────────────────────────────────────────
# USAGE EXAMPLES
# ──────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("S3Util - Usage Examples")
    print("=" * 60)

    util = S3Util()

    print(f"\nBucket: {util.bucket_name}, Region: {util.region}")

    # List files
    print("\n--- List Files ---")
    result = util.list_files()
    print(f"Files: {result.get('files', [])}")

    # Upload
    print("\n--- Upload ---")
    result = util.upload_file("test.txt", folder="uploads")
    print(f"Upload result: {result}")

    # File exists
    print("\n--- File Exists ---")
    if result.get("s3_key"):
        print(f"File exists: {util.file_exists(result['s3_key'])}")

    # Download
    print("\n--- Download ---")
    if result.get("s3_key"):
        result = util.download_file(result["s3_key"], "/tmp/downloaded_test.txt")
        print(f"Download result: {result}")

    # Get metadata
    print("\n--- Metadata ---")
    if result.get("s3_key"):
        meta = util.get_file_metadata(result["s3_key"])
        print(f"Metadata: {meta}")

    # Delete
    print("\n--- Delete ---")
    if result.get("s3_key"):
        result = util.delete_file(result["s3_key"])
        print(f"Delete result: {result}")

    util.close()
    print("\nDone!")
