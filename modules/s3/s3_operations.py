"""
s3_operations — Backward-compatible S3 helper functions.
All functions delegate to S3Util from s3_util.py for production-ready implementation.

For new code, prefer using S3Util directly.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .s3_util import S3Util

_DEFAULT_UTIL: Optional[S3Util] = None


def _util() -> S3Util:
    global _DEFAULT_UTIL
    if _DEFAULT_UTIL is None:
        _DEFAULT_UTIL = S3Util()
    return _DEFAULT_UTIL


def upload_file_to_s3(local_file_path: str, s3_key: Optional[str] = None) -> bool:
    """
    Upload a file to S3 bucket.

    Args:
        local_file_path: Path to the local file to upload.
        s3_key: S3 object key. If None, uses the filename.

    Returns:
        True if successful, False otherwise.
    """
    result = _util().upload_file(local_file_path, s3_key=s3_key)
    return result["success"]


def download_file_from_s3(s3_key: str, local_file_path: Optional[str] = None) -> bool:
    """
    Download a file from S3 bucket.

    Args:
        s3_key: S3 object key to download.
        local_file_path: Local path to save file. If None, uses s3_key as filename.

    Returns:
        True if successful, False otherwise.
    """
    result = _util().download_file(s3_key, local_path=local_file_path)
    return result["success"]


def delete_file_from_s3(s3_key: str) -> bool:
    """
    Delete a file from S3 bucket.

    Args:
        s3_key: S3 object key to delete.

    Returns:
        True if successful, False otherwise.
    """
    result = _util().delete_file(s3_key)
    return result["success"]


def list_files_in_s3(prefix: str = "") -> List[str]:
    """
    List files in S3 bucket with optional prefix filter.

    Args:
        prefix: Filter files by prefix (folder path).

    Returns:
        List of file keys.
    """
    result = _util().list_files(prefix=prefix)
    return result.get("files", [])


def fetch_file_from_s3(s3_key: str, return_type: str = "content") -> Dict[str, Any]:
    """
    Fetch/read a file from S3 bucket.

    Args:
        s3_key: S3 object key to fetch.
        return_type: 'content' for bytes, 'stream' for file object, 'metadata' for file info.

    Returns:
        Dict with success, data, metadata, error.
    """
    return _util().get_file(s3_key, return_type=return_type)


def fetch_text_file_from_s3(s3_key: str, encoding: str = "utf-8") -> Dict[str, Any]:
    """
    Fetch a text file from S3 and decode it.

    Args:
        s3_key: S3 object key.
        encoding: Text encoding (default: utf-8).

    Returns:
        Dict with success, data (string), metadata, error.
    """
    return _util().get_file_text(s3_key, encoding=encoding)


def fetch_json_file_from_s3(s3_key: str) -> Dict[str, Any]:
    """
    Fetch a JSON file from S3 and parse it.

    Args:
        s3_key: S3 object key.

    Returns:
        Dict with success, data (parsed JSON), metadata, error.
    """
    return _util().get_file_json(s3_key)


def check_file_exists_in_s3(s3_key: str) -> bool:
    """
    Check if a file exists in S3 bucket.

    Args:
        s3_key: S3 object key to check.

    Returns:
        True if file exists, False otherwise.
    """
    return _util().file_exists(s3_key)
