import boto3
from botocore.exceptions import ClientError
import os


def upload_to_s3(file_input, folder_name, filename=None, bucket_name='mstchatapp', region='ap-south-1'):
    """
    Upload a file to S3 bucket - supports both file paths and file objects (including Flask FileStorage)

    Args:
        file_input: Either file path (str) or file object (including Flask FileStorage)
        folder_name (str): S3 folder/prefix
        filename (str): Optional custom filename (required for file objects)
        bucket_name (str): S3 bucket name
        region (str): AWS region

    Returns:
        dict: Success/error response
    """
    try:
        s3_client = boto3.client(
            's3',
            region_name=region,
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )

        # Handle file path vs file object
        if isinstance(file_input, str):
            # File path
            if not filename:
                filename = os.path.basename(file_input)
            s3_key = f"{folder_name}/{filename}" if folder_name else filename
            s3_client.upload_file(file_input, bucket_name, s3_key)
        else:
            # File object (including Flask FileStorage)
            if not filename:
                # Try to get filename from FileStorage object
                if hasattr(file_input, 'filename') and file_input.filename:
                    filename = file_input.filename
                else:
                    raise ValueError("filename required when uploading file object")

            s3_key = f"{folder_name}/{filename}" if folder_name else filename

            # Determine content type
            content_type = 'application/octet-stream'
            if hasattr(file_input, 'content_type') and file_input.content_type:
                content_type = file_input.content_type

            # Upload file object to S3
            s3_client.upload_fileobj(
                file_input,
                bucket_name,
                s3_key,
                ExtraArgs={'ContentType': content_type}
            )

        s3_url = f"https://{bucket_name}.s3.{region}.amazonaws.com/{s3_key}"

        return {
            'success': True,
            'url': s3_url,
            's3_key': s3_key,
            'filename': filename
        }

    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }


def delete_from_s3(s3_key, bucket_name='mstchatapp', region='ap-south-1'):
    """
    Delete a file from S3 bucket

    Args:
        s3_key (str): S3 object key (path to file in bucket, e.g., 'uploads/file.jpg')
        bucket_name (str): S3 bucket name (default: 'mstchatapp')
        region (str): AWS region (default: 'ap-south-1')

    Returns:
        dict: Success/error response
    """
    try:
        # Initialize S3 client
        s3_client = boto3.client(
            's3',
            region_name=region,
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )

        # Delete the object
        s3_client.delete_object(Bucket=bucket_name, Key=s3_key)

        return {
            'success': True,
            'message': f'File deleted successfully: {s3_key}',
            's3_key': s3_key
        }

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            return {
                'success': False,
                'error': f'File not found: {s3_key}'
            }
        else:
            return {
                'success': False,
                'error': f'S3 delete failed: {str(e)}'
            }
    except Exception as e:
        return {
            'success': False,
            'error': f'Delete error: {str(e)}'
        }


# Alternative method to delete by folder and filename
def delete_file_from_folder(filename, folder_name, bucket_name='mstchatapp', region='ap-south-1'):
    """
    Delete a file from specific folder in S3

    Args:
        filename (str): Name of the file to delete
        folder_name (str): Folder/prefix where file is located
        bucket_name (str): S3 bucket name
        region (str): AWS region

    Returns:
        dict: Success/error response
    """
    # Construct S3 key
    s3_key = f"{folder_name}/{filename}" if folder_name else filename

    # Use the main delete function
    return delete_from_s3(s3_key, bucket_name, region)


# Bulk delete method
def delete_multiple_from_s3(s3_keys, bucket_name='mstchatapp', region='ap-south-1'):
    """
    Delete multiple files from S3 bucket

    Args:
        s3_keys (list): List of S3 object keys to delete
        bucket_name (str): S3 bucket name
        region (str): AWS region

    Returns:
        dict: Success/error response with details
    """
    try:
        s3_client = boto3.client(
            's3',
            region_name=region,
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )

        # Prepare objects for deletion
        objects_to_delete = [{'Key': key} for key in s3_keys]

        # Delete objects
        response = s3_client.delete_objects(
            Bucket=bucket_name,
            Delete={'Objects': objects_to_delete}
        )

        deleted = response.get('Deleted', [])
        errors = response.get('Errors', [])

        return {
            'success': True,
            'message': f'Deleted {len(deleted)} files',
            'deleted': [obj['Key'] for obj in deleted],
            'errors': errors
        }

    except Exception as e:
        return {
            'success': False,
            'error': f'Bulk delete error: {str(e)}'
        }


# Usage examples:
# if __name__ == "__main__":
#     # Upload to uploads folder
#     result = upload_to_s3('/path/to/your/file.jpg', 'uploads')
#     print(result)
#
#     # Upload to images folder
#     result = upload_to_s3('/path/to/image.png', 'images')
#     print(result)
#
#     # Upload to root (no folder)
#     result = upload_to_s3('/path/to/document.pdf', '')
#     print(result)
