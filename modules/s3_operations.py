# s3_operations.py

import boto3
import os
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

BUCKET_NAME = "chatapp-keys"


def upload_file_to_s3(local_file_path, s3_key=None):
    """
    Upload a file to S3 bucket

    Args:
        local_file_path (str): Path to the local file to upload
        s3_key (str, optional): S3 object key. If None, uses the filename

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # If no S3 key provided, use the filename
        if s3_key is None:
            s3_key = os.path.basename(local_file_path)

        # Upload the file
        s3_client.upload_file(
            local_file_path,
            BUCKET_NAME,
            s3_key,
            ExtraArgs={'ServerSideEncryption': 'AES256'}  # Optional encryption
        )

        print(f"✅ Successfully uploaded {local_file_path} to s3://{BUCKET_NAME}/{s3_key}")
        return True

    except FileNotFoundError:
        print(f"❌ Error: File {local_file_path} not found")
        return False
    except NoCredentialsError:
        print("❌ Error: AWS credentials not found")
        return False
    except ClientError as e:
        print(f"❌ Error uploading file: {e}")
        return False


def download_file_from_s3(s3_key, local_file_path=None):
    """
    Download a file from S3 bucket

    Args:
        s3_key (str): S3 object key to download
        local_file_path (str, optional): Local path to save file. If None, uses s3_key as filename

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # If no local path provided, use the S3 key as filename
        if local_file_path is None:
            local_file_path = os.path.basename(s3_key)

        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(local_file_path) if os.path.dirname(local_file_path) else '.', exist_ok=True)

        # Download the file
        s3_client.download_file(
            BUCKET_NAME,
            s3_key,
            local_file_path
        )

        print(f"✅ Successfully downloaded s3://{BUCKET_NAME}/{s3_key} to {local_file_path}")
        return True

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            print(f"❌ Error: File {s3_key} not found in bucket {BUCKET_NAME}")
        else:
            print(f"❌ Error downloading file: {e}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def delete_file_from_s3(s3_key):
    """
    Delete a file from S3 bucket

    Args:
        s3_key (str): S3 object key to delete

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Delete the file
        s3_client.delete_object(
            Bucket=BUCKET_NAME,
            Key=s3_key
        )

        print(f"✅ Successfully deleted s3://{BUCKET_NAME}/{s3_key}")
        return True

    except ClientError as e:
        print(f"❌ Error deleting file: {e}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


# Optional: Helper function to list files in bucket
def list_files_in_s3(prefix=""):
    """
    List files in S3 bucket

    Args:
        prefix (str): Filter files by prefix

    Returns:
        list: List of file keys
    """
    try:
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=prefix
        )

        if 'Contents' in response:
            files = [obj['Key'] for obj in response['Contents']]
            print(f"📁 Files in bucket {BUCKET_NAME}:")
            for file in files:
                print(f"  - {file}")
            return files
        else:
            print(f"📁 No files found in bucket {BUCKET_NAME}")
            return []

    except ClientError as e:
        print(f"❌ Error listing files: {e}")
        return []


def fetch_file_from_s3(s3_key, return_type='content'):
    """
    Fetch/read a file from S3 bucket by its key

    Args:
        s3_key (str): S3 object key to fetch
        return_type (str): Type of return - 'content' for file content, 'stream' for file object, 'metadata' for file info

    Returns:
        dict: Contains success status, data, and metadata
        {
            'success': bool,
            'data': bytes/dict/object (depending on return_type),
            'metadata': dict,
            'error': str (if any)
        }
    """
    try:
        # Get the object from S3
        response = s3_client.get_object(
            Bucket=BUCKET_NAME,
            Key=s3_key
        )

        # Extract metadata
        metadata = {
            'content_type': response.get('ContentType', 'unknown'),
            'content_length': response.get('ContentLength', 0),
            'last_modified': response.get('LastModified'),
            'etag': response.get('ETag', '').strip('"'),
            'server_side_encryption': response.get('ServerSideEncryption'),
            'metadata': response.get('Metadata', {})
        }

        if return_type == 'content':
            # Return file content as bytes
            file_content = response['Body'].read()
            print(f"✅ Successfully fetched content from s3://{BUCKET_NAME}/{s3_key}")
            return {
                'success': True,
                'data': file_content,
                'metadata': metadata,
                'error': None
            }

        elif return_type == 'stream':
            # Return file stream object
            print(f"✅ Successfully fetched stream from s3://{BUCKET_NAME}/{s3_key}")
            return {
                'success': True,
                'data': response['Body'],
                'metadata': metadata,
                'error': None
            }

        elif return_type == 'metadata':
            # Return only metadata
            print(f"✅ Successfully fetched metadata from s3://{BUCKET_NAME}/{s3_key}")
            return {
                'success': True,
                'data': metadata,
                'metadata': metadata,
                'error': None
            }

        else:
            return {
                'success': False,
                'data': None,
                'metadata': {},
                'error': f"Invalid return_type: {return_type}. Use 'content', 'stream', or 'metadata'"
            }

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            error_msg = f"File {s3_key} not found in bucket {BUCKET_NAME}"
        elif error_code == 'AccessDenied':
            error_msg = f"Access denied to file {s3_key}"
        else:
            error_msg = f"AWS Error: {e}"

        print(f"❌ Error fetching file: {error_msg}")
        return {
            'success': False,
            'data': None,
            'metadata': {},
            'error': error_msg
        }

    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        print(f"❌ Error: {error_msg}")
        return {
            'success': False,
            'data': None,
            'metadata': {},
            'error': error_msg
        }


# Helper methods for specific file types
def fetch_text_file_from_s3(s3_key, encoding='utf-8'):
    """
    Fetch a text file and return as string

    Args:
        s3_key (str): S3 object key
        encoding (str): Text encoding (default: utf-8)

    Returns:
        dict: Contains success status and text content
    """
    result = fetch_file_from_s3(s3_key, 'content')

    if result['success']:
        try:
            text_content = result['data'].decode(encoding)
            result['data'] = text_content
            print(f"✅ Successfully decoded text file from s3://{BUCKET_NAME}/{s3_key}")
        except UnicodeDecodeError as e:
            result['success'] = False
            result['error'] = f"Failed to decode text file: {str(e)}"
            result['data'] = None

    return result


def fetch_json_file_from_s3(s3_key):
    """
    Fetch a JSON file and return as Python dict

    Args:
        s3_key (str): S3 object key

    Returns:
        dict: Contains success status and JSON data
    """
    import json

    result = fetch_text_file_from_s3(s3_key)

    if result['success']:
        try:
            json_data = json.loads(result['data'])
            result['data'] = json_data
            print(f"✅ Successfully parsed JSON file from s3://{BUCKET_NAME}/{s3_key}")
        except json.JSONDecodeError as e:
            result['success'] = False
            result['error'] = f"Failed to parse JSON file: {str(e)}"
            result['data'] = None

    return result


def check_file_exists_in_s3(s3_key):
    """
    Check if a file exists in S3 bucket

    Args:
        s3_key (str): S3 object key to check

    Returns:
        bool: True if file exists, False otherwise
    """
    try:
        s3_client.head_object(Bucket=BUCKET_NAME, Key=s3_key)
        print(f"✅ File exists: s3://{BUCKET_NAME}/{s3_key}")
        return True
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"❌ File not found: s3://{BUCKET_NAME}/{s3_key}")
        else:
            print(f"❌ Error checking file: {e}")
        return False

# Example usage (you can remove this part)
if __name__ == "__main__":
    # Example usage
    print("🚀 S3 Operations Example")

    # Upload a file
    # upload_file_to_s3("/home/paras/Desktop/Projects/play/keys/gendu_priv.pem", "keys/gendu_priv.pem")

    # Download a file
    # download_file_from_s3("keys/gendu_priv.pem", "downloads/gendu_priv.pem")

    # Delete a file
    # delete_file_from_s3("keys/gendu_priv.pem")

    # List files
    # list_files_in_s3()

    # if check_file_exists_in_s3("gendu_priv.pem"):
    #     print("File exists!")
    # else:
    #     print("File not found!")

    # result = fetch_file_from_s3("gendu_priv.pem", "metadata")
    # if result['success']:
    #     print(result['data'])
    #     metadata = result['data']
    #     print(f"File size: {metadata['content_length']}")
    #     print(f"Last modified: {metadata['last_modified']}")

    # result = fetch_file_from_s3("gendu_priv.pem", "stream")
    # if result['success']:
    #     stream = result['data']
    #     # Process stream in chunks
    #     for chunk in iter(lambda: stream.read(1024), b''):
    #         # Process chunk
    #         print(chunk)
    #         pass

    # result = fetch_file_from_s3("gendu_priv.pem","content")
    # if result['success']:
    #     file_content = result['data']  # bytes
    #     metadata = result['metadata']
    #     print(f"File size: {metadata['content_length']} bytes")
    #     print(f"Content type: {metadata['content_type']}")
    # else:
    #     print(f"Error: {result['error']}")