from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.exceptions import InvalidSignature, InvalidKey
from .pymongo_sync import MongoDB
from .s3_util import upload_to_s3
from typing import Tuple,Optional,Union
import os
import io
from cryptography.fernet import Fernet
import boto3
import base64
from icecream import ic
# os.system("clear")


MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
DATABASE_NAME = os.getenv("DATABASE_NAME", "CHAT")

db: MongoDB  =  MongoDB(DATABASE_NAME, "keys", MONGODB_URI)

# Custom Exceptions
class CryptoError(Exception):
    """Custom exception for cryptographic errors."""
    pass

# Key Generation
def generate_key_pair(initials: str = None, password: bytes = None, save_to_files: bool = False,
                      directory: str = "keys") -> Tuple[bytes, bytes]:
    """
    Generates an RSA public/private key pair.
    In production, encrypts private key with master key before uploading to S3.
    """
    try:
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )
        encryption_algo = (
            serialization.BestAvailableEncryption(password)
            if password else serialization.NoEncryption()
        )
        private_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=encryption_algo
        )
        public_pem = private_key.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )

        if save_to_files:
            # Generate filenames
            if initials:
                priv_filename = f"{initials}_priv.pem"
                pub_filename = f"{initials}_pub.pem"
            else:
                priv_filename = "priv.pem"
                pub_filename = "pub.pem"

            if os.getenv('FLASK_ENV') and os.getenv('FLASK_ENV') == "prod":
                ic("Generating key pair in production")
                # Encrypt private key before uploading to S3
                try:
                    # Get master encryption key from environment
                    master_key = os.getenv('MASTER_ENCRYPTION_KEY')
                    if not master_key:
                        raise CryptoError("MASTER_ENCRYPTION_KEY not found in environment variables")

                    # Encrypt private key
                    fernet = Fernet(master_key.encode())
                    encrypted_private_key = fernet.encrypt(private_pem)

                    # Upload encrypted private key to S3
                    encrypted_priv_file_obj = io.BytesIO(encrypted_private_key)
                    priv_upload_result = upload_to_s3(encrypted_priv_file_obj, directory, priv_filename)

                    # Upload public key to S3 (no encryption needed)
                    pub_file_obj = io.BytesIO(public_pem)
                    pub_upload_result = upload_to_s3(pub_file_obj, directory, pub_filename)

                    if not priv_upload_result['success']:
                        raise CryptoError(
                            f"Failed to upload encrypted private key to S3: {priv_upload_result['error']}")
                    if not pub_upload_result['success']:
                        raise CryptoError(f"Failed to upload public key to S3: {pub_upload_result['error']}")


                except Exception as e:
                    raise CryptoError(f"S3 key upload failed: {e}")
            else:
                # Save locally in development (unencrypted)
                os.makedirs(directory, exist_ok=True)
                priv_path = os.path.join(directory, priv_filename)
                pub_path = os.path.join(directory, pub_filename)

                with open(priv_path, "wb") as priv_file:
                    priv_file.write(private_pem)
                with open(pub_path, "wb") as pub_file:
                    pub_file.write(public_pem)

        return private_pem, public_pem
    except Exception as e:
        raise CryptoError(f"Key generation failed: {e}")

# Message Encryption
def encrypt_message(message: str, public_key: bytes or str, keys_dir: str = "keys") -> bytes:
    """
    Encrypts a message using the provided public key (as bytes or filename).

    Args:
        message (str): The plaintext message to encrypt.
        public_key (bytes or str): The public key in PEM format, or the filename in keys_dir.
        keys_dir (str): Directory to look for key files if public_key is a filename.

    Returns:
        bytes: The encrypted message (ciphertext).

    Raises:
        CryptoError: If encryption fails or input is invalid.
    """
    if not isinstance(message, str) or not message:
        raise CryptoError("Message must be a non-empty string.")

    # Load key from file if a filename is provided
    if isinstance(public_key, str):
        key_path = os.path.join(keys_dir, public_key)
        if not os.path.isfile(key_path):
            raise CryptoError(f"Public key file '{key_path}' does not exist.")
        with open(key_path, "rb") as f:
            public_key_pem = f.read()
    elif isinstance(public_key, bytes):
        public_key_pem = public_key
    else:
        raise CryptoError("Public key must be bytes (PEM format) or a filename (str).")

    try:
        public_key_obj = serialization.load_pem_public_key(public_key_pem)
        ciphertext = public_key_obj.encrypt(
            message.encode('utf-8'),
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        return ciphertext
    except Exception as e:
        raise CryptoError(f"Encryption failed: {e}")

# Message Decryption
def decrypt_message(ciphertext: bytes, private_key: bytes or str, password: bytes = None, keys_dir: str = "keys") -> str:
    """
    Decrypts a ciphertext using the provided private key (as bytes or filename).

    Args:
        ciphertext (bytes): The encrypted message.
        private_key (bytes or str): The private key in PEM format, or the filename in keys_dir.
        password (bytes, optional): Password for the private key, if encrypted.
        keys_dir (str): Directory to look for key files if private_key is a filename.

    Returns:
        str: The decrypted plaintext message.

    Raises:
        CryptoError: If decryption fails or input is invalid.
    """
    if not isinstance(ciphertext, bytes) or not ciphertext:
        raise CryptoError("Ciphertext must be non-empty bytes.")

    # Load key from file if a filename is provided
    if isinstance(private_key, str):
        key_path = os.path.join(keys_dir, private_key)
        if not os.path.isfile(key_path):
            raise CryptoError(f"Private key file '{key_path}' does not exist.")
        with open(key_path, "rb") as f:
            private_key_pem = f.read()
    elif isinstance(private_key, bytes):
        private_key_pem = private_key
    else:
        raise CryptoError("Private key must be bytes (PEM format) or a filename (str).")

    try:
        private_key_obj = serialization.load_pem_private_key(
            private_key_pem,
            password=password
        )
        plaintext = private_key_obj.decrypt(
            ciphertext,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        return plaintext.decode('utf-8')
    except Exception as e:
        raise CryptoError(f"Decryption failed: {e}")

# Save Public Key to MongoDB
def save_public_key(username: str, public_key: Union[bytes, str] = None, db: MongoDB = db):
    """
    Save a user's public key to MongoDB. Downloads from S3 in production.
    """
    if isinstance(public_key, str):
        # public_key is a file path/key
        if os.getenv('FLASK_ENV') == "prod":
            ic("saving public key to S3 in PRODUCTION")
            # Download from S3
            try:
                s3_client = boto3.client(
                    's3',
                    region_name='ap-south-1',
                    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
                )

                # Extract S3 key from file path
                s3_key = public_key.replace('static/', '') if public_key.startswith('static/') else public_key

                # Download public key content from S3
                response = s3_client.get_object(Bucket='mstchatapp', Key=s3_key)
                public_key = response['Body'].read()

            except Exception as e:
                raise CryptoError(f"Failed to download public key from S3: {e}")
        else:
            # Read from local file
            with open(public_key, "rb") as f:
                public_key = f.read()

    # Always use base64 for binary data
    public_key_b64 = base64.b64encode(public_key).decode('utf-8')
    return db.insert_unique({"username": username}, {"username": username, "public_key": public_key_b64})


# Fetch Public Key form a file or MongoDB
def fetch_public_key(username: str, db: MongoDB = db, public_key_file: str = None) -> Optional[bytes]:
    """
    Fetch a user's public key from S3 in production, local file in development, or MongoDB.
    """
    if public_key_file:
        if os.getenv('FLASK_ENV') == "prod":
            ic("fetching public key from S3 in PRODUCTION")
            # Download from S3
            try:
                s3_client = boto3.client(
                    's3',
                    region_name='ap-south-1',
                    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
                )

                s3_key = public_key_file.replace('static/', '') if public_key_file.startswith(
                    'static/') else public_key_file
                response = s3_client.get_object(Bucket='mstchatapp', Key=s3_key)
                return response['Body'].read()

            except Exception as e:
                return None
        else:
            # Read from local file
            try:
                with open(public_key_file, "rb") as f:
                    return f.read()
            except FileNotFoundError:
                return None

    # Fetch from MongoDB as fallback
    doc = db.get({"username": username})
    if not doc or "public_key" not in doc:
        return None
    return base64.b64decode(doc["public_key"])


def fetch_private_key(username: str, private_key_file: str = None) -> Optional[bytes]:
    """
    Fetch and decrypt private key from S3 in production, or from local file in development.
    """
    if os.getenv('FLASK_ENV') == "prod":
        ic("fetching private key from S3 in PRODUCTION")
        try:
            # Get master encryption key
            master_key = os.getenv('MASTER_ENCRYPTION_KEY')
            if not master_key:
                return None

            # Download encrypted private key from S3
            s3_client = boto3.client(
                's3',
                region_name='ap-south-1',
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
            )

            # Construct S3 key for private key
            if private_key_file:
                s3_key = private_key_file.replace('static/', '') if private_key_file.startswith(
                    'static/') else private_key_file
            else:
                s3_key = f"keys/{username}_priv.pem"

            # Download encrypted private key
            response = s3_client.get_object(Bucket='mstchatapp', Key=s3_key)
            encrypted_private_key = response['Body'].read()

            # Decrypt private key
            fernet = Fernet(master_key.encode())
            decrypted_private_key = fernet.decrypt(encrypted_private_key)

            return decrypted_private_key

        except Exception as e:
            return None
    else:
        # Read from local file in development
        if private_key_file:
            file_path = private_key_file
        else:
            file_path = os.path.join("keys", f"{username}_priv.pem")

        try:
            with open(file_path, "rb") as f:
                return f.read()
        except FileNotFoundError:
            return None

def generate_master_key() -> str:
    """
    Generate a new master encryption key for encrypting private keys.
    Run this once and store the result in your environment variables.
    """
    key = Fernet.generate_key()
    return key.decode('utf-8')

# Usage: Run this once to generate your master key
# if __name__ == "__main__":
#     master_key = generate_master_key()
#     print(f"Set this as your MASTER_ENCRYPTION_KEY environment variable:")
#     print(master_key)

# Generate keys with initials
# private_pem, public_pem = generate_key_pair(initials="alice", password=b"mysecret", save_to_files=True)

# Save public key to MongoDB (from variable)
# print("New key inserted: ",save_public_key("alice", public_key_pem=public_pem))

# Save public key to MongoDB (from file)
# save_public_key("bob", public_key_file="keys/bob_pub.pem")

# Fetch public key from MongoDB
# alice_pubkey = fetch_public_key("alice")
# # print(alice_pubkey.decode('utf-8'))
#
# # Using key variables (as before)
# ciphertext = encrypt_message("Hello, secure world!", alice_pubkey)
# print("-------------------------------------")
# plaintext = decrypt_message(ciphertext, "alice_priv.pem", password=b"mysecret")
# print("-------------------------------------")
# print(plaintext)
#
# # Using key files (after saving with generate_key_pair(..., save_to_files=True))
# ciphertext = encrypt_message("Hello, secure world!", "alice_pub.pem")
# plaintext = decrypt_message(ciphertext, "alice_priv.pem", password=b"mysecret")
# print(plaintext)