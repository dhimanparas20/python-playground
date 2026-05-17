# =====================================================================
# STEP 1: TERMINAL INSTALLATION COMMAND
# Run this command in your terminal before running this Python script:
#
# pip install bcrypt cryptography
# =====================================================================

import bcrypt
from cryptography.fernet import Fernet

# =====================================================================
# STEP 2: THE 3 SECURITY FUNCTIONS
# =====================================================================

def hash_password(password: str) -> str:
    """
    1. PASSWORD HASHING (One-Way)
    Converts a plain password into an unreadable, salted hash for the DB.
    """
    password_bytes = password.encode('utf-8')
    salt = bcrypt.gensalt()
    hashed_password = bcrypt.hashpw(password_bytes, salt)
    return hashed_password.decode('utf-8')


def validate_password(stored_hash: str, password_attempt: str) -> bool:
    """
    2. PASSWORD VALIDATION (One-Way Check)
    Verifies if a login password attempt matches the stored database hash.
    """
    hash_bytes = stored_hash.encode('utf-8')
    attempt_bytes = password_attempt.encode('utf-8')
    return bcrypt.checkpw(attempt_bytes, hash_bytes)


# Generate a unique secret key for data encryption. 
# WARNING: In production, save this key in a secure .env file, do not hardcode it!
SECRET_KEY = Fernet.generate_key()
cipher_suite = Fernet(SECRET_KEY)

def encrypt_and_decrypt_data(sensitive_data: str, mode: str) -> str:
    """
    3. DATA ENCRYPTION/DECRYPTION (Two-Way)
    Protects sensitive data (like SSNs or API keys) that you need to read later.
    """
    if mode == "encrypt":
        data_bytes = sensitive_data.encode('utf-8')
        encrypted_bytes = cipher_suite.encrypt(data_bytes)
        return encrypted_bytes.decode('utf-8')
        
    elif mode == "decrypt":
        decrypted_bytes = cipher_suite.decrypt(sensitive_data.encode('utf-8'))
        return decrypted_bytes.decode('utf-8')
    
    else:
        raise ValueError("Mode must be either 'encrypt' or 'decrypt'")


# =====================================================================
# STEP 3: TESTING THE CODE
# =====================================================================
if __name__ == "__main__":
    print("--- 🔐 RUNNING SECURITY DEMO 🔐 ---\n")

    # --- Test 1: Password Signup & Hashing ---
    user_password = "MySuperSecretPassword123"
    database_hash = hash_password(user_password)
    print("[SIGNUP] User registered.")
    print(f"-> Stored Hash in DB: {database_hash}\n")

    # --- Test 2: Password Login Validation ---
    print("[LOGIN] Testing password validation...")
    
    # Correct attempt
    is_correct = validate_password(database_hash, "MySuperSecretPassword123")
    print(f"-> Attempt with correct password: {is_correct} (Expected: True)")
    
    # Wrong attempt
    is_wrong = validate_password(database_hash, "WrongPassword123")
    print(f"-> Attempt with wrong password: {is_wrong} (Expected: False)\n")

    # --- Test 3: Two-Way Data Encryption ---
    ssn_data = "123-456-7890"
    print(f"[DATA PROTECTION] Original Sensitive Data: {ssn_data}")
    
    # Encrypt
    encrypted_db_data = encrypt_and_decrypt_data(ssn_data, "encrypt")
    print(f"-> Encrypted string for DB: {encrypted_db_data}")
    
    # Decrypt
    decrypted_admin_view = encrypt_and_decrypt_data(encrypted_db_data, "decrypt")
    print(f"-> Decrypted back for app use: {decrypted_admin_view}\n")
    
    print("--- DEMO COMPLETE ---")
