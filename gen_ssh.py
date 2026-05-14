import subprocess
import os
from pathlib import Path

def generate_ssh_access(key_name: str, comment: str = "generated_key"):
    """
    Generates an Ed25519 key pair, authorizes the public key, 
    and sets correct server permissions.
    """
    home = Path.home()
    ssh_dir = home / ".ssh"
    priv_key_path = home / key_name
    pub_key_path = home / f"{key_name}.pub"
    auth_keys_path = ssh_dir / "authorized_keys"

    # 1. Ensure .ssh directory exists with correct permissions
    ssh_dir.mkdir(mode=0o700, parents=True, exist_ok=True)

    # 2. Generate the Ed25519 key pair
    print(f"--- Generating key pair: {key_name} ---")
    try:
        subprocess.run([
            "ssh-keygen", "-t", "ed25519", 
            "-f", str(priv_key_path), 
            "-C", comment, 
            "-N", ""  # Empty passphrase for automation
        ], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error generating keys: {e}")
        return

    # 3. Append Public Key to authorized_keys
    print("--- Authorizing public key ---")
    with open(pub_key_path, "r") as pub_file:
        pub_key_content = pub_file.read()
    
    with open(auth_keys_path, "a") as auth_file:
        auth_file.write(f"\n{pub_key_content}")

    # 4. Set strict permissions on authorized_keys
    auth_keys_path.chmod(0o600)

    # 5. Read Private Key to return to user
    with open(priv_key_path, "r") as priv_file:
        private_key_data = priv_file.read()

    # 6. Cleanup: Remove keys from server for security
    priv_key_path.unlink()
    pub_key_path.unlink()
    
    print("--- Setup Complete. Keys removed from server disk. ---")
    return private_key_data

# Example Usage:
if __name__ == "__main__":
    key_label = input("Enter a name for this key (e.g., kaneki_key): ")
    private_key = generate_ssh_access(key_label, comment=f"access_for_{key_label}")
    
    if private_key:
        print("\n" + "="*30)
        print("COPY THE PRIVATE KEY BELOW:")
        print("="*30 + "\n")
        print(private_key)
        print("="*30)
