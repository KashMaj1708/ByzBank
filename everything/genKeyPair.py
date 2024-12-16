from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding

def generate_key_pair():
    """
    Generate a public-private key pair for digital signatures
    """
    # Generate private key
    private_key = rsa.generate_private_key(
        public_exponent=65537,  # Common choice for RSA
        key_size=2048,  # Recommended key size
        backend=default_backend()
    )

    # Get public key
    public_key = private_key.public_key()

    # Save the private key to a file
    with open("private_key.pem", "wb") as f:
        f.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ))

    # Save the public key to a file
    with open("public_key.pem", "wb") as f:
        f.write(public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ))

    print("Key pair generated and saved successfully.")

def sign_message(private_key_path, message):
    """
    Sign a message using the private key
    
    :param private_key_path: Path to the private key file
    :param message: Message to sign
    :return: Signature
    """
    # Load the private key
    with open(private_key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    
    # Sign the message
    signature = private_key.sign(
        message.encode(),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )
    
    return signature

def verify_signature(public_key_path, message, signature):
    """
    Verify a signature using the public key
    
    :param public_key_path: Path to the public key file
    :param message: Original message
    :param signature: Signature to verify
    :return: Boolean indicating if signature is valid
    """
    # Load the public key
    with open(public_key_path, "rb") as key_file:
        public_key = serialization.load_pem_public_key(
            key_file.read(),
            backend=default_backend()
        )
    
    # Verify the signature
    try:
        public_key.verify(
            signature,
            message.encode(),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        return True
    except:
        return False

def main():
    # Generate a new key pair
    generate_key_pair()

    # Example of signing and verifying a message
    message = "Test message for signing"
    
    # Sign the message
    signature = sign_message("private_key.pem", message)
    
    # Verify the signature
    is_valid = verify_signature("public_key.pem", message, signature)
    
    print(f"Signature valid: {is_valid}")

if __name__ == "__main__":
    main()