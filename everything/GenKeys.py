from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
import os

class KeyGenerator:
    def __init__(self, base_dir='keys'):
        self.base_dir = base_dir
        self._ensure_base_dir()

    def _ensure_base_dir(self):
        os.makedirs(self.base_dir, exist_ok=True)

    def generate_server_keys(self, start_port=500051, end_port=500057):
        server_keys_dir = os.path.join(self.base_dir, 'server_keys')
        os.makedirs(server_keys_dir, exist_ok=True)

        for port in range(start_port, end_port + 1):
            private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048,
                backend=default_backend()
            )
            public_key = private_key.public_key()

            private_key_filename = os.path.join(server_keys_dir, f"private_key_{port}.pem")
            public_key_filename = os.path.join(server_keys_dir, f"public_key_{port}.pem")

            with open(private_key_filename, "wb") as f:
                f.write(private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                ))

            with open(public_key_filename, "wb") as f:
                f.write(public_key.public_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PublicFormat.SubjectPublicKeyInfo
                ))

            print(f"Key pair generated for server port {port}")

    def generate_client_keys(self, num_clients=10):
        client_keys_dir = os.path.join(self.base_dir, 'client_keys')
        os.makedirs(client_keys_dir, exist_ok=True)

        for i in range(num_clients):
            client_id = str(i)

            private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048,
                backend=default_backend()
            )
            public_key = private_key.public_key()

            private_key_filename = os.path.join(client_keys_dir, f"client_{client_id}_private_key.pem")
            public_key_filename = os.path.join(client_keys_dir, f"client_{client_id}_public_key.pem")

            with open(private_key_filename, "wb") as f:
                f.write(private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                ))

            with open(public_key_filename, "wb") as f:
                f.write(public_key.public_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PublicFormat.SubjectPublicKeyInfo
                ))

            print(f"Key pair generated for Client {client_id}")

def main():
    key_generator = KeyGenerator()
    key_generator.generate_server_keys(start_port=500051, end_port=500062)
    key_generator.generate_client_keys(num_clients=3000)

if __name__ == "__main__":
    main()