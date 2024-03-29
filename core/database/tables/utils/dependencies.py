import os

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from core.config import api_settings_config

PRIVATE_KEY_FILE = api_settings_config.security["private_key_file"]

password = b'kAT\xc2\x04\x1d\x1b\xe9\x96yQ\x1f<\xda\xdfm\xbeo$\x96\x03\xa1\x1d\xc1\xef\x1e\xe4\xb0\xdc),\x96'

def generate_new_private_key():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
    with open(PRIVATE_KEY_FILE, "wb") as private_key_file:
        private_key_file.write(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.BestAvailableEncryption(password),
            )
        )
    os.chmod(PRIVATE_KEY_FILE, 0o400)
    return private_key

# Generate or load RSA private key
# if os.path.exists(PRIVATE_KEY_FILE):
#     try:
#         with open(PRIVATE_KEY_FILE, "rb") as private_key_file:
#             private_key = serialization.load_pem_private_key(
#                 private_key_file.read(), password=password
#             )
#     except:
#         os.remove(PRIVATE_KEY_FILE)
#         private_key = generate_new_private_key()
# else:
#     private_key = generate_new_private_key()

with open(PRIVATE_KEY_FILE, "rb") as private_key_file:
    private_key = serialization.load_pem_private_key(
        private_key_file.read(), password=password
    )

public_key = private_key.public_key()

# Serialize keys to PEM format
private_pem = private_key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)
public_pem = public_key.public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo,
)