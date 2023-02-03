import logging

from cryptography.fernet import Fernet


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    key = Fernet.generate_key()
    logging.info(f'{key}')
    cipher_suite = Fernet(key)
    ciphered_text = cipher_suite.encrypt(b"KJPTFj4csGLtImbM")
    logging.info(f'{ciphered_text}')
    unciphered_text = (cipher_suite.decrypt(ciphered_text))
    logging.info(f'{unciphered_text}')