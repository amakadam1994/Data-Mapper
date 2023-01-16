from cryptography.fernet import Fernet


if __name__ == '__main__':

    key = Fernet.generate_key()
    print(key)
    cipher_suite = Fernet(key)
    ciphered_text = cipher_suite.encrypt(b"password")
    print(ciphered_text)
    unciphered_text = (cipher_suite.decrypt(ciphered_text))
    print(unciphered_text)