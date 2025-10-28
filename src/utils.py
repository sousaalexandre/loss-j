import hashlib

def generate_file_hash(file_path: str) -> str:
    """
    Generates a SHA-256 hash for the content of a file.

    Args:
        file_path (str): The path to the file.

    Returns:
        str: The hexadecimal SHA-256 hash of the file's content.
    """
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()
