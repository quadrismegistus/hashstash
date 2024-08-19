import pytest
from hashstash.utils.encodings import encode, decode, encode_hash
import json
import base64
import zlib

@pytest.fixture
def default_params():
    return {"b64": True, "compress": True, "as_string": False}

def test_encode_decode(default_params):
    data = {"test": "data", "number": 42}
    encoded = encode(data, **default_params)
    decoded = decode(encoded, **default_params)
    assert decoded == data

def test_encode_decode_list(default_params):
    data = ["list", "of", "items"]
    encoded = encode(data, **default_params)
    decoded = decode(encoded, **default_params)
    assert decoded == data

def test_hash():
    data = b"test data"
    hashed = encode_hash(data)
    assert len(hashed) == 32  # MD5 hash is 32 characters long

def test_b64_encoding(default_params):
    data = {"test": "data"}
    encoded = encode(data, b64=True, compress=False)
    decoded_json = base64.b64decode(encoded).decode('utf-8')
    assert json.loads(decoded_json) == data

def test_compression(default_params):
    data = {"test": "data" * 1000}  # Create a larger dataset to test compression
    encoded_compressed = encode(data, b64=False, compress=True)
    encoded_uncompressed = encode(data, b64=False, compress=False)
    assert len(encoded_compressed) < len(encoded_uncompressed)

def test_as_string(default_params):
    data = {"test": "data"}
    encoded = encode(data, as_string=True)
    assert isinstance(encoded, str)
    decoded = decode(encoded, as_string=True)
    assert isinstance(decoded, str)

def test_no_compression(default_params):
    data = {"test": "data"}
    encoded = encode(data, b64=True, compress=False)
    decoded = decode(encoded, b64=True, compress=False)
    assert decoded == data

def test_different_combinations():
    data = {"test": "data", "number": 42}
    combinations = [
        {"b64": True, "compress": True, "as_string": False},
        {"b64": True, "compress": False, "as_string": False},
        {"b64": False, "compress": True, "as_string": False},
        {"b64": False, "compress": False, "as_string": False},
        {"b64": True, "compress": True, "as_string": True},
    ]
    for params in combinations:
        encoded = encode(data, **params)
        decparams = {**params}
        decparams['as_string'] = False
        decoded = decode(encoded, **decparams)
        assert decoded == data, f"Failed with params: {params}"

# Add more tests as needed