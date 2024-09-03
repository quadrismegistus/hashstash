import pytest
from hashstash.utils.encodings import encode, decode, encode_hash, RAW_NO_COMPRESS
import json
import base64
import zlib

@pytest.fixture
def default_params():
    return {"b64": True, "compress": RAW_NO_COMPRESS, "as_string": False}

def test_encode_decode(default_params):
    data = json.dumps({"test": "data", "number": 42})
    encoded = encode(data, **default_params)
    decoded = json.loads(decode(encoded, **default_params).decode('utf-8'))
    assert decoded == json.loads(data)

def test_encode_decode_list(default_params):
    data = json.dumps(["list", "of", "items"])
    encoded = encode(data, **default_params)
    decoded = json.loads(decode(encoded, **default_params).decode('utf-8'))
    assert decoded == json.loads(data)

def test_hash():
    data = b"test data"
    hashed = encode_hash(data)
    assert len(hashed) == 32  # MD5 hash is 32 characters long

def test_b64_encoding(default_params):
    data = json.dumps({"test": "data"})
    encoded = encode(data, b64=True, compress=False)
    decoded_json = base64.b64decode(encoded).decode('utf-8')
    assert json.loads(decoded_json) == json.loads(data)

def test_compression(default_params):
    data = json.dumps({"test": "data" * 1000})
    encoded_compressed = encode(data, b64=False, compress='zlib')
    encoded_uncompressed = encode(data, b64=False, compress=RAW_NO_COMPRESS)
    assert len(encoded_compressed) < len(encoded_uncompressed)

def test_as_string(default_params):
    data = json.dumps({"test": "data"})
    encoded = encode(data, as_string=True)
    assert isinstance(encoded, str)
    decoded = decode(encoded, as_string=True)
    assert isinstance(decoded, str)

def test_no_compression(default_params):
    data = json.dumps({"test": "data"})
    encoded = encode(data, b64=True, compress=False)
    decoded = json.loads(decode(encoded, b64=True, compress=False).decode('utf-8'))
    assert decoded == json.loads(data)

def test_different_combinations():
    data = json.dumps({"test": "data", "number": 42})
    combinations = [
        {"b64": True, "compress": 'zlib', "as_string": False},
        {"b64": True, "compress": RAW_NO_COMPRESS, "as_string": False},
        {"b64": False, "compress": 'zlib', "as_string": False},
        {"b64": False, "compress": RAW_NO_COMPRESS, "as_string": False},
        {"b64": True, "compress": 'zlib', "as_string": True},
    ]
    for params in combinations:
        encoded = encode(data, **params)
        decparams = {**params}
        decparams['as_string'] = False
        decoded = json.loads(decode(encoded, **decparams).decode('utf-8'))
        assert decoded == json.loads(data), f"Failed with params: {params}"

# Add more tests as needed