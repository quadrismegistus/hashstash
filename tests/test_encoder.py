import pytest
from hashstash.utils.encodings import Encoder, Decoder
import json
import base64
import zlib

@pytest.fixture
def encoder():
    return Encoder(b64=True, compress=True, as_string=False)

@pytest.fixture
def decoder():
    return Decoder(b64=True, compress=True, as_string=False)

def test_encoder_init():
    encoder = Encoder(b64=True, compress=False, as_string=True)
    assert encoder.b64 == True
    assert encoder.compress == False
    assert encoder.as_string == True

def test_encoder_encode_decode(encoder, decoder):
    data = {"test": "data", "number": 42}
    encoded = encoder.encode(data)
    decoded = decoder.decode(encoded)
    assert decoded == data

def test_encoder_getitem(encoder, decoder):
    data = ["list", "of", "items"]
    encoded = encoder[data]
    decoded = decoder[encoded]
    assert decoded == data

def test_encoder_hash():
    encoder = Encoder()
    data = b"test data"
    hashed = encoder.hash(data)
    assert len(hashed) == 32  # MD5 hash is 32 characters long

def test_encoder_b64(encoder):
    data = b"test data"
    encoded = encoder._encode_b64(data)
    assert base64.b64decode(encoded) == data

def test_encoder_zlib(encoder):
    data = b"test data"
    compressed = encoder._encode_zlib(data)
    assert zlib.decompress(compressed) == data

def test_decoder_b64(decoder):
    data = b"dGVzdCBkYXRh"  # "test data" in base64
    decoded = decoder._decode_b64(data)
    assert decoded == b"test data"

def test_decoder_zlib(decoder):
    data = b'x\x9c+I-.\x01\x00\x04]\x01\xc1'  # "test" compressed
    decompressed = decoder._decode_zlib(data)
    assert decompressed == b"test"

def test_encoder_as_string():
    encoder = Encoder(b64=True, compress=True, as_string=True)
    data = {"test": "data"}
    encoded = encoder.encode(data)
    assert isinstance(encoded, str)

def test_decoder_as_string():
    decoder = Decoder(b64=True, compress=True, as_string=True)
    encoded_str = "eJyrVipJLS5RslJQSkksSVSqBQAs1AU1"  # Encoded and compressed {"test": "data"}
    decoded = decoder.decode(encoded_str)
    assert decoded == {"test": "data"}

def test_encoder_no_compression():
    encoder = Encoder(b64=True, compress=False)
    data = {"test": "data"}
    encoded = encoder.encode(data)
    decoded_json = base64.b64decode(encoded).decode('utf-8')
    assert json.loads(decoded_json) == data

