from .utils import *
from ..serialize import deserialize, serialize


@debug
def encode(data, b64=DEFAULT_B64, compress=DEFAULT_COMPRESS, as_string=False):
    data = serialize(data, as_string=True)
    data_b = data.encode()
    return _encode(data_b, b64=b64 or as_string, compress=compress, as_string=as_string)

def _encode(data_b, b64=DEFAULT_B64, compress=DEFAULT_COMPRESS, as_string=False):
    if compress:
        data_b = encode_zlib(data_b)
    if b64:
        data_b = encode_b64(data_b)

    return data_b if not as_string else data_b.decode('utf-8')

@debug
def decode(data, b64=DEFAULT_B64, compress=DEFAULT_COMPRESS, as_string=False, as_bytes=False):
    data_b = data.encode('utf-8') if isinstance(data, str) else data
    data_b = _decode(data_b, b64=b64, compress=compress)
    if as_bytes:
        return data_b
    else:
        data = data_b.decode('utf-8')
        return data if as_string else deserialize(data)

def _decode(data_b, b64=DEFAULT_B64, compress=DEFAULT_COMPRESS):
    if b64:
        data_b = decode_b64(data_b)
    if compress:
        data_b = decode_zlib(data_b)
    return data_b


def encode_zlib(data):
    try:
        return zlib.compress(data)
    except Exception as e:
        logger.debug(f"Compression error: {e}")
        return data

def encode_b64(data):
    try:
        return base64.b64encode(data)
    except Exception as e:
        logger.debug(f"Base64 encoding error: {e}")
        return data

def decode_b64(data):
    try:
        return base64.b64decode(data)
    except Exception as e:
        logger.debug(f"Base64 decoding error: {e}")
        return data

def decode_zlib(data):
    try:
        return zlib.decompress(data)
    except Exception as e:
        logger.debug(f"Decompression error: {e}")
        return data

def encode_hash(data_b):
    if isinstance(data_b, str):
        data_b = data_b.encode()
    return hashlib.md5(data_b).hexdigest()