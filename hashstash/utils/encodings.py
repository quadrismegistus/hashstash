from .utils import *
from ..serialize import deserialize, serialize


@debug
def encode(obj, b64=DEFAULT_B64, compress=DEFAULT_COMPRESS, as_string=False):
    from ..serialize import serialize
    data = serialize(obj)
    data_b = data.encode()
    if compress:
        data_b = encode_zlib(data_b)
    if b64 or as_string:
        data_b = encode_b64(data_b) # ensure decodable as string
    return data_b if not as_string else data_b.decode('utf-8')

@debug
def decode(data, b64=DEFAULT_B64, compress=DEFAULT_COMPRESS, as_string=True):

    print(f"Input data: {data}")  # Log first 100 characters

    data_b = data.encode('utf-8') if isinstance(data, str) else data

    if b64 or as_string:
        data_b = decode_b64(data_b)

    if compress:
        data_b = decode_zlib(data_b)

    data = data_b.decode('utf-8')    
    return deserialize(data)

    # try:
    #     parsed_data = json.loads(decoded_data)
    #     logger.debug(f"Parsed JSON data: {parsed_data}")
    # except json.JSONDecodeError as e:
    #     logger.error(f"JSON parsing failed: {e}")
    #     raise ValueError(f"Failed to parse JSON data: {e}")

    return data

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