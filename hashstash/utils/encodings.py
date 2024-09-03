from . import *
import zlib
import base64
import hashlib


@log.debug
def encode(data: Union[str, bytes], b64=DEFAULT_B64, compress=DEFAULT_COMPRESS, as_string=False):
    if not isinstance(data, (str, bytes)):
        raise ValueError("Input data must be either a string or bytes.")
    data_b = data.encode() if isinstance(data, str) else data
    return _encode(data_b, b64=b64 or as_string, compress=compress, as_string=as_string)

def _encode(data_b: bytes, b64=DEFAULT_B64, compress=DEFAULT_COMPRESS, as_string=False):
    if compress:
        data_b = encode_compressed(data_b, compress)
    if b64:
        data_b = encode_b64(data_b)
    return data_b if not as_string else data_b.decode('utf-8')

@log.debug
def decode(data, b64=DEFAULT_B64, compress=DEFAULT_COMPRESS, as_string=False):
    data_b = data.encode() if isinstance(data, str) else data
    data_b = _decode(data_b, b64=b64, compress=compress)
    return data_b.decode('utf-8') if as_string else data_b

def _decode(data_b, b64=DEFAULT_B64, compress=DEFAULT_COMPRESS):
    if b64:
        data_b = decode_b64(data_b)
    if compress:
        data_b = decode_compressed(data_b, compress)
    return data_b

def encode_compressed(data, compress_type=DEFAULT_COMPRESS):
    compress_type = get_compresser(compress_type)
    if compress_type == RAW_NO_COMPRESS:
        return data
    try:
        if compress_type == 'zlib':
            return zlib.compress(data)
        elif compress_type == 'blosc':
            import blosc
            return blosc.compress(data)
        elif compress_type == 'lz4':
            import lz4.block
            return lz4.block.compress(data)
        elif compress_type == 'gzip':
            import gzip
            return gzip.compress(data, mtime=0)  # Ensure deterministic output by setting mtime to 0
        elif compress_type == 'bz2':
            import bz2
            return bz2.compress(data)
        else:
            raise ValueError(f"Unsupported compression type: {compress_type}")
    except Exception as e:
        log.error(f"Compression error: {e}")
        return data

def decode_compressed(data, compress_type=DEFAULT_COMPRESS):
    compress_type = get_compresser(compress_type)
    if compress_type == RAW_NO_COMPRESS:
        return data
    try:
        if compress_type == 'zlib':
            return zlib.decompress(data)
        elif compress_type == 'blosc':
            import blosc
            return blosc.decompress(data)
        elif compress_type == 'lz4':
            import lz4.block
            return lz4.block.decompress(data)
        elif compress_type == 'gzip':
            import gzip
            return gzip.decompress(data)
        elif compress_type == 'bz2':
            import bz2
            return bz2.decompress(data)
        else:
            raise ValueError(f"Unsupported compression type: {compress_type}")
    except Exception as e:
        log.error(f"Decompression error: {e}")
        raise e
        return data

def encode_b64(data):
    try:
        return base64.b64encode(data)
    except Exception as e:
        log.debug(f"Base64 encoding error: {e}")
        return data

def decode_b64(data):
    try:
        return base64.b64decode(data)
    except Exception as e:
        log.debug(f"Base64 decoding error: {e}")
        return data

def encode_hash(data_b):
    if isinstance(data_b, str):
        data_b = data_b.encode()
    return hashlib.md5(data_b).hexdigest()

