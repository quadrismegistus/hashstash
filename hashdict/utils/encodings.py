from .utils import *

class Encoder:
    def __init__(self, b64=DEFAULT_B64, compress=DEFAULT_COMPRESS, as_string=False):
        self.b64 = b64
        self.compress = compress
        self.as_string = as_string

    def encode(self, obj):
        data = jsonpickle.encode(obj).encode()
        
        if self.compress:
            data = self._encode_zlib(data)
        
        if self.b64:
            data = self._encode_b64(data)
        
        if self.as_string:
            data = data.decode('utf-8')
        
        return data

    def decode(self, data):
        if self.as_string and isinstance(data, str):
            data = data.encode('utf-8')

        if self.b64:
            data = self._decode_b64(data)
        
        if self.compress:
            data = self._decode_zlib(data)
        
        return jsonpickle.decode(data)
    
    def hash(self, data):
        return self._encode_hash(data)

    @staticmethod
    def _encode_zlib(data):
        try:
            return zlib.compress(data)
        except Exception as e:
            logger.debug(f"Compression error: {e}")
            return data

    @staticmethod
    def _encode_b64(data):
        try:
            return base64.b64encode(data)
        except Exception as e:
            logger.debug(f"Base64 encoding error: {e}")
            return data

    @staticmethod
    def _decode_b64(data):
        try:
            return base64.b64decode(data)
        except Exception as e:
            logger.debug(f"Base64 decoding error: {e}")
            return data

    @staticmethod
    def _decode_zlib(data):
        try:
            return zlib.decompress(data)
        except Exception as e:
            logger.debug(f"Decompression error: {e}")
            return data
        
    @staticmethod
    def _encode_hash(data_b):
        if type(data_b) is str: data_b=data_b.encode()
        return hashlib.md5(data_b).hexdigest()

# Example usage:
# encoder = Encoder(b64=True, compress=False)
# assert encoder.decode(encoder.encode('unencoded')) == 'unencoded'