from .utils import *
from .serialize import Serializer, Deserializer
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class Encoder:
    def __init__(self, b64=DEFAULT_B64, compress=DEFAULT_COMPRESS, as_string=False):
        self.b64 = b64
        self.compress = compress
        self.as_string = as_string
        self.serializer = Serializer()

    @cached_property
    def de(self):
        return Decoder(b64=self.b64, compress=self.compress, as_string=self.as_string)

    def __getitem__(self, obj):
        return self.encode(obj)

    def encode(self, obj):
        data = self.serializer[obj]

        data = json.dumps(data, sort_keys=True).encode('utf-8')
        
        if self.compress:
            data = self._encode_zlib(data)
        
        if self.b64 or self.as_string:
            data = self._encode_b64(data)
        
        if self.as_string:
            data = data.decode('utf-8')
        
        return data
    
    def decode(self, obj):
        return self.de[obj]

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

class Decoder(Encoder):
    def __getitem__(self, data):
        return self.decode(data)

    def decode(self, data):
        logger.debug(f"Input data: {data[:100]}...")  # Log first 100 characters

        if isinstance(data, str):
            data = data.encode('utf-8')
        
        if self.b64 or self.as_string:
            data = self._decode_b64(data)
            logger.debug(f"After base64 decoding: {data[:100]}...")

        if self.compress:
            data = self._decode_zlib(data)
            logger.debug(f"After decompression: {data[:100]}...")

        try:
            decoded_data = data.decode('utf-8')
            logger.debug(f"UTF-8 decoded data: {decoded_data[:100]}...")
        except UnicodeDecodeError:
            logger.debug("UTF-8 decoding failed, assuming already decoded")
            decoded_data = data

        try:
            parsed_data = json.loads(decoded_data)
            logger.debug(f"Parsed JSON data: {parsed_data}")
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing failed: {e}")
            raise ValueError(f"Failed to parse JSON data: {e}")

        return self.serializer.de[parsed_data]