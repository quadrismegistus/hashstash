from . import *

class Config:
    serializer: Union[SERIALIZER_TYPES, List[SERIALIZER_TYPES]] = DEFAULT_SERIALIZER
    engine: ENGINE_TYPES = DEFAULT_ENGINE_TYPE
    compress: bool = DEFAULT_COMPRESS
    b64: bool = DEFAULT_B64

    @classmethod
    def to_dict(cls):
        return {
            "serializer": cls.serializer,
            "engine": cls.engine,
            "compress": cls.compress,
            "b64": cls.b64,
        }
    
    def __repr__(self):
        return f'hashstash.Config({self.to_dict()})'

    @classmethod
    def set_serializer(cls, serializer: Union[SERIALIZER_TYPES, List[SERIALIZER_TYPES]]):
        # check if serializer is allowable
        if not serializer in set(SERIALIZER_TYPES.__args__):
            raise ValueError(f"Invalid serializer: {serializer}. Options: {', '.join(SERIALIZER_TYPES.__args__)}.")
        cls.serializer = serializer

    @classmethod
    def set_engine(cls, engine: ENGINE_TYPES):
        # check if engine is allowable
        if engine not in set(ENGINE_TYPES.__args__):
            raise ValueError(f"Invalid engine: {engine}. Options: {', '.join(ENGINE_TYPES.__args__)}.")
        cls.engine = engine
    
    @classmethod
    def set_compress(cls, compress: bool):
        cls.compress = compress
    
    @classmethod
    def set_b64(cls, b64: bool):
        cls.b64 = b64

    @classmethod
    def disable_compression(cls):
        cls.compress = False

    @classmethod
    def disable_b64(cls):
        cls.b64 = False


    @classmethod
    def enable_compression(cls):
        cls.compress = True

    @classmethod
    def enable_b64(cls):
        cls.b64 = True

config = Config()