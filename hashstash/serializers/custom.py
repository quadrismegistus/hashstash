from . import *
from pprint import pprint
import json
from typing import Any
import numpy as np
import pandas as pd
from pathlib import Path

arr = np.array([[[1,2,3,4,5],[6,7,8,9,10]],[[11,12,13,14,15],[16,17,18,19,20]]])

@log.debug
def serialize_custom(obj: Any) -> str:
    serialized = _serialize_custom(obj)
    print(serialized)
    return json.dumps(serialized)

@log.debug
def _serialize_custom(obj: Any) -> Any:
    if obj is None:
        return None
    
    if isinstance(obj, (str, int, float, bool)):
        return obj
    
    if isinstance(obj, dict):
        return {k: _serialize_custom(v) for k, v in obj.items()}
    
    if isinstance(obj, list):
        return [_serialize_custom(v) for v in obj]

    addr = get_obj_addr(obj)
    if addr in CUSTOM_SERIALIZERS:
        return CUSTOM_SERIALIZERS[addr](obj)

    if hasattr(obj, 'to_dict') and callable(obj.to_dict):
        return {
            '__py__': addr,
            '__data__': _serialize_custom(obj.to_dict())
        }
    
    if isinstance(obj, type):
        return ClassSerializer.serialize(obj)
    
    if inspect.isgenerator(obj):
        return GeneratorSerializer.serialize(obj)

    if isinstance(obj, (types.FunctionType, types.LambdaType)) or callable(obj):
        return FunctionSerializer.serialize(obj)
    
    # Handle class instances
    if hasattr(obj, '__dict__'):
        return InstanceSerializer.serialize(obj)

    if hasattr(obj, '__reduce__'):
        return ReducerSerializer.serialize(obj)
    
    log.warning(f"Unsupported object type: {type(obj)}")
    return obj


### Deserializing

def deserialize_custom(serialized_str: str) -> Any:
    return _deserialize_custom(json.loads(serialized_str))

def _deserialize_custom(data: Any) -> Any:
    if isinstance(data, (str, int, float, bool, type(None))):
        return data
    
    if isinstance(data, list):
        return [_deserialize_custom(v) for v in data]
    
    if isinstance(data, dict):
        if '__class__' in data:
            return InstanceSerializer.deserialize(data)
        
        addr = data.get('__py__')
        if addr and addr in CUSTOM_DESERIALIZERS:
            return CUSTOM_DESERIALIZERS[addr](data)
        
        if '__state__' in data:
            return InstanceSerializer.deserialize(data)
        
        if '__args__' in data:  # Check if it's a reduced object
            return ReducerSerializer.deserialize(data)
        
        if '__closure__' in data:
            return FunctionSerializer.deserialize(data)
        
        if '__bases__' in data:
            return ClassSerializer.deserialize(data)
        
        if '__generator_state__' in data:
            return GeneratorSerializer.deserialize(data)

        obj_data = data.get('__data__')
        if obj_data:
            obj = flexible_import(addr)
            if hasattr(obj, 'from_dict') and callable(obj.from_dict):
                return obj.from_dict(_deserialize_custom(obj_data))
            
        if '__py__' in data:
            return flexible_import(data['__py__'])
            
        
        return {k: _deserialize_custom(v) for k, v in data.items()}
    
    
    return obj



## custom object de/serializers

class CustomSerializer:
    @staticmethod
    def serialize(obj: Any) -> dict:
        raise NotImplementedError

    @staticmethod
    def deserialize(data: dict) -> Any:
        raise NotImplementedError

class IterableSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        return {
            '__py__': get_obj_addr(obj),
            '__data__': [_serialize_custom(x) for x in obj]
        }

    @staticmethod
    def deserialize(data):
        obj = flexible_import(data['__py__'])
        return obj(_deserialize_custom(data['__data__']))

class PandasDataFrameSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        df, index = reset_index(obj)
        return {
            '__py__': get_obj_addr(obj),
            '__data__': {
                'values': NumpySerializer.serialize(df.values),
                'columns': NumpySerializer.serialize(df.columns.values),
                'index_columns': _serialize_custom(index),
                'dtypes': _serialize_custom({k:str(v) for k,v in df.dtypes.to_dict().items()})
            }
        }

    @staticmethod
    def deserialize(data):
        values = NumpySerializer.deserialize(data['__data__']['values'])
        columns = NumpySerializer.deserialize(data['__data__']['columns'])
        index_columns = _deserialize_custom(data['__data__'].get('index_columns'))
        dtypes = _deserialize_custom(data['__data__']['dtypes'])
        
        df = pd.DataFrame(values, columns=columns)
        for col, dtype in dtypes.items():
            df[col] = df[col].astype(dtype)
        
        if index_columns:
            df = df.set_index(index_columns)
        if index_columns == ['_index']:
            df = df.rename_axis(None)
        return df

class NumpySerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        outd = {
            '__py__': get_obj_addr(obj),
            '__data__': {
                'dtype': str(obj.dtype),
                'shape': obj.shape
            }
        }
        if obj.dtype.kind == 'O':
            outd['__data__']['values'] = [_serialize_custom(item) for item in obj.flatten()]
        else:
            outd['__data__']['bytes'] = encode(obj.tobytes(), compress=False, b64=True, as_string=True)
        return outd

    @staticmethod
    def deserialize(data):
        dtype = data['__data__']['dtype']
        shape = data['__data__']['shape']
        if 'bytes' in data['__data__']:
            arr_bytes = decode(data['__data__']['bytes'], compress=False, b64=True)
            return np.frombuffer(arr_bytes, dtype=dtype).reshape(shape)
        else:
            return np.array([_deserialize_custom(item) for item in data['__data__']['values']], dtype=dtype).reshape(shape)

class PandasSeriesSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        return {
            '__py__': get_obj_addr(obj),
            '__data__': {
                'values': NumpySerializer.serialize(obj.values),
                'index': NumpySerializer.serialize(obj.index.values)
            }
        }

    @staticmethod
    def deserialize(data):
        return pd.Series(
            NumpySerializer.deserialize(data['__data__']['values']),
            index=NumpySerializer.deserialize(data['__data__']['index'])
        )

class ReducerSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        try:
            reduced = obj.__reduce__()
            if not isinstance(reduced, tuple) or len(reduced) < 2:
                raise ValueError("Invalid __reduce__ output")
            
            result = {
                '__py__': get_obj_addr(reduced[0]),
                '__args__': list(reduced[1]) if reduced[1] else []
            }
            if len(reduced) > 2:
                result['__state__'] = reduced[2]
            if len(reduced) > 3:
                result['__listitems__'] = reduced[3]
            if len(reduced) > 4:
                result['__dictitems__'] = reduced[4]
            if len(reduced) > 5:
                result['__state_setter__'] = get_obj_addr(reduced[5])
            return result
        except Exception as e:
            log.warning(f"Error using __reduce__ for {type(obj)}: {e}")
            return None

    @staticmethod
    def deserialize(data):
        try:
            constructor = flexible_import(data['__py__'])
            args = _deserialize_custom(data['__args__'])
            state = _deserialize_custom(data.get('__state__'))
            listitems = _deserialize_custom(data.get('__listitems__'))
            dictitems = _deserialize_custom(data.get('__dictitems__'))
            state_setter = flexible_import(data.get('__state_setter__')) if data.get('__state_setter__') else None

            obj = constructor(*args)

            if state is not None:
                if state_setter:
                    state_setter(obj, state)
                elif hasattr(obj, '__setstate__'):
                    obj.__setstate__(state)
                else:
                    obj.__dict__.update(state)

            if listitems is not None:
                obj.extend(listitems)

            if dictitems is not None:
                obj.update(dictitems)

            return obj
        except Exception as e:
            log.warning(f"Error using safe_unreduce: {e}")
            return None
        
class BytesSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        return {
            '__py__': get_obj_addr(obj),
            '__data__': encode(obj, compress=False, b64=True, as_string=True)
        }

    @staticmethod
    def deserialize(data):
        return decode(data['__data__'], compress=False, b64=True)

class FunctionSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        log.debug(f"Serializing function: {obj.__name__}")
        log.debug(f"Function closure: {obj.__closure__}")
        log.debug(f"Function free variables: {obj.__code__.co_freevars}")

        if can_import_object(obj):
            return {'__py__': get_obj_addr(obj)}
        else:
            closure_dict = {}
            if obj.__closure__:
                for name, cell in zip(obj.__code__.co_freevars, obj.__closure__):
                    try:
                        if isinstance(cell.cell_contents, types.FunctionType):
                            closure_dict[name] = f"<function {cell.cell_contents.__name__}>"
                        elif isinstance(cell.cell_contents, type(obj)):
                            closure_dict[name] = "<self>"
                        else:
                            closure_dict[name] = _serialize_custom(cell.cell_contents)
                    except ValueError:
                        log.warning(f"Empty cell encountered for {name} in function {obj.__name__}")
                        closure_dict[name] = None

            return {
                '__py__': get_obj_addr(obj),
                '__source__': get_function_src(obj),
                '__closure__': closure_dict if closure_dict else None
            }

    @staticmethod
    def deserialize(data):
        log.debug(f"Deserializing function: {data['__py__']}")
        source = data['__source__']
        
        # Handle lambda functions
        if source.startswith('lambda'):
            # Extract just the lambda expression
            lambda_expr = source.split(':')[0] + ':' + source.split(':')[1].split(',')[0]
            code = compile(lambda_expr, '<string>', 'eval')
            return eval(code)
        
        code = compile(source, '<string>', 'exec')
        closure = {}
        if data.get('__closure__'):
            for k, v in data['__closure__'].items():
                if v == "<self>":
                    closure[k] = None
                elif isinstance(v, str) and v.startswith("<function "):
                    closure[k] = lambda: None
                else:
                    closure[k] = _deserialize_custom(v)
        
        func_name = data['__py__'].split('.')[-1]
        
        # Create a new function object
        func = types.FunctionType(code.co_consts[0], closure, func_name)
        
        # Update self-referential closures
        for k, v in closure.items():
            if v is None:
                closure[k] = func
        
        return func

class ClassSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        if can_import_object(obj):
            return {'__py__': get_obj_addr(obj)}
        
        return {
            '__py__': get_obj_addr(obj),
            '__name__': obj.__name__,
            '__module__': obj.__module__,
            '__qualname__': obj.__qualname__,  # Add this line
            '__bases__': [get_obj_addr(base) for base in obj.__bases__],
            '__dict__': {
                k: _serialize_custom(v) for k, v in obj.__dict__.items()
                if not k.startswith('__') or k == '__init__'
            },
            '__methods__': {
                name: get_obj_addr(method) if can_import_object(method) else FunctionSerializer.serialize(method)
                for name, method in obj.__dict__.items()
                if callable(method) and not name.startswith('__')
            }
        }

    @staticmethod
    def deserialize(data):
        if '__name__' not in data:
            return flexible_import(data['__py__'])
        
        bases = tuple(flexible_import(base) for base in data['__bases__'])
        
        # Create a new namespace for the class
        namespace = {}
        
        # Deserialize and add methods to the namespace
        for name, method_data in data['__methods__'].items():
            if isinstance(method_data, dict):
                namespace[name] = FunctionSerializer.deserialize(method_data)
            else:
                namespace[name] = flexible_import(method_data)
        
        # Deserialize and add other attributes to the namespace
        for k, v in data['__dict__'].items():
            if k not in namespace:  # Don't overwrite methods
                namespace[k] = _deserialize_custom(v)
        
        # Create the class
        cls = type(data['__name__'], bases, namespace)
        cls.__module__ = data['__module__']
        cls.__qualname__ = data['__qualname__']  # Add this line
        
        return cls

class InstanceSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        addr = get_obj_addr(obj)
        cls = obj.__class__
        if can_import_object(cls):
            return {
                '__py__': addr,
                '__class__': get_obj_addr(cls),
                '__state__': _serialize_custom(obj.__dict__)
            }
        else:
            return {
                '__py__': addr,
                '__class__': ClassSerializer.serialize(cls),
                '__state__': _serialize_custom(obj.__dict__)
            }

    @staticmethod
    def deserialize(data):
        if isinstance(data['__class__'], dict):
            cls = ClassSerializer.deserialize(data['__class__'])
        else:
            cls = flexible_import(data['__class__'])
        
        instance = cls.__new__(cls)
        instance.__dict__.update(_deserialize_custom(data['__state__']))
        return instance

class GeneratorSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        # We'll serialize the generator function and its current state
        return {
            '__py__': get_obj_addr(obj),
            '__generator_state__': _serialize_custom(list(obj))  # This consumes the generator
        }

    @staticmethod
    def deserialize(data):
        state = _deserialize_custom(data['__generator_state__'])
        
        def reconstructed_gen():
            yield from state
        
        return reconstructed_gen()

class PathSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        return {
            '__py__': get_obj_addr(obj),
            '__data__': str(obj)
        }

    @staticmethod
    def deserialize(data):
        return Path(data['__data__'])

CUSTOM_SERIALIZERS = {
    'pandas.core.frame.DataFrame': PandasDataFrameSerializer.serialize,
    'pandas.core.series.Series': PandasSeriesSerializer.serialize,
    'numpy.ndarray': NumpySerializer.serialize,
    'builtins.set': IterableSerializer.serialize,
    'builtins.tuple': IterableSerializer.serialize,
    'builtins.frozenset': IterableSerializer.serialize,
    'builtins.bytes': BytesSerializer.serialize,
    'types.FunctionType': FunctionSerializer.serialize,
    'types.LambdaType': FunctionSerializer.serialize,
    'type': ClassSerializer.serialize,
    'object': InstanceSerializer.serialize,
    'types.GeneratorType': GeneratorSerializer.serialize,
    'pathlib.PosixPath': PathSerializer.serialize,
    'pathlib.WindowsPath': PathSerializer.serialize,
}

CUSTOM_DESERIALIZERS = {
    'pandas.core.frame.DataFrame': PandasDataFrameSerializer.deserialize,
    'pandas.core.series.Series': PandasSeriesSerializer.deserialize,
    'numpy.ndarray': NumpySerializer.deserialize,
    'builtins.set': IterableSerializer.deserialize,
    'builtins.tuple': IterableSerializer.deserialize,
    'builtins.frozenset': IterableSerializer.deserialize,
    'builtins.bytes': BytesSerializer.deserialize,
    'function': FunctionSerializer.deserialize,
    'type': ClassSerializer.deserialize,
    'object': InstanceSerializer.deserialize,
    'types.GeneratorType': GeneratorSerializer.deserialize,
    'pathlib.PosixPath': PathSerializer.deserialize,
    'pathlib.WindowsPath': PathSerializer.deserialize,
}