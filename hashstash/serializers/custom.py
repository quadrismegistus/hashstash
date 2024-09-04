from . import *
from ..utils.logs import *
from pprint import pprint
import json
from typing import Any
from pathlib import Path
from ..utils.misc import ReusableGenerator

PANDAS_EXTENSION_ACTIVATED = True

def dump_json(obj,as_string=False):
    try:
        # res = serialize_orjson(obj)
        res = serialize_json(obj)
        log.debug('serialized via orjson')
        if as_string: res = res.decode('utf-8')
    except ImportError:
        res = serialize_json(obj)
        if not as_string: res = res.encode('utf-8')
    return res
    

@log.debug
def serialize_custom(obj: Any) -> str:
    serialized = _serialize_custom(obj)
    return json.dumps(serialized)

def stuff(obj):
    return _serialize_custom(obj)
def unstuff(obj):
    return _deserialize_custom(obj)

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

    if hasattr(obj, 'to_dict') and callable(obj.to_dict) and not inspect.isclass(obj):
        return {
            '__py__': addr,
            '__data__': _serialize_custom(obj.to_dict())
        }
    
    if isinstance(obj, type):
        return ClassSerializer.serialize(obj)
    
    if inspect.isgenerator(obj):
        return GeneratorSerializer.serialize(obj)

    if is_function(obj):
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

def _deserialize_object_data(obj, obj_data: Any) -> Any:
    if hasattr(obj, 'from_dict') and callable(obj.from_dict):
        return obj.from_dict(obj_data)
    if hasattr(obj, '__setstate__'):
        obj.__setstate__(obj_data)
    else:
        obj.__dict__.update(obj_data)
    return obj
            


def _deserialize_custom(data: Any) -> Any:
    if isinstance(data, (str, int, float, bool, type(None))):
        return data
    
    if isinstance(data, list):
        return [_deserialize_custom(v) for v in data]
    
    if isinstance(data, dict):
        pytype = data.get('__pytype__')
        addr = data.get('__py__')
        
        if pytype == 'instance':
            return InstanceSerializer.deserialize(data)
        
        if addr and addr in CUSTOM_DESERIALIZERS:
            return CUSTOM_DESERIALIZERS[addr](data)
        
        if pytype == 'reducer':
            return ReducerSerializer.deserialize(data)
        
        if pytype in {'function', 'classmethod', 'instancemethod'}:
            return FunctionSerializer.deserialize(data)
        
        if pytype == 'class':
            return ClassSerializer.deserialize(data)
        
        if pytype == 'generator':
            return GeneratorSerializer.deserialize(data)

        obj_data = data.get('__data__')
        if obj_data and can_import_object(addr):
            return _deserialize_object_data(flexible_import(addr), _deserialize_custom(obj_data))
        
        if '__py__' in data:
            return flexible_import(data['__py__'])
        
        return {k: _deserialize_custom(v) for k, v in data.items()}
    
    return data



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

class MetaDataFrameSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        return {
            '__py__': get_obj_addr(obj),
            '__data__': obj.stuff()
        }
        # data = obj.to_dict()
        # data['data'] = PandasDataFrameSerializer.serialize(data['data'])
        # return {
        #     '__py__': get_obj_addr(obj),
        #     '__data__': data
        # }
    
    @staticmethod
    def deserialize(data):
        data = data['__data__']
        return MetaDataFrame.unstuff(data)
        # data['data'] = PandasDataFrameSerializer.deserialize(data['data'])
        # return MetaDataFrame(**data)

def deactivate_pandas_extension():
    global PANDAS_EXTENSION_ACTIVATED
    PANDAS_EXTENSION_ACTIVATED = False

def activate_pandas_extension():
    global PANDAS_EXTENSION_ACTIVATED
    PANDAS_EXTENSION_ACTIVATED = True

def pandas_extension_activated():
    global PANDAS_EXTENSION_ACTIVATED
    return PANDAS_EXTENSION_ACTIVATED

@fcache
def pandas_installed():
    try:
        import pandas
        return True
    except ImportError:
        return False


class PandasDataFrameSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        assert pandas_installed(), "Pandas is required for this serializer."
        if pandas_extension_activated():
            # log.debug("serializing MetaDataFrame")
            mdf = MetaDataFrame(obj)
            return {
                '__py__': get_obj_addr(obj),
                '__data__': MetaDataFrameSerializer.serialize(mdf)
            }
        else:
            df, index = reset_index_misc(obj, _index=True)
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
        assert pandas_installed(), "Pandas is required for this serializer."

        if pandas_extension_activated():
            # log.info("deserializing MetaDataFrame")
            mdf = MetaDataFrameSerializer.deserialize(data['__data__'])
            # log.debug(f"deserialized MetaDataFrame with shape {mdf.data.shape}")
            return mdf.data
        else:
            import pandas as pd
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
        try:
            import numpy as np
        except ImportError:
            raise ImportError("NumPy is required for this deserializer.")
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
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("Pandas is required for this deserializer.")
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
                '__pytype__': 'reducer',
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
            '__pytype__': 'bytes',
            '__data__': encode(obj, compress=False, b64=True, as_string=True)
        }

    @staticmethod
    def deserialize(data):
        return decode(data['__data__'], compress=False, b64=True)


class FunctionSerializer(CustomSerializer):
    # @log.info
    @staticmethod
    def serialize(obj, incl_class=True):
        func = unwrap_func(obj)
        obj_d = {}
        obj_d['__py__'] = full_name = get_obj_addr(obj)
        pytype = obj_d['__pytype__'] = get_pytype(obj)
        if pytype == 'classmethod':
            if incl_class:
                obj_d['__cls__'] = ClassSerializer.serialize(get_class_from_method(obj))
                return obj_d
        
        elif pytype == 'instancemethod':
            obj_d['__self__'] = InstanceSerializer.serialize(obj.__self__)
        
        if not can_import_object(full_name) or get_obj_module(obj) == '__main__':
            obj_d['__source__'] =  get_function_src(obj)

        return obj_d
        

    @staticmethod
    @log.debug
    def deserialize(data):
        log.debug(f"Deserializing function: {data['__py__']}")
        
        # If only __py__ is present, try to import the function
        # if len(data) == 1 and '__py__' in data:
        #     obj = flexible_import(data['__py__'])
        #     return classmethod(obj) if data['__pytype__'] == 'classmethod' else obj
        
        pytype = data.get('__pytype__')
        pyaddr = data.get('__py__')
        #pprint(data)
        pyname = pyaddr.split('.')[-1]
        if pytype == 'classmethod':
            cls = ClassSerializer.deserialize(data['__cls__'])
            return getattr(cls, pyname)
        
        if pytype == 'instancemethod':
            obj = InstanceSerializer.deserialize(data['__self__'])
            return getattr(obj, pyname)

        elif can_import_object(pyaddr):
            return flexible_import(pyaddr)

        elif '__source__' in data:
            source = data['__source__']
            func_name = data['__py__'].split('.')[-1]
            return recreate_function_from_src(source, func_name)
        
        else:
            #pprint(data)
            raise Exception('what happened?')
            
        
        return func


def recreate_function_from_src(source, func_name):
    # Handle lambda functions
    if source.startswith('lambda'):
        lambda_expr = source.split(':')[0] + ':' + source.split(':')[1].split(',')[0]
        code = compile(lambda_expr, '<string>', 'eval')
        return eval(code)
    
    code = compile(source, '<string>', 'exec')
    
    try:
        namespace = globals()
        exec(code, namespace)
        func = namespace[func_name]
        return func

        # closure = get_function_closure(func)    
        # if closure:
        #     func.__closure__ = tuple(cell(v) for v in closure.values())
    except Exception as e:
        log.error(f"Error creating function: {e}")
        raise


class ClassSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        if can_import_object(obj):
            return {'__py__': get_obj_addr(obj), '__pytype__': 'class'}
        
        return {
            '__py__': get_obj_addr(obj),
            '__pytype__': 'class',
            '__bases__': [get_obj_addr(base) for base in obj.__bases__],
            '__methods__': {
                key: FunctionSerializer.serialize(func, incl_class=False) for key,func in obj.__dict__.items()
                if is_function(unwrap_func(func))
            },
            '__attrs__': {
                key: _serialize_custom(attr) for key,attr in obj.__dict__.items()
                if not is_function(unwrap_func(attr)) and not key.startswith('__')
            }

            
            # '__dict__': {
            #     k: _serialize_custom(v) for k, v in obj.__dict__.items()
            #     if not k.startswith('__') or k in {'__init__'}
            # },
            # '__methods__': {
            #     name: get_obj_addr(method) if can_import_object(method) else FunctionSerializer.serialize(method)
            #     for name, method in obj.__dict__.items()
            #     if callable(method) and not name.startswith('__')
            # }
        }

    @staticmethod
    def deserialize(data):
        if can_import_object(data['__py__']):
            return flexible_import(data['__py__'])
        
        bases = tuple(flexible_import(base) for base in data['__bases__'])
        
        # Create a new namespace for the class
        namespace = globals().copy()
        
        # Deserialize and add methods to the namespace
        for name, func_d in data['__methods__'].items():
            namespace[name] = FunctionSerializer.deserialize(func_d)



        # Create the class
        module_path, class_name = data['__py__'].rsplit('.', 1)
        cls = type(class_name, bases, namespace)
        cls.__module__ = module_path
        cls.__qualname__ = class_name
        
        return cls





    # @staticmethod
    # def deserialize(data):
    #     if '__name__' not in data:
    #         return flexible_import(data['__py__'])
        
    #     bases = tuple(flexible_import(base) for base in data['__bases__'])
        
    #     # Create a new namespace for the class
    #     namespace = {}
        
    #     # Deserialize and add methods to the namespace
    #     for name, method_data in data['__methods__'].items():
    #         if isinstance(method_data, dict):
    #             namespace[name] = FunctionSerializer.deserialize(method_data)
    #         else:
    #             namespace[name] = flexible_import(method_data)
        
    #     # Deserialize and add other attributes to the namespace
    #     for k, v in data['__dict__'].items():
    #         if k not in namespace:  # Don't overwrite methods
    #             namespace[k] = _deserialize_custom(v)
        
    #     # Create the class
    #     cls = type(data['__name__'], bases, namespace)
    #     cls.__module__ = data['__module__']
    #     cls.__qualname__ = data['__qualname__']  # Add this line
        
    #     return cls


# class ClassSerializer(CustomSerializer):
#     @staticmethod
#     def serialize(obj):
#         obj_d = {'__py__': get_obj_addr(obj)}
#         if not can_import_object(obj):
#             obj_d['__source__'] =  get_class_src(obj)
#         return obj_d
#             # return {
#             #     '__py__': get_obj_addr(obj),
#             #     '__name__': obj.__name__,
#             #     '__module__': obj.__module__,
#             #     '__qualname__': obj.__qualname__,  # Add this line
#             # '__bases__': [get_obj_addr(base) for base in obj.__bases__],
#             # '__dict__': {
#             #     k: _serialize_custom(v) for k, v in obj.__dict__.items()
#             #     if not k.startswith('__') or k in {'__init__'}
#             # },
#             # '__methods__': {
#             #     name: get_obj_addr(method) if can_import_object(method) else FunctionSerializer.serialize(method)
#             #     for name, method in obj.__dict__.items()
#             #     if callable(method) and not name.startswith('__')
#             # }
#         # }

#     @staticmethod
#     def deserialize(data):
#         if can_import_object(data['__py__']):
#             return flexible_import(data['__py__'])
        
#         # Create a new namespace based on the current globals
#         namespace = globals().copy()
        
#         # Compile and execute the source code in the new namespace
#         code = compile(data['__source__'], '<string>', 'exec')
#         exec(code, namespace)
        
#         # Extract the class name and module path from __py__
#         module_path, class_name = data['__py__'].rsplit('.', 1)
        
#         # Get the class object from the namespace
#         cls = namespace[class_name]
        
#         # Set the __module__ attribute
#         cls.__module__ = module_path
        
#         # Set the __qualname__ attribute if it's not already set
#         cls.__qualname__ = class_name
#         # Attach source code to methods
#         for name, method in inspect.getmembers(cls, predicate=inspect.isfunction):
#             method.__source__ = inspect.getsource(method)
        
        
#         # Update the class in the namespace
#         namespace[class_name] = cls
        
#         return cls

    # def deserialize(data):
    #     if can_import_object(data['__py__']):
    #         return flexible_import(data['__py__'])
        
    #     # Create a new namespace to hold the class
    #     namespace = globals().c
        
    #     # Compile and execute the source code in the new namespace
    #     code = compile(data['__source__'], '<string>', 'exec')
    #     exec(code, namespace)
        
    #     # Extract the class object from the namespace
    #     class_name = data['__py__'].split('.')[-1]
    #     if class_name in namespace:
    #         return namespace[class_name]
    #     else:
    #         raise ValueError(f"Class {class_name} not found in the executed code")
        

    #     if can_import_object(data['__py__']):
    #         return flexible_import(data['__py__'])
        
    #     code = compile(data['__source__'], '<string>', 'exec')
    #     exec(code)
    #     return flexible_import(data['__py__'])

    #     if '__name__' not in data:
    #         return flexible_import(data['__py__'])
        
    #     bases = tuple(flexible_import(base) for base in data['__bases__'])
        
    #     # Create a new namespace for the class
    #     namespace = {}
        
    #     # Deserialize and add methods to the namespace
    #     for name, method_data in data['__methods__'].items():
    #         if isinstance(method_data, dict):
    #             namespace[name] = FunctionSerializer.deserialize(method_data)
    #         else:
    #             namespace[name] = flexible_import(method_data)
        
    #     # Deserialize and add other attributes to the namespace
    #     for k, v in data['__dict__'].items():
    #         if k not in namespace:  # Don't overwrite methods
    #             namespace[k] = _deserialize_custom(v)
        
    #     # Create the class
    #     cls = type(data['__name__'], bases, namespace)
    #     cls.__module__ = data['__module__']
    #     cls.__qualname__ = data['__qualname__']  # Add this line
        
    #     return cls

class InstanceSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        addr = get_obj_addr(obj)
        cls = obj.__class__
        return {
            '__py__': addr,
            '__pytype__': 'instance',
            '__cls__': ClassSerializer.serialize(cls),
            '__state__': _serialize_custom(obj.__dict__)
        }

    @staticmethod
    def deserialize(data):
        if isinstance(data['__cls__'], dict):
            cls = ClassSerializer.deserialize(data['__cls__'])
        else:
            cls = flexible_import(data['__cls__'])
        
        instance = cls.__new__(cls)
        instance.__dict__.update(_deserialize_custom(data['__state__']))
        return instance

class GeneratorSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        # We'll serialize the generator function and its current state
        return {
            '__py__': get_obj_addr(obj),
            '__pytype__': 'generator',
            '__state__': _serialize_custom(list(obj))  # This consumes the generator
        }

    @staticmethod
    def deserialize(data):
        state = _deserialize_custom(data['__state__'])
        
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

class ReusableGeneratorSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        return {
            '__py__': get_obj_addr(obj),
            '__func__': FunctionSerializer.serialize(obj.func),
            '__args__': _serialize_custom(obj.args),
            '__kwargs__': _serialize_custom(obj.kwargs)
        }

    @staticmethod
    def deserialize(data):
        func = FunctionSerializer.deserialize(data['__func__'])
        args = _deserialize_custom(data['__args__'])
        kwargs = _deserialize_custom(data['__kwargs__'])
        return ReusableGenerator(func, *args, **kwargs)


def get_function_closure(func):
    if not hasattr(func, '__closure__') or not hasattr(func, '__code__'):
        return None
    obj = func
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
    return closure_dict if closure_dict else None




class PmapSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        return {
            '__py__': get_obj_addr(obj),
            '__data__': _serialize_custom(obj.to_dict())
        }

    @staticmethod
    def deserialize(data):
        from ..utils.pmap import Pmap
        return Pmap.from_dict(
            _deserialize_custom(data['__data__'])
        )

class PmapResultSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        return {
            '__py__': get_obj_addr(obj),
            '__pytype__': 'PmapResult',
            '__data__': {
                'func': FunctionSerializer.serialize(obj.func),
                'args': _serialize_custom(obj.args),
                'kwargs': _serialize_custom(obj.kwargs),
                '_result': _serialize_custom(obj._result),
                '_computed': obj._computed,
                '_processing_started': obj._processing_started,
            }
        }

    @staticmethod
    def deserialize(data):
        from ..utils.pmap import PmapResult
        result_data = _deserialize_custom(data['__data__'])
        result = PmapResult(
            FunctionSerializer.deserialize(result_data['func']),
            result_data['args'],
            result_data['kwargs'],
            None  # We'll set _pmap_instance later
        )
        result._result = result_data['_result']
        result._computed = result_data['_computed']
        result._processing_started = result_data['_processing_started']
        return result


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
    'hashstash.utils.misc.ReusableGenerator': ReusableGeneratorSerializer.serialize,
    'hashstash.utils.dataframes.MetaDataFrame': MetaDataFrameSerializer.serialize,
    # 'hashstash.utils.pmap.Pmap': PmapSerializer.serialize,
    # 'hashstash.utils.pmap.PmapResult': PmapResultSerializer.serialize,
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
    'hashstash.utils.misc.ReusableGenerator': ReusableGeneratorSerializer.deserialize,
    'hashstash.utils.dataframes.MetaDataFrame': MetaDataFrameSerializer.deserialize,
    # 'hashstash.utils.pmap.Pmap': PmapSerializer.deserialize,
    # 'hashstash.utils.pmap.PmapResult': PmapResultSerializer.deserialize,
}
