from .utils import *

class SerializerBase(ABC):
    def __init__(self, instantiate=True):
        self.instantiate = instantiate

    @cached_property
    def de(self):
        return Deserializer(instantiate=self.instantiate)
    
    def __getitem__(self, key):
        return self.get(key)

    @staticmethod
    def get_obj_addr(obj):
        if hasattr(obj, '__module__') and hasattr(obj, '__name__'):
            return f"{obj.__module__}.{obj.__name__}"
        if hasattr(obj, '__class__'):
            cls = obj.__class__
            return f"{cls.__module__}.{cls.__name__}"
        if isinstance(obj, type):
            return obj.__name__
        return f"{type(obj).__module__}.{type(obj).__name__}"

    @staticmethod
    def get_function_str(func):
        try:
            return get_function_str(func)
        except Exception:
            func_code = func.__name__
        return func_code

    @staticmethod
    def is_jsonable(obj):
        try:
            json.dumps(obj)
            return True
        except Exception:
            return False

    @abstractmethod
    def get(self, obj):
        pass

class Serializer(SerializerBase):
    def get(self, obj):
        # first get any wrapped's
        while hasattr(obj, '__wrapped__'):
            obj = obj.__wrapped__

        if self.is_jsonable(obj):
            return obj
        
        if isinstance(obj, (list, tuple, set, frozenset)):
            return [self.get(item) for item in obj]
        
        if isinstance(obj, (dict, UserDict)):
            return {self.get(k): self.get(v) for k, v in obj.items()}
        
        if obj.__class__.__name__ == 'ndarray':
            return self.get_numpy(obj)
        
        if obj.__class__.__name__ == 'DataFrame':
            return self.get_pandas_df(obj)
        
        if obj.__class__.__name__ == 'Series':
            return self.get_pandas_series(obj)
        
        if isinstance(obj, types.FunctionType):
            return self.get_function(obj)
        
        if isinstance(obj, typing._SpecialForm):
            return {
                'py/typing._SpecialForm': obj._name
            }
        
        if hasattr(obj, '__dict__') or hasattr(obj, 'to_dict'):
            return self.get_object(obj)
        
        return {
            'py/unknown': self.get_obj_addr(obj)
        }

    def get_numpy(self, obj):
        return {
            'py/object': self.get_obj_addr(obj),
            'values': obj.tolist(),
            'dtype': str(obj.dtype),
            'shape': obj.shape
        }

    def get_pandas_df(self, obj):
        index = [x for x in obj.index.names if x is not None]
        if index:
            obj = obj.reset_index()
        return {
            'py/object': self.get_obj_addr(obj),
            'values': obj.values.tolist(),
            "columns": obj.columns.tolist(),
            'index': index
        }

    def get_pandas_series(self, obj):
        return {
            'py/object': self.get_obj_addr(obj),
            'values': obj.values.tolist(),
            'index': obj.index.tolist(),
            'name': obj.name,
            'dtype': str(obj.dtype)
        }

    def get_function(self, obj):
        return {
            'py/function': self.get_obj_addr(obj),
            '__src__': self.get_function_str(obj)
        }

    def get_object(self, obj):
        d = obj.to_dict() if hasattr(obj, 'to_dict') else obj.__dict__
        return {
            'py/object': self.get_obj_addr(obj), **self.get(d)
        }

class Deserializer(SerializerBase):
    def get(self, obj):
        if not isinstance(obj, dict):
            return obj
        
        if 'py/object' in obj:
            return self.get_object(obj)
        
        if 'py/function' in obj:
            return self.get_function(obj)
        
        return {self.get(k): self.get(v) for k, v in obj.items()}

    def get_object(self, obj):
        class_path = obj['py/object']
        obj_data = {k: self.get(v) for k, v in obj.items() if k != 'py/object'}
        
        if 'py/function' in obj_data:
            return self.get_function(obj_data)
        
        if self.instantiate:
            return self.instantiate_object(class_path, obj_data)
        return obj_data

    def instantiate_object(self, class_path, obj_data):
        module_name, class_name = class_path.rsplit('.', 1)
        module = importlib.import_module(module_name)
        cls_obj = getattr(module, class_name)
        
        if isinstance(cls_obj, functools.partial):
            return cls_obj
        
        if callable(cls_obj):
            while hasattr(cls_obj, '__wrapped__'):
                cls_obj = cls_obj.__wrapped__
        
        if cls_obj is str:
            return str(next(iter(obj_data.values())))
        
        if hasattr(cls_obj, 'from_dict'):
            return cls_obj.from_dict(obj_data)
        
        if cls_obj == typing._GenericAlias:
            return self.reconstruct_generic_alias(obj_data)
        
        try:
            return cls_obj(**obj_data)
        except TypeError:
            if issubclass(cls_obj, (str, int, float, bool)):
                return cls_obj(next(iter(obj_data.values())))
            instance = object.__new__(cls_obj)
            for key, value in obj_data.items():
                if not isinstance(value, dict) or 'py/unknown' not in value:
                    setattr(instance, key, value)
            return instance

    def reconstruct_generic_alias(self, obj_data):
        origin = self.get(obj_data.get('__origin__'))
        args = tuple(self.get(arg) for arg in obj_data.get('__args__', []))
        return typing._GenericAlias(origin, args)

    def get_function(self, obj):
        func_path = obj['py/function']
        try:
            module_name, func_name = func_path.rsplit('.', 1)
            module = importlib.import_module(module_name)
            func = getattr(module, func_name)
            
            while hasattr(func, '__wrapped__'):
                func = func.__wrapped__
            
            return func
        except Exception as e:
            if '__src__' in obj:
                try:
                    exec(obj['__src__'], globals())
                    func = globals()[func_name]
                    
                    while hasattr(func, '__wrapped__'):
                        func = func.__wrapped__
                    
                    return func
                except Exception as e:
                    logger.error(f"Error recreating function from source: {e}")
            logger.error(f"Error getting function {func_path}: {e}")
            return obj

    def get_pandas_df(self, obj):
        import pandas as pd
        values = obj['values']
        columns = obj['columns']
        index = obj['index']
        df = pd.DataFrame(values, columns=columns)
        if index:
            df = df.set_index(index)
        return df

    def get_pandas_series(self, obj):
        import pandas as pd
        values = obj['values']
        index = obj['index']
        name = obj['name']
        dtype = obj['dtype']
        return pd.Series(values, index=index, name=name, dtype=dtype)

    def get_numpy(self, obj):
        import numpy as np
        values = obj['values']
        dtype = obj['dtype']
        shape = obj['shape']
        return np.array(values, dtype=dtype).reshape(shape)