from .serialize import *

@debug
def deserialize(data, method=DEFAULT_SERIALIZER):
    # logger.debug(f"Deserializing object: {obj}")
    obj = None
    if method == JSONPICKLE_SERIALIZER:
        obj = _deserialize_jsonpickle(data)
    
    if obj is None:
        if type(data) is bytes:
            data = data.decode("utf-8")
        if type(data) is str:
            data = json.loads(data)
        obj = _deserialize(data)

    return obj


# @debug
def _deserialize(data):
    # logger.debug(f"Deserializing data: {data}")
    if isinstance(data, list):
        return [_deserialize(v) for v in data]
    elif isinstance(data, dict):
        if OBJ_ADDR_KEY in data:
            return _deserialize_python(data)
        else:
            return {k: _deserialize(v) for k, v in data.items()}
    elif is_jsonable(data):
        return data
    else:
        logger.warning(f"cannot deserialize data ({type(data)}): {data}")
        return data



def _deserialize_jsonpickle(obj):
    try:
        import jsonpickle
        return jsonpickle.loads(obj)
    except Exception as e:
        logger.warning(e)
        return None

#@debug
def _deserialize_python(data: dict, init_funcs=["from_dict"]):
    if not OBJ_ADDR_KEY in data:
        return

    obj_addr = data.get(OBJ_ADDR_KEY)
    args = _deserialize(data.get(OBJ_ARGS_KEY, []))
    kwargs = _deserialize(data.get(OBJ_KWARGS_KEY, {}))

    from .custom import CUSTOM_OBJECT_DESERIALIZERS

    if obj_addr in CUSTOM_OBJECT_DESERIALIZERS:
        obj_func = CUSTOM_OBJECT_DESERIALIZERS[obj_addr]
        x = obj_func(*args, **kwargs)
        return x

    def call_init():
        return call_function_politely(obj, *args, **kwargs)

    def call_init_func(func_name, args, kwargs):
        if hasattr(obj, func_name):
            func = getattr(obj, func_name)
            if not args and kwargs:
                args = (kwargs,)
                kwargs = {}
            return call_function_politely(func, *args, **kwargs)

    obj = flexible_import(obj_addr)
    if obj is None and OBJ_SRC_KEY in data:
        src = data[OBJ_SRC_KEY]
        if src.startswith("class "):
            return deserialize_class(data)
        else:
            return deserialize_function(src, obj_addr)

    if args or kwargs:
        for initfunc in init_funcs:
            res = call_init_func(initfunc, args, kwargs)
            if res is not None:
                return res
        return call_init()

    return obj


#@debug
def deserialize_class(data):
    # logger.debug(f"Deserializing class: {data[OBJ_ADDR_KEY]}")
    if OBJ_SRC_KEY not in data:
        raise ValueError(
            f"Cannot deserialize class {data[OBJ_ADDR_KEY]}: no source code provided"
        )

    class_source = data[OBJ_SRC_KEY]
    class_name = data[OBJ_ADDR_KEY].split(".")[-1]
    module_name = data[OBJ_ADDR_KEY].rsplit(".", 1)[0]

    # Create a new module to hold the class
    mod = types.ModuleType(module_name)

    # Use a copy of the current globals() and update it with the module's dict
    exec_globals = globals().copy()
    exec_globals.update(mod.__dict__)

    # Execute the class source in the prepared global namespace
    exec(class_source, exec_globals)

    # Get the class object from the executed globals
    cls = exec_globals[class_name]

    # Set the correct module for the class
    cls.__module__ = module_name

    return cls



#@debug
def deserialize_function(src, func_path):
    logger.debug(f"Recreating function from source: {func_path}")
    try:
        exec(src, globals())
        func_name = func_path.split(".")[-1]
        func = globals()[func_name]

        while hasattr(func, "__wrapped__"):
            func = func.__wrapped__

        return func
    except Exception as e:
        logger.error(f"Error recreating function from source: {e}")
        return None
