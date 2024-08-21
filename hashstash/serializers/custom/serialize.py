from . import *

def serialize_custom(obj):
    return serialize_json(_serialize(obj))

# @log.debug
def _serialize(obj):
    # log.debug(f"_serialize called with object type {type(obj)}")

    # first get any wrapped's
    while hasattr(obj, "__wrapped__") and obj.__wrapped__:
        obj = obj.__wrapped__

    # if isinstance(obj, (list, tuple, set, frozenset)):
    if type(obj) is list:
        log.debug("Serializing list")
        return [_serialize(item) for item in obj]

    elif type(obj) is dict:
        log.debug("Serializing dictionary")
        return {k: _serialize(v) for k, v in obj.items()}

    # (dict, list, str, int, float, bool, type(None)))
    elif is_jsonable(obj):
        # log.debug("Returning as jsonable")
        return obj

    elif isinstance(obj, types.FunctionType):
        log.debug("Serializing function")
        return serialize_function(obj)

    elif isinstance(obj, type):
        log.debug("Serializing class")
        return serialize_class(obj)

    log.debug("Serializing object")
    return serialize_object(obj)






@log.debug
def serialize_object(obj):
    log.debug(f"serialize_object called with type: {type(obj)}")

    # is dict itself? continue
    if type(obj) is dict:
        return {k: _serialize(v) for k, v in obj.items()}

    # otherwise make outd
    obj_addr = get_obj_addr(obj)
    # logger.info(f"Serializing object with address: {obj_addr}")

    # object registered in custom serializer functions?
    
    from .custom import CUSTOM_OBJECT_SERIALIZERS
    if obj_addr in CUSTOM_OBJECT_SERIALIZERS:
        custom_serialize_func = CUSTOM_OBJECT_SERIALIZERS[obj_addr]
        res = custom_serialize_func(obj)
        if type(res) is dict:
            if not OBJ_ARGS_KEY in res and not OBJ_KWARGS_KEY in res:
                inpd = {OBJ_KWARGS_KEY: res}
            else:
                inpd={**res}
        else:
            if not (
                isinstance(res, list)
                and all(isinstance(item, (list, tuple)) for item in res)
            ):
                res = [res]
            inpd = {OBJ_ARGS_KEY: res}

        return {OBJ_ADDR_KEY: obj_addr, **inpd}
        # else:
        # return {f"__py/{get_obj_name(obj)}":res}

    # any dictionary at all?
    obj_d = prune_none_values(get_dict(obj, __ignore=False))
    if obj_d:
        out = {
            OBJ_ADDR_KEY: obj_addr,
            OBJ_KWARGS_KEY: {k: _serialize(v) for k, v in obj_d.items()},
        }
        if not can_import_object(obj):
            out[OBJ_SRC_KEY] = reconstruct_class_source(obj.__class__)

        return out

    # otherwise unknown
    return serialize_unknown(obj)


@log.debug
def serialize_unknown(obj, pickle=False):
    logging.warning(f"Cannot serialize unknown object type: {type(obj)}")
    return None
    obj_addr = get_obj_addr(obj)
    # if obj_addr.startswith('builtins.'): return obj
    outd = {OBJ_ADDR_KEY: obj_addr}  # , "__pytype__": "unknown"}
    if pickle:
        pkl = serialize_pickle(obj)
        if pkl:
            outd["__pkl__"] = pkl
    return outd


@log.debug
def serialize_pickle(obj):
    # logger.info(f"Attempting to pickle object: {type(obj)}")
    try:
        print("pickling", obj)
        out = base64.b64encode(pickle.dumps(obj))
        print("pickled", out)
        return out
    except Exception as e:
        logging.error(f"Failed to pickle object: {e}")


@log.debug
def serialize_function(obj):
    log.debug(f"Serializing function: {obj.__name__}")
    obj_addr = get_obj_addr(obj)
    out = {OBJ_ADDR_KEY: obj_addr}

    # Only serialize OBJ_SRC_KEY if the function is in   or can't be imported
    if obj.__module__ == "__main__" or not can_import_object(obj):
        src = get_function_str(obj)
        if src:
            out[OBJ_SRC_KEY] = src

    return out


@log.debug
def get_dict(obj, __ignore=True, _ignore=False, use_attrs=False):
    log.debug(f"get_dict called for object type: {type(obj)}")
    if isinstance(obj, dict):
        return {**obj}

    try:
        obj = dict(obj.to_dict())
        log.debug('using to_dict')
        return obj
    except Exception:
        pass

    try:
        obj = {**obj.__dict__}
        log.debug('using obj.__dict__')
        return obj
    except Exception:
        pass

    try:
        obj = dict(obj)
        log.debug('using dict(obj)')
        return obj
    except Exception:
        pass

    if use_attrs:
        log.debug('using dir(obj)')
        init_params = inspect.signature(obj.__class__.__init__).parameters.keys()
        return {
            attr: getattr(obj, attr)
            for attr in dir(obj)
            if attr in init_params
            and (not _ignore or not attr.startswith("_"))
            and (not __ignore or not attr.startswith("__"))
            and getattr(obj, attr) is not None
        }



@log.debug
def serialize_class(obj):
    log.debug(f"Serializing class: {obj.__name__}")
    out = {
        OBJ_ADDR_KEY: get_obj_addr(obj),
    }

    # Check if the class is defined in __main__ or can't be imported
    if obj.__module__ == '__main__' or not can_import_object(obj):
        try:
            out[OBJ_SRC_KEY] = reformat_python_source(inspect.getsource(obj))
        except OSError:
            logger.warning(f"Could not get source for {obj.__name__} using inspect. Attempting to reconstruct.")
            try:
                out[OBJ_SRC_KEY] = reconstruct_class_source(obj)
            except Exception as e:
                logger.error(f"Failed to reconstruct source for {obj.__name__}: {e}")
        
        # Include class attributes only for classes that need full serialization
        for name, value in obj.__dict__.items():
            if name.startswith('__') and name.endswith('__'):
                continue
            if callable(value):
                out[name] = serialize_function(value)
            else:
                out[name] = serialize(value)

    return out

@log.info
def reconstruct_class_source(cls):
    lines = [f"class {cls.__name__}:"]
    
    # Add class attributes
    for name, value in cls.__dict__.items():
        if name.startswith('__') and name.endswith('__'):
            continue
        if not inspect.isfunction(value):
            lines.append(f"    {name} = {repr(value)}")
    
    # Add an empty line if there were class attributes
    if len(lines) > 1:
        lines.append("")
    
    # Add methods
    for name, value in cls.__dict__.items():
        if inspect.isfunction(value):
            try:
                func_source = get_function_str(value)
                # Remove any leading whitespace and add proper indentation
                func_lines = func_source.split('\n')
                func_lines = ["    " + line if line.strip() else line for line in func_lines]
                lines.extend(func_lines)
                lines.append("")  # Add an empty line after each method
            except OSError:
                logger.warning(f"Could not get source for method {name}")
    
    src = "\n".join(lines)
    out = reformat_python_source(src)
    print("reconstructed")
    print(out)
    print()

    return out