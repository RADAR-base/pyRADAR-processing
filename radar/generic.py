import re
import collections
from functools import lru_cache, singledispatch, update_wrapper


class Registry(dict):
    def register(self, name, val=None):
        def wrapper(func):
            self[name] = func
        if val is None:
            return wrapper
        else:
            self[name] = val


def update(d, u):
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            d[k] = update(d.get(k, {}), v)
        else:
            d[k] = v
    return d


def methdispatch(func):
    def wrapper(*args, **kw):
        return dispatcher.dispatch(args[1].__class__)(*args, **kw)
    dispatcher = singledispatch(func)
    wrapper.register = dispatcher.register
    update_wrapper(wrapper, func)
    return wrapper


@lru_cache(8)
def re_compile(pattern):
    return re.compile(pattern)
