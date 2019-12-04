import re
import collections
from functools import lru_cache, singledispatch, update_wrapper


class RecursiveDict(dict):
    """ A dictionary that can directly access items from nested dictionaries
    using the '/' character.
    Example:
        d = RecursiveDict((('inner', {'innerkey':'innerval'}),))
        d['inner/innerkey'] returns 'innerval'
    """
    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)

    def __repr__(self):
        string = 'Recursive dict with\nTop level keys:\n{}\nLeaf keys:\n{}'.\
                format(', '.join((k for k in self.keys())),
                       ', '.join((k for k in self._get_keys())))
        return string

    def __getitem__(self, key):
        if key == '/':
            return self
        key_split = key.split('/')
        key = key_split.pop(0)
        if key == '':
            return KeyError('')
        if key_split:
            return dict.__getitem__(self, key).__getitem__('/'.join(key_split))
        else:
            return dict.__getitem__(self, key)

    def __setitem__(self, key, value):
        key_split = key.split('/')
        key = key_split.pop(0)
        if key == '':
            return KeyError('')
        if key_split:
            self[key].__setitem__(self, key, value)
        else:
            dict.__setitem__(self, key, value)

    def _get_x(self, xattr):
        out = []
        for x, v in zip(getattr(self, xattr)(), self.values()):
            if isinstance(v, RecursiveDict):
                out.extend(v._get_x(xattr))
            elif isinstance(v, dict) & hasattr(v, xattr):
                out.extend(getattr(v, xattr)())
            else:
                out.append(x)
        return out

    def _get_items(self):
        return self._get_x('items')

    def _get_values(self):
        return self._get_x('values')

    def _get_keys(self):
        return self._get_x('keys')

    def __iter__(self):
        return iter(self._get_values())

    def __len__(self):
        return len(self._get_keys())


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
