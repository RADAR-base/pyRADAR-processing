import collections

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


class AttrRecDict(RecursiveDict):
    """ A dictionary, based on RecursiveDict, that allows recurisve access to
    nested dictionary items using object attributes.
    Only the final level / leaf dictionaries will have attribute accessible
    items. Primarily used to access subproject participants from a higher level
    project.
    Example:
        d = AttrRecDict((('inner', {'innerkey':'innerval'}),))
        d.inner raises an AttributeError
        d.innerkey returns 'innerval'
        d['inner'] returns {'inner': 'innerkey'}
        d['inner/innerkey'] returns 'innerval
    """
    def __getattr__(self, name):
        if name not in [x for x in self._get_keys()]:
            raise AttributeError(
                "No such attribute '{}' in '{}'".format(name, self))
        kv = self._get_items()
        val = [x[1] for x in kv if x[0] == name] or None
        if val is None:
            raise AttributeError(
                "No such attribute '{}' in '{}'".format(name, self))
        elif len(val) > 1:
            raise ValueError(
                'Multiple participants with the same ID: {}'.format(name))
        return val[0]

    def __repr__(self):
        string = super(AttrRecDict, self).__repr__()
        string = string.split('\n')
        string[0] = 'Attribute recursive dict with'
        string = '\n'.join(string)
        return string

    def __dir__(self):
        return self.keys()


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


def update(d, u):
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            d[k] = update(d.get(k, {}), v)
        else:
            d[k] = v
    return d
