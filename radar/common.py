import numpy as np

class RecursiveDict(dict):
    """ A dictionary that can directly access items from nested dictionaries
    using the '/' character.
    Example:
        d = RecursiveDict((('inner', {'innerkey':'innerval'}),))
        d['inner/innerkey'] returns 'innerval'
    """
    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)

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
        repr_string = ('Recursive attribute dictionary: {}\n'
                       'Total items: {}\n'
                       'Top-level keys: {}\n').format(
                           self.__class__,
                           len(self),
                           ', '.join(list(self.keys())))
        return repr_string


def obj_col_names(obj):
    """ Returns a list of column names from a numpy record array or pandas
    dataframe
    """
    if isinstance(obj, np.ndarray):
        return list(obj.dtype.names)
    else:
        return list(obj.columns)

def iter_repeater(x):
    """ Returns a function that will yield an object a given number of times
    Parameters
    _________
    x: object
        The object to be yielded
    Returns
    _______
    repeat: function
        A function that yields 'x' a given number of times (set by the
        function's parameter 'times').
    """
    def repeat(times):
        """ Yields a set object a number of times
        Parameters
        _________
        times: int
            The number of times to yield the object
        Yields
        ______
        x: object
            An object defined at the functions creation time.
        See Also
        ________
        radar.common.iter_repeater
        """
        i = 0
        while i < times:
            i += 1
            yield x
    return repeat

def progress_bar(progress, total, prefix='Progress: ', suffix='', length=50):
    """ Prints a progress bar on the terminal line, which can be
    updated/overwritten with subsequent calls
    Parameters
    _________
    progress: int or float
        A number corresponding to the current progress
    total: int or float
        The number at which the task will be 100% complete.
        I.e. 100 * progress / total should give the percentage complete.
    prefix: str (optional)
        A string to print preceeding the bar
    suffix: str (optional)
        A string to print after the bar
    length: int (optional)
        The character length of the bar
    """
    completed = int((progress/total)*length)
    bar = "{pre} |{comp}{empty}| {suff}".format(
        pre=prefix,
        comp='█' * completed,
        empty='-' * (length - completed),
        suff=suffix)
    print(bar, end='\r')
    if progress >= total:
        print('')

def date_parser(timestamp):
    return pd.Timestamp(float(timestamp))


