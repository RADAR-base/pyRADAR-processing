""" FileSystem classes to provide a unified way to interact with
local and remote files. Currently only LocalFileSystem.
"""
import os
from functools import lru_cache
from dask.utils import import_required
from dask.bytes.local import LocalFileSystem as daskLFC


class LocalFileSystem(daskLFC):
    """ An extended version of dask's LocalFileSystem
    See: dask.bytes.local.LocalFileSystem
    """
    def listdir_conditional(self, path=None, conditional=lambda: True):
        """
        Function that returns a list of paths relative to the input path if the
        conditional function called on the full path evaluates to True
        Params:
            path (str): A folder
            conditional (Callable): A function to test the input path on
        Returns:
            List[str]: List of items in a folder than meet the condition
        """
        path = path if path is not None else self.cwd
        return [f for f in os.listdir(path) if
                conditional(os.path.join(path, f))]

    def list_folders(self, path=None):
        """
        Returns a list of relative paths to folders within the given path.
        If path is not given, uses the current working directory.
        Params:
            path (str, optional): Directory to list folders in
        Returns:
            List[str]: List of subdirectories
        """
        return self.listdir_conditional(path, os.path.isdir)

    def list_files(self, path=None):
        """
        Returns a list of relative paths to files within the given path. If
        path is not given, uses the current working directory.
        Params:
            path (str, optional): Folder path to list files in
        Returns:
            List[str]: All files within a folder
        """
        return self.listdir_conditional(path, self.isfile)

    @staticmethod
    def isfile(path):
        """
        Params:
            path (str): Path to check whether is a file
        Returns:
            bool
        """
        return os.path.isfile(path)

    @staticmethod
    def isdir(path):
        """
        Params:
            path (str): Path to check whether is a directory/folder
        Returns:
            bool
        """
        return os.path.isdir(path)

    @staticmethod
    def getctime(path):
        """
        Params:
            path (str): Path to get ctime
        Returns:
            bool
        """
        return os.path.getctime(path)


_filesystems = {'file': LocalFileSystem}


@lru_cache(maxsize=2)
def get_fs(protocol, **storage_options):
    """ Creates a filesystem object
    Essentially the same as in dask.bytes.core, but we need a few extra
    functions on the filesystem objects.

    Parameters
    __________
    protocol : str
    storage_options : dict (optional)

    Returns
    _________
    fs : FileSystem
    fs_token : str (?)"""

    if protocol != 'file':
        raise NotImplementedError('Only local filesystems are supported')

    if protocol in _filesystems:
        cls = _filesystems[protocol]

    elif protocol == 's3':
        import_required('s3fs',
                        '"s3fs" is required for the s3 protocol\n"'
                        '    pip install s3fs')
        cls = _filesystems[protocol]
        raise NotImplementedError('s3 protocol is not yet implemented')

    else:
        raise ValueError('Unsupported protocol "{}"'.format(protocol))

    if storage_options is None:
        storage_options = {}

    fs = cls(**storage_options)
    return fs
