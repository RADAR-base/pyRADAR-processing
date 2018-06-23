import os
import glob
import dask.bytes.core
from dask.bytes.local import LocalFileSystem as daskLFC

class LocalFileSystem(daskLFC):
    def listdir_conditional(path, conditional):
        """
        Function that returns a list of paths relative to the input path if the
        conditional function called on the full path evaluates to True
        """
        pathstr = '' if path is None else path
        return [f for f in os.listdir(path) if
                conditional(os.path.join(pathstr, f))]

    def listfolders(path=None):
        """
        Returns a list of relative paths to folders within the given path. If path
        is not given, uses the current working directory.
        """
        return listdir_conditional(path, os.path.isdir)

    def listfiles(path=None):
        """
        Returns a list of relative paths to files within the given path. If path
        is not given, uses the current working directory.
        """
        return listdir_conditional(path, os.path.isfile)

dask.bytes.core._filesystems['local'] = LocalFileSystem
