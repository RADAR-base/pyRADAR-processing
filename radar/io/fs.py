import os
import glob
from dask.bytes.local import LocalFileSystem as daskLFC

class LocalFileSystem(daskLFC):
    def listdir_conditional(self, path=None, conditional=lambda:True):
        """
        Function that returns a list of paths relative to the input path if the
        conditional function called on the full path evaluates to True
        """
        path = path if path is not None else self.cwd
        return [f for f in os.listdir(path) if
                conditional(os.path.join(path, f))]

    def list_folders(self, path=None):
        """
        Returns a list of relative paths to folders within the given path. If path
        is not given, uses the current working directory.
        """
        return self.listdir_conditional(path, os.path.isdir)

    def list_files(self, path=None):
        """
        Returns a list of relative paths to files within the given path. If path
        is not given, uses the current working directory.
        """
        return self.listdir_conditional(path, self.isfile)

    def isfile(self, path):
        return os.path.isfile(path)

    def isdir(self, path):
        return os.path.isdir(path)
