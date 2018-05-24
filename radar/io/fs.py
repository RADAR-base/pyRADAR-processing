import os
import glob
from collections import Counter
from ..common import AttrRecDict
from .csv import CsvTable
from .generic import ProjectIO, ParticipantIO, ParticipantData

"""
File system project IO.
Project and participant objects for getting data in the standard filesystem
folder heirarchy (as apposed to within a hdf5 file).
"""

class ProjectFolder(ProjectIO):
    def __init__(self, path, mode='a', subprojects=None, **kwargs):
        """
        """
        self.path = path if os.path.isabs(path) else os.path.abspath(path)
        self.name = kwargs['name'] if 'name' in kwargs else \
                    os.path.split(self.path)[1]
        self.parent = kwargs.get('parent')
        self.subprojects = AttrRecDict()
        participants = kwargs.get('participants')
        self.participants = AttrRecDict() if participants is None \
                            else participants

        if subprojects is not None:
            self.subprojects.update(self.get_subprojects(subprojects, **kwargs))
        ptckw = kwargs.get('ptckw')
        if ptckw is None:
            ptckw = {}
        self.participants.update(self.get_participants(**ptckw))

    def get_subprojects(self, subproject_names, **kwargs):
        sp = AttrRecDict()
        for path in subproject_names:
            split_path = [f for f in path.split(os.path.sep) if f]
            name = split_path[0]
            sub_subproject = os.path.sep.join(split_path[1:]) if \
                len(split_path) > 1 else None
            if name in sp:
                sp[name].append(sub_subproject)
            else:
                sp[name] = [sub_subproject]
        for name in sp.keys():
            self.participants[name] = AttrRecDict()
            folder_path = os.path.join(self.path, name)
            sub_sp = sp[name] if any(sp[name]) else None
            sp[name] = ProjectFolder(**kwargs,
                                     path=folder_path,
                                     name=name,
                                     subproject_names=sub_sp,
                                     participants=self.participants[name],
                                     parent=self)
        return sp

    def get_participants(self, *args, **kwargs):
        ptcs = AttrRecDict()
        participant_names = [f for f in listfolders(self.path)
                             if f not in self.subprojects]
        paths = [os.path.join(self.path, name) for name in participant_names]
        for i, name in enumerate(participant_names):
            ptcs[name] = ParticipantFolder(folder_path=paths[i], name=name,
                                           *args, **kwargs)
        return ptcs


class ParticipantFolder(ParticipantIO):
    def __init__(self, folder_path, *args, **kwargs):
        self._data_funcs = {
            'csv': self._load_csv,
            'csv.gz': self._load_csvgz,
            'imec': self._load_imec,
            }
        self.path = folder_path
        self.name = kwargs.get('name')
        if self.name is None:
            self.name = os.path.split[folder_path][1]
        self._filetypes = kwargs.get('filetypes')
        if self._filetypes is None:
            self._filetypes = list(self._data_funcs)
        self._subdirs = kwargs.get('subdirs')
        if self._subdirs is None:
            self._subdirs = []
        self.data = self.get_data_dict()
        self.info = {}

    def get_data_dict(self, filetypes=None, subdirs=None, **kwargs):
        """ Returns a dictionary of data modality objects within the participant
        folder.

        Input
        _______
        filetypes : List of str
            A list of filetypes to load. Currently can be csv, csv.gz, imec. All
            by default.
        subdirs : List of str
            A list of subdirectories to look within, when modality data folders
            are stored within a heirarchy.

        Returns
        ________
        data_dict : dict
            A dictionary containing data modality objects
        """
        def get_folders(path, subdirs):
            if subdirs is None:
                subdirs = []
            folders = listfolders(path)
            for sd in subdirs:
                if sd not in folders:
                    continue
                for modal in listfolders(os.path.join(path, sd)):
                    folders.append(os.path.join(sd, modal))

            return [f for f in folders if f not in subdirs]

        def get_modalities(where, names, filetypes, data_funcs, whitelist=None):
            modalities = ParticipantData()
            for name in names:
                if whitelist is not None:
                    if name not in whitelist:
                        continue
                key = os.path.basename(name)
                modalities[key] = data_loader(where, name, data_funcs)
            return modalities

        def data_loader(where, name, data_funcs):
            files = listfiles(os.path.join(where, name))
            if 'schema.json' in files:
                files.remove('schema.json')
            filetype = determine_filetype(files)
            if filetype in data_funcs:
                return data_funcs[filetype](where, name)
            else:
                return None

        def determine_filetype(files):
            def split_filename(f):
                f = os.path.basename(f)
                f_split = f.split(os.path.extsep)
                if len(f_split) > 1:
                    return os.path.extsep.join(f_split[1:])
                else:
                    return ''
            extensions = [split_filename(f) for f in files]
            count = Counter(extensions)
            if len(count) == 0:
                return ''
            return max(extensions, key=count.get)

        if filetypes is None:
            filetypes = self._filetypes
        if subdirs is None:
            subdirs = self._subdirs

        return get_modalities(where=self.path,
                              names=get_folders(self.path, subdirs),
                              filetypes=filetypes,
                              data_funcs=self._data_funcs,
                              whitelist=kwargs.get('whitelist'))

    def _load_csv(self, where, name):
        return CsvTable(where, name)

    def _load_csvgz(self, where, name):
        return CsvTable(where, name, compression='gzip')

    def _load_imec(self, where, name):
        return 0


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

def open_project_folder(folder, *args, **kwargs):
    return ProjectFolder(folder, *args, **kwargs)
