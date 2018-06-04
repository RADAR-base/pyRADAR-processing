#!/usr/bin/env python3
import os
import glob
from . import visualise
from .common import AttrRecDict, progress_bar
from .io.generic import ProjectIO, ParticipantIO
from .io.fs import open_project_folder as fs_project
from .io.hdf5 import open_project_file as h5_project
from .util.specifications import ProjectSpecs
from .util.avro import ProjectSchemas

default_schemas = ProjectSchemas()
default_specs = ProjectSpecs()

class RadarWrapper():
    def _get_attr_or_parents(self, attr):
        if hasattr(self, attr):
            if getattr(self, attr) is not None:
                return getattr(self, attr)
        if getattr(self, parent) is None:
            return None
        else:
            return self.parent._get_attr_or_parents(attr)

    def _get_path(self):
        """ Returns a path to the participant relative to the project root,
        based on subproject names

        Output
        ______
        path: str
            A string of the relative path.
        """
        parent = self.parent
        if parent is None:
            return '/'
        path = '/' + self.name
        while parent.parent is not None:
            path = '/' + parent.name + path
            parent = parent.parent
        return path


class Project(RadarWrapper):
    """
    """
    def __init__(self, proj_data=None, **kwargs):
        self.name = kwargs['name'] if 'name' in kwargs else proj_data.name
        self.parent = kwargs['parent'] if 'parent' in kwargs else None
        self._data = [proj_data] if proj_data is not None else []
        self.subprojects = AttrRecDict()
        self.participants = self.parent.participants[self.name] if self.parent\
                        else AttrRecDict()
        self.schemas = kwargs['schemas'] if 'schemas' in kwargs\
                                         else default_schemas
        self.specifications = kwargs['specifications'] if 'specifications'\
                                          in kwargs else default_schemas
        if self._data:
            self._get_subprojects(self._data[0].subprojects)
            self._get_participants(self._data[0].participants)

    def __getitem__(self, key):
        if key in self.subprojects:
            return self.subprojects[key]
        elif hasattr(self.participants, key):
            return getattr(self.participants, key)
        else:
            raise KeyError('No such subproject or participant: {}'.format(key))

    def load_h5(self, h5_path, *args, **kwargs):
        data = h5_project(h5_path, *args, **kwargs).data
        self._get_subprojects(data.subprojects)
        self._get_participants(data.participants)
        self._data.append(data)

    def load_fs(self, folder_path, *args, **kwargs):
        data = fs_project(folder_path, *args, **kwargs)
        self._get_subprojects(data.subprojects)
        self._get_participants(data.participants)
        self._data.append(data)

    def to_h5(self, h5_path, *args, **kwargs):
        raise NotImplementedError

    def to_csvs(self, folder_path, *args, **kwargs):
        raise NotImplementedError

    def save_project(self, output=None, *args, **kwargs):
        if output is None:
            output = self._get_attr_or_parents('output')
        for sp in self.subprojects:
            output.create_subproject(name=sp.name, where=self._get_path())
            sp.save_project(output=output, *args, **kwargs)
        for ptc in self.participants:
            ptc.save_all(output=output)

    def _get_subprojects(self, subproject_data_dict):
        for sp_name, sp_data in subproject_data_dict.items():
            self.add_subproject(sp_name, data=sp_data)

    def _get_participants(self, participant_data_dict):
        for ptc_name, ptc_data in participant_data_dict.items():
            if not isinstance(ptc_data, AttrRecDict):
                if not isinstance(ptc_data, dict):
                    ptc_data = \
                        ptc_data.get_data_dict(specifications=default_specs,
                                               schemas=default_schemas)
                self.add_participant(ptc_name, data=ptc_data)

    def add_participant(self, name, where='.', data=None, info=None):
        proj = self if where == '.' else self.subprojects[where]
        proj.participants[name] = Participant(data=data, name=name, info=info,
                                              parent=self)
        return proj.participants[name]

    def add_subproject(self, name, where='.', data=None):
        proj = self if where == '.' else self.subprojects[where]
        proj.participants[name] = AttrRecDict()
        proj.subprojects[name] = Project(data, name=name, parent=self)
        return proj.subprojects[name]

    def __str__(self):
        return 'RADAR {}: {}, {} participants'.format(
            'Subproject' if getattr(self, 'parent') is None else 'Project',
            self.name,
            len(self.participants))

    def __repr__(self):
        info_string = '''member of {class}
        Name: {name}
        Subprojects: {subprojs}
        No. participants: {num_partic}
        '''
        format_kwargs = {'class': type(self),
                         'name': self.name,
                         'subprojs': ', '.join(self.subprojects.keys()) or 'None',
                         'num_partic': len(self.participants),
                        }
        return info_string.format(**format_kwargs)

    def map_participants(self, func, *args, **kwargs):
        def ptc_func(ptc, *args, **kwargs):
            return func(ptc, *args, **kwargs)
        return map(ptc_func, list(self.participants))


class Participant(RadarWrapper):
    """ A class to hold data and methods concerning participants/subjects in a
    RADAR trial. Typically intialised by opening a Project.
    """
    def __init__(self, data=None, info=None, **kwargs):
        self.data = data if data is not None else {}
        self.info = info if info is not None else {}
        self.name = kwargs['name'] if 'name' in kwargs else data.name
        self.parent = kwargs['parent'] if 'parent' in kwargs else None
        self.labels = kwargs['labels'] if 'labels' in kwargs else None

    def __repr__(self):
        return "Participant {}. of type {}".format(self.name, type(self))


    def save_all(self, modalities=None, output=None, *args, **kwargs):
        """ Saves all of the participants data to output.
        Parameters
        __________
        modalities: list
            A list of the modalities to save. If None, all will be saved.
        output:
            A radar data handle. If None, uses the default output for the
            participant (or parent, etc)
        """
        print("Saving {}...".format(self.name))
        if modalities is None:
            modalities = list(self.data)
        for mod in modalities:
            print('...{}...'.format(mod))
            self.save_data(data=self.data[mod], name=mod, output=output,
                           *args, **kwargs)

    def save_data(self, data, name, output=None, *args, **kwargs):
        """ Saves a data object to an output.
        Parameters
        __________
        data: DataFrame / RecArray / RadarTable
            Data object to save
        name: str
            Name under which to save the output
        output:
            A radar data handle. If no output is specified, uses
            the default output for itself/its parent.
        """
        if output is None:
            output = self._get_attr_or_parents('output')
        if not isinstance(output, ParticipantIO):
            output = output.create_participant(where=self.parent._get_path(),
                                               name=self.name)
        output.create_table(name=name, obj=data, *args, **kwargs)


def from_h5(h5_path, name=None, *args, **kwargs):
    proj = Project(name=name if name else h5_path)
    proj.load_h5(h5_path, *args, **kwargs)
    return proj

def from_fs(folder_path, name=None, *args, **kwargs):
    proj = Project(name=name if name else folder_path)
    proj.load_fs(folder_path, *args, **kwargs)
    return proj
