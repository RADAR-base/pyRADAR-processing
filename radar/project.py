#!/usr/bin/env python3
import os
import glob
from . import visualise
from .common import AttrRecDict, progress_bar
from .io.fs import open_project_folder as fs_project
from .io.hdf5 import open_project_file as h5_project
from .util.specifications import ProjectSpecs
from .util.avro import ProjectSchemas

class Project():
    """
    """
    def __init__(self, proj_data=None, **kwargs):
        self.name = kwargs['name'] if 'name' in kwargs else proj_data.name
        self.parent = kwargs['parent'] if 'parent' in kwargs else None
        self._data = [proj_data] if proj_data is not None else []
        self.subprojects = AttrRecDict()
        self.participants = self.parent.participants[self.name] if self.parent\
                        else AttrRecDict()
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

    def _get_subprojects(self, subproject_data_dict):
        for sp_name, sp_data in subproject_data_dict.items():
            self.add_subproject(sp_name, data=sp_data)

    def _get_participants(self, participant_data_dict):
        for ptc_name, ptc_data in participant_data_dict.items():
            if not isinstance(ptc_data, AttrRecDict):
                if not isinstance(ptc_data, dict):
                    ptc_data = ptc_data.get_data_dict()
                self.add_participant(ptc_name, data=ptc_data)

    def add_participant(self, name, where='.', data=None, info=None):
        proj = self if where == '.' else self.subprojects[where]
        proj.participants[name] = Participant(data=data, name=name, info=info)
        return proj.participants[name]

    def add_subproject(self, name, where='.', data=None):
        proj = self if where == '.' else self.subprojects[where]
        proj.participants[name] = AttrRecDict()
        proj.subprojects[name] = Project(data, name=name, parent=self)
        return proj.subprojects[name]

    def __str__(self):
        return 'RADAR {}: {}, {} participants'.format(
            'Project' if hasattr(self, '_hdf_file') else 'Subproject',
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


class Participant():
    """ A class to hold data and methods concerning participants/subjects in a
    RADAR trial. Typically intialised by opening a Project.
    """
    def __init__(self, data=None, info=None, **kwargs):
        self.data = data if data is not None else {}
        self.info = info if info is not None else {}
        self.name = kwargs['name'] if 'name' in kwargs else data.name
        self.parent = kwargs['parent'] if 'parent' in kwargs else None

    def __repr__(self):
        return "Participant {}. of type {}".format(self.name, type(self))

    def plot_time_span(self, source, timespan, ycols,
                       xcol='value.time', fig=None, events=None):
        print('To be deprecated')
        df = self.df_from_data(source, ycols.append(xcol))
        df.set_index(xcol)
        fig = visualise.interactive.time_span(
            df, ycols, timespan, fig=fig)
        if events != None:
            visualise.interactive.add_events(fig, events)
        return fig


def from_h5(h5_path, name=None, *args, **kwargs):
    proj = Project(name=name if name else h5_path)
    proj.load_h5(h5_path, *args, **kwargs)
    return proj

def from_fs(folder_path, name=None, *args, **kwargs):
    proj = Project(name=name if name else folder_path)
    proj.load_fs(folder_path, *args, **kwargs)
    return proj
