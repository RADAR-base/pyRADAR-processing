#!/usr/bin/env python3
import os
import glob
from . import visualise
from .io import *
from .common import AttrRecDict, progress_bar
from .util.avro import ProjectSchemas
from .util.specifications import ProjectSpecs

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

    def save_project(self, output=None, *args, **kwargs):
        """OLD
        if output is None:
            output = self._get_attr_or_parents('output')
        for sp in self.subprojects:
            output.create_subproject(name=sp.name, where=self._get_path())
            sp.save_project(output=output, *args, **kwargs)
        for ptc in self.participants:
            ptc.save_all(output=output)
            """
        pass

    def _get_subprojects(self, subproject_data_dict):
        """OLD
        for sp_name, sp_data in subproject_data_dict.items():
            self.add_subproject(sp_name, data=sp_data)
        """
        pass

    def _get_participants(self, participant_data_dict):
        """OLD
        for ptc_name, ptc_data in participant_data_dict.items():
            if not isinstance(ptc_data, AttrRecDict):
                if not isinstance(ptc_data, dict):
                    ptc_data = \
                        ptc_data.get_data_dict(specifications=default_specs,
                                               schemas=default_schemas)
                self.add_participant(ptc_name, data=ptc_data)
                """
        pass

    def add_participant(self, name, where='.', data=None, info=None):
        proj = self if where == '.' else self.subprojects[where]
        proj.participants[name] = Participant(folder=folder, name=name, info=info,
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
    def __init__(self, name=None, path=None, info=None, **kwargs):

        if not (name or folder or 'paths' in kwargs):
            raise ValueError(('You must specify a name or else provide'
                              'a path (or paths) to the participant'))

        paths = [path] if path is not None else \
                 kwargs['path'] if 'path' in kwargs else []
        self.name = name if name is not None \
                         else paths[0].split('/')[-1]
        self.parent = kwargs['parent'] if 'parent' in kwargs else None
        self.info = info if info is not None else {}
        self.labels = kwargs['labels'] if 'labels' in kwargs else {}

        datakw = datakw if 'datakw' in kwargs else {}
        self.data = ParticipantData(paths=paths, datakw=datakw)

    def __repr__(self):
        return "Participant {}. of type {}".format(self.name, type(self))

    def export_data(self, data=None, names=None, project_path=None,
                    filetype=None, *args, **kwargs):
        """ Exports a data object to another file or format.
        Parameters
        __________
        data : DataFrame / RecArray / RadarTable
            Data object to save
        name : str
            Name under which to save the output
        path : str
        filetype : str
        """
        output = self._get_attr_or_parents('output')
        given = (name, project_path, filetype)
        generic = (self.name, output['project_path'], output['filetype'])
        name, project_path, filetype = \
                   (x if x is not None else (generic)[i]
                    for i, x in enumerate(given))

        """
        if not isinstance(output, ParticipantIO):
            output = output.create_participant(where=self.parent._get_path(),
                                               name=self.name)
        output.create_table(name=name, obj=data, *args, **kwargs)
        """



class ParticipantData(object):

    def __init__(self, *args, **kwargs):
        print(args)
        print(kwargs)

