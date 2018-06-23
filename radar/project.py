#!/usr/bin/env python3
from .io import *
from .common import abs_path
from .generic import AttrRecDict


class RadarObject():
    def _get_attr_or_parents(self, attr):
        if hasattr(self, attr) and getattr(self, attr) is not None:
            res = getattr(self, attr)
        elif getattr(self, parent) is None:
            res = None
        else:
            res =  self._parent._get_attr_or_parents(attr)

        return res

    def _get_path(self):
        """ Returns a path to the RadarObject relative to the project root,
        based on subproject names

        Output
        ______
        path: str
            A string of the relative path.
        """
        parent = self._parent
        if parent is None:
            return '/'
        path = '/' + self.name
        while parent._parent is not None:
            path = '/' + parent.name + path
            parent = parent._parent
        return path

    def _norm_paths(self, paths):
        paths = [paths] if isinstance(paths, str) else \
                paths if isinstance(paths ,list) else []
        return [abs_path(p) for p in paths]

    def _p_schemas(self):
        return self._get_attr_or_parents('schemas')

    def _p_specifications(self):
        return self._get_attr_or_parents('specifications')

    def _p_armt_definitions(self):
        return self._get_attr_or_parents('armt_definitions')

    def _p_armt_protocols(self):
        return self._get_attr_or_parents('armt_protocols')


class Project(RadarObject):
    """
    """
    def __init__(self, paths=None, **kwargs):
        self.name = kwargs['name'] if 'name' in kwargs else ''
        self._paths = self._norm_paths(paths)
        self._parent = kwargs['parent'] if 'parent' in kwargs else None
        self.subprojects = AttrRecDict()
        self.participants = self._parent.participants[self.name] if self.parent\
                            else AttrRecDict()
        self.schemas = kwargs['schemas'] if 'schemas' in kwargs else \
                       default_schemas
        self.specifications = kwargs['specifications'] if 'specifications'\
                              in kwargs else default_schemas

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
        proj.participants[name] = Participant(folder=folder, name=name,
                                              info=info, parent=self)
        return proj.participants[name]

    def add_subproject(self, name, where='.', paths=None):
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


class Participant(RadarObject):
    """ A class to hold data and methods concerning participants/subjects in a
    RADAR trial. Typically intialised by opening a Project.
    """
    def __init__(self, name=None, path=None, info=None, **kwargs):

        if not (name or folder or 'paths' in kwargs):
            raise ValueError(('You must specify a name or else provide'
                              'a path (or paths) to the participant'))

        self._paths = self._norm_paths(paths)
        self.name = name if name is not None \
                         else paths[0].split('/')[-1]
        self._parent = kwargs['parent'] if 'parent' in kwargs else None
        self.info = info if info is not None else {}
        self.labels = kwargs['labels'] if 'labels' in kwargs else {}

        datakw = datakw if 'datakw' in kwargs else {}
        self.data = ParticipantData(self, paths=paths, **datakw)

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
            output = output.create_participant(where=self._parent._get_path(),
                                               name=self.name)
        output.create_table(name=name, obj=data, *args, **kwargs)
        """


class ParticipantData(RadarObject):

    def __init__(self, participant, *args, **kwargs):
        """
        Parameters
        ___________
        participant : radar.Participant
        data : dict
            Dict storing data names as key and the data in a dask object as the
            value. Strings are always assumed to be paths.
        paths : str or list of str
        """
        data = kwargs.get('data')
        self._data = data.copy() if data is not None else {}
        self._parent = participant
        paths = self._norm_paths(kwargs.get('paths'))
        for path in paths:
            self._search_path(path)

    def _search_path(self, path, replace=False, **kwargs):
        modals = io.search_path_for_data(path, **kwargs)
        for k, v in modals.items():
            if replace or (k not in self._data):
                self._data[k] = modals[k]

    def _load(self, name, path, **kwargs):
        self._data[name] = io.load_data_path(path, **kwargs)

    def save(self, outpath, outfmt, data_names=None):
        print('no')

    def __getitem__(self, key):
        val = self._data[key]
        if isinstance(val, str):
            self._load(key, val)
        return self._data[key]

    def __setitem__(self, key, value):
        self._data[key] = value

    def __getattr__(self, key):
        if key in self._data:
            return self[key]
        raise AttributeError('No such attribute "{}" in {}'.format(key, self))
