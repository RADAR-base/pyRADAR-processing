#!/usr/bin/env python3
from . import config
from .generic import AttrRecDict, update
from .common import abs_path, log
from .io.generic import search_project_dir, search_dir_for_data, load_data_path


class PtcDict(AttrRecDict):
    """ Dictionary to contain participants
    It is an attribute / recursive dictionary
    (values can be accessed through __getattribute__,
     and sub-dictionaries can be accessed by splitting the
     key with "/")
    """
    def __repr__(self):
        return 'Participants:\n\t' + '\n\t'.join(self._get_keys())


class RadarObject():
    """ Base class for RADAR Project, Participant, and Data classes
    Provides methods for accessing parent class attributes, although
    several are semi-depricated
    """
    _parent = None

    def _get_attr_or_parents(self, attr):
        if hasattr(self, attr) and getattr(self, attr) is not None:
            res = getattr(self, attr)
        elif getattr(self, '_parent') is None:
            res = None
        else:
            res = self._parent._get_attr_or_parents(attr)

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
        path = '/' + (self.name if hasattr(self, 'name') else '')
        while parent._parent is not None:
            path = '/' + parent.name + path
            parent = parent._parent
        return path

    @staticmethod
    def _norm_paths(paths):
        paths = [paths] if isinstance(paths, str) else \
                paths if isinstance(paths, list) else []
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
    def __init__(self, path=None, name='', parent=None, **kwargs):
        """
        Parameters
        __________
        name : str
            The name of the project
        path : list / str
            Path(s) containing project data
        parent : radar.Project / None
            A radar.Project instance if the project is a subproject, else None
        subprojects : list
            A list of subprojects to load in paths
        participants : list
            A list of participants to load in paths
        blacklist : list
            Files/folders to ignore in paths
        ptckw : dict
            Participant keywords
        datakw : dict
            Keywords for participant's data loading
        info : dict
            dict of dicts (key:participant id, inner dict: participant info)
        labels : dict
            dict of dicts (key: participant id, inner dict: participant labels)
        schemas : None
            --
        specifications : None
            --
        armt_definitions : None
            --
        armt_protocols : None
            --
        """
        self.name = name
        self._parent = parent
        self._paths = []
        """
        self.schemas = schemas
        self.specifications = specifications
        self.armt_definitions = armt_definitions
        self.armt_protocols = armt_protocols
        """
        self.subprojects = AttrRecDict()
        self.participants = self._parent.participants[self.name] if \
            self._parent else PtcDict()
        for p in self._norm_paths(path):
            self.add_path(p, **kwargs)

        info = kwargs.get('info', False)
        if info:
            for ptc in info:
                if ptc not in self.participants:
                    self.add_participant(ptc)
            self.ptcs_update_info(info)

        labels = kwargs.get('labels', False)
        if labels:
            for ptc in labels:
                if ptc not in self.participants:
                    self.add_participant(ptc)
                self.ptcs_update_labels(labels)

    def __repr__(self):
        return 'RADAR {} of type {}: {}, {} participants'\
                .format('Subproject' if getattr(self, '_parent') is None
                        else 'Project',
                        type(self),
                        self.name,
                        len(self.participants))

    def __str__(self):
        info_string = '''
        member of {class}
        Name: {name}
        Subprojects: {subprojs}
        No. participants: {num_partic}
        '''
        format_kwargs = {
            'class': type(self),
            'name': self.name,
            'subprojs': ', '.join(self.subprojects.keys()) or 'None',
            'num_partic': len(self.participants),
        }
        return info_string.format(**format_kwargs)

    def _ipython_key_completions_(self):
        return list(self.participants.keys()) + list(self.subprojects.keys())

    def __getitem__(self, key):
        if key in self.subprojects:
            return self.subprojects[key]
        elif hasattr(self.participants, key):
            return getattr(self.participants, key)
        else:
            raise KeyError('No such subproject or participant: {}'.format(key))

    def add_participant(self, name, where='.', *args, **kwargs):
        proj = self if where == '.' else self.subprojects[where]
        if name in proj.participants:
            log.debug('participant {} already exists in project {}'
                      .format(name, proj.name))
        else:
            proj.participants[name] = Participant(name=name, parent=self,
                                                  *args, **kwargs)
        return proj.participants[name]

    def add_subproject(self, name, where='.', *args, **kwargs):
        proj = self if where == '.' else self.subprojects[where]
        if name in proj.subprojects:
            log.debug('Subproject {} already exists in project {}'
                      .format(name, proj.name))
        elif name in proj.participants:
            log.error('The subproject name {} is already '.format(name) +
                      'used as a participant in project {}'.format(proj.name))
            return None
        else:
            proj.participants[name] = PtcDict()
            proj.subprojects[name] = Project(name=name, parent=self,
                                             *args, **kwargs)
        return proj.subprojects[name]

    def _parse_path(self, path, subprojects=None, participants=None,
                    blacklist=None, **kwargs):
        dir_dict = search_project_dir(path,
                                      subprojects=subprojects,
                                      participants=participants,
                                      blacklist=blacklist)
        return dir_dict

    def _add_subprojects(self, paths, **kwargs):
        for p in paths:
            name = p.split('/')[-1]
            sp = self.add_subproject(name, **kwargs)
            sp.add_path(p, **kwargs)

    def _add_participants(self, paths, **kwargs):
        for p in paths:
            name = p.split('/')[-1]
            if name in self.participants:
                ptc = self.participants[name]
                ptc.add_path(p, **kwargs.get('datakw', {}))
            else:
                ptc = self.add_participant(name, paths=p, **kwargs)

    def add_path(self, path, **kwargs):
        self._paths.append(path)
        dirs = self._parse_path(path, **kwargs)
        self._add_subprojects(dirs['subprojects'], **kwargs)
        ptckw = kwargs.get('ptckw', {})
        ptckw.update(config.project.ptckw)
        datakw = kwargs.get('datakw', {})
        datakw.update(config.project.datakw)
        self._add_participants(dirs['participants'], datakw=datakw, **ptckw)

    def map_participants(self, func, *args, **kwargs):
        def ptc_func(ptc, *args, **kwargs):
            return func(ptc, *args, **kwargs)
        return map(ptc_func, list(self.participants))

    def _ptc_update_dict(self, new_dict, dictname):
        for ptc in self.participants:
            old_dict = getattr(ptc, dictname)
            update(old_dict, new_dict.get(ptc.name, {}))

    def ptcs_update_labels(self, labels):
        self._ptc_update_dict(labels, 'labels')

    def ptcs_update_info(self, info):
        self._ptc_update_dict(info, 'info')


class Participant(RadarObject):
    """ A class to hold data and methods concerning participants/subjects in a
    RADAR trial. Typically intialised by opening a Project.
    """
    def __init__(self, name=None, paths=None, **kwargs):
        """
        Parameters
        __________
        name : str

        paths : str or list

        info : dict (optional)

        labels : dict (optional)

        datakw : dict (optional)
            Keywords for participant's data loading
        """
        if not (name or paths):
            raise ValueError(('You must specify a name or else provide'
                              'a path (or paths) to the participant'))
        self.name = name if name is not None else paths[0].split('/')[-1]
        self._parent = kwargs.get('parent', None)
        self.info = kwargs.get('info', {})
        self.labels = kwargs.get('labels', {})
        datakw = kwargs.get('datakw', {})
        datakw.update(config.project.datakw)
        self.data = ParticipantData(self, **datakw)
        self._paths = []
        for p in self._norm_paths(paths):
            self.add_path(p, **datakw)

    def __repr__(self):
        return "Participant {}. of type {}".format(self.name, type(self))

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    def _ipython_key_completions_(self):
        return list(self.data.keys())

    def add_path(self, path, **kwargs):
        self._paths.append(path)
        self.data._search_path(path, **kwargs)

    def reparse_data(self, **datakw):
        datakw.update(config.project.datakw)
        self.data = ParticipantData(self, paths=self._paths, **datakw)

    def export_data(self, data=None, names=None, project_path=None,
                    filetype=None, *args, **kwargs):
        raise NotImplementedError
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
        given = (names, project_path, filetype)
        generic = (self.name, output['project_path'], output['filetype'])
        name, project_path, filetype = (
            x if x is not None else (generic)[i]
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
        self._parent = participant
        self._data = kwargs.pop('data', {})
        paths = self._norm_paths(kwargs.pop('paths', None))
        for path in paths:
            self._search_path(path, **kwargs)

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
        if hasattr(self._data, key):
            return getattr(self._data, key)
        raise AttributeError('No such attribute "{}" in {}'.format(key, self))

    def __iter__(self):
        return iter(self._data)

    def __repr__(self):
        topics = list(self._data.keys())
        topics.sort()
        return 'Participant data topics:\n {}'\
            .format(', '.join(topics) if topics else 'None')

    def __dir__(self):
        return self.keys()

    def _ipython_key_completions_(self):
        return list(self.keys())

    def _search_path(self, path, replace=False, **kwargs):
        modals = search_dir_for_data(path, **kwargs)
        for k, v in modals.items():
            if replace or (k not in self._data):
                self._data[k] = modals[k]

    def _load(self, name, path, **kwargs):
        data = load_data_path(path, **kwargs)
        if isinstance(data, dict):
            self._data.update(data)
        else:
            self._data[name] = data

    def save(self, outpath, outfmt, data_names=None, **kwargs):
        raise NotImplementedError
