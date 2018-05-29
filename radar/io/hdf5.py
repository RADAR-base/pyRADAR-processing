#!/usr/bin/env python3
import os
import tables
import numpy as np
import pandas as pd
from ..common import obj_col_names, progress_bar, AttrRecDict
from ..defaults import _FILTER
from .generic import ProjectIO, ParticipantIO, ParticipantData, RadarTable

SPEC_HDF_TYPE = {
    'TIMESTAMP': tables.Int64Col(),
    'DURATION': tables.Int64Col(),
}

NP_HDF_CONVERTERS = {
    '<M8[ns]': lambda x=None: 'int64',
    '|O': lambda x: 'S' + str(max(map(len, str(x)))),
}

# Radar metadata names. Radar type can be project, participant, or data
RADAR_TYPE = 'RADAR_TYPE'
PROJECT = 'PROJECT'
PARTICIPANT = 'PARTICIPANT'
DATA = 'DATA'

class ProjectFile(tables.File):
    """ HDF5 file for a RADAR project. Subclass of tables.File TODO
    Parameters
    __________
    See Also
    ________
    tables.File : For more information on the PyTables File object
    """

    def __init__(self, filename, mode='r', title='', root_uep='/',
                 filters=_FILTER, subprojects=None, **kwargs):
        super(ProjectFile, self).__init__(filename=filename, mode=mode,
                                          title=title, root_uep=root_uep,
                                          filters=filters, **kwargs)
        self.data = ProjectGroup(self.root)

    def create_project(self, where, name, title="", filters=_FILTER, **kwargs):
        """ Creates a RADAR project group (Can be a subproject).
        Parameters
        __________
        where: str
            The location to create the project relative to the overall project
            root directory
        name: str
            The name of the project
        title: str (optional)
            A title for the HDF5 group
        filters: pytables filter object (optional)
            The filters to apply to data stored within this group.

        Output
        ______
        sp: ProjectGroup
            A ProjectGroup object of the newly created project.
        """
        if where + '/' + name in self.root:
            sp = self.get_node(where + '/' + name)
        else:
            sp = self.create_group(where, name, title=title, filters=filters)
            setattr(sp._v_attrs, 'NAME', name)
            setattr(sp._v_attrs, RADAR_TYPE, PROJECT)
        sp = ProjectGroup(sp, **kwargs)
        return sp

    def create_participant(self, where, name, title="", filters=_FILTER):
        if where + '/' + name in self.root:
            ptc = self.get_node(where + '/' + name)
        else:
            ptc = self.create_group(where, name, title=title, filters=filters)
            setattr(ptc._v_attrs, 'NAME', name)
            setattr(ptc._v_attrs, RADAR_TYPE, PARTICIPANT)
        ptc.__class__ = ParticipantGroup
        return ptc

    def create_radar_data_group(self, where, name, description=None, title='',
                                filters=_FILTER, createparents=True, obj=None,
                                **kwargs):
        """ Create a new radar data group
        Parameters
        __________

        See also
        ________
        """
        parentnode = self._get_or_create_path(where, createparents)
        tables.file._checkfilters(filters)
        new = name not in parentnode
        ptobj = H5DataGroup(parentnode, name, title=title,
                               filters=filters, new=new, **kwargs)
        if obj:
            ptobj.append_dataframe(obj)
        return ptobj

    def create_radar_table(self, where, name, description=None, title='',
                     filters=_FILTER, expectedrows=1000000,
                     chunkshape=None, byteorder=None,
                     createparents=True, obj=None):
        """ Create a new radar table
        Essentially a copy of tables.File.create_table()
        Parameters
        __________
        See also
        ________
        tables.File.create_table()
        """
        if obj is not None:
            if isinstance(obj, np.ndarray):
                pass
            elif isinstance(obj, pd.DataFrame):
                obj = _df_to_usable(obj)
            else:
                raise TypeError('Invalid obj type %r' %obj)
            descr, _ = tables.description.descr_from_dtype(obj.dtype)
            if (description is not None and
                tables.description.dtype_from_descr(description) != obj.dtype):
                raise TypeError('The description parameter is not consistent ',
                            'with the data object')
            description = descr
        parentnode = self._get_or_create_path(where, createparents)
        if description is None:
            raise ValueError('No description provided')
        tables.file._checkfilters(filters)

        ptobj = H5Table(parentnode, name, description=description,
                           title=title, filters=filters,
                           expectedrows=expectedrows, chunkshape=chunkshape,
                           byteorder=byteorder)
        if obj is not None:
            ptobj.append(obj)

    def create_table_schema(self, where, name, schema, createparents=True,
                            **kwargs):
        """ Create a new table based on a given schema TODO
        """
        raise NotImplementedError()
        description = schemafunc(schema)
        self.create_radar_table(where, name, description=description,
                                createparents=createparents, **kwargs)

    def save_dataframe(self, df, where, name, source_type=DATA, **kwargs):
        """Add a pandas dataframe to an entrypoint in the hdf5 file
        """
        table = self.create_radar_table(where, name, obj=df, overwrite=True,
                                        **kwargs)
        setattr(table._v_attrs, 'NAME', name)
        setattr(table._v_attrs, RADAR_TYPE, source_type)
        return table


class ProjectGroup(ProjectIO):
    """
    """
    def __init__(self, hdf, **kwargs):
        if isinstance(hdf, tables.link.ExternalLink):
            hdf = hdf()
        self._hdf = hdf

        subprojects = kwargs.get('subprojects')
        self.subprojects = {} if subprojects is None else subprojects

        participants = kwargs.get('participants')
        self.participants = AttrRecDict() if participants is None else \
                            participants

        self.subprojects.update(self.get_subprojects())
        self.participants.update(self.get_participants())

        self.name = kwargs['name'] if 'name' in kwargs else \
                    self._hdf._v_file.filename + self._hdf._v_pathname

        self.parent = kwargs['parent'] if 'parent' in kwargs else None

    def get_subprojects(self):
        sp = AttrRecDict()
        for name, child in self._hdf._v_children.items():
            if isinstance(child, tables.link.Link):
                child = child()
            if not hasattr(child._v_attrs, RADAR_TYPE):
                continue
            if child._v_attrs.RADAR_TYPE == PROJECT:
                self.participants[name] = AttrRecDict()
                sp[name] = ProjectGroup(child,
                                        participants=self.participants[name],
                                        name=name, parent=self)
        return sp

    def get_participants(self):
        ptcs = AttrRecDict()
        for name, child in self._hdf._v_children.items():
            if isinstance(child, tables.link.Link):
                child = child()
            if not hasattr(child._v_attrs, RADAR_TYPE):
                continue
            if child._v_attrs.RADAR_TYPE == PARTICIPANT:
                child.__class__ = ParticipantGroup
                ptcs[name] = child
        return ptcs

    def create_subproject(self, name, where=None):
        if where is None:
            where = self._hdf._v_pathname
        self.participants[name] = AttrRecDict()
        sp = self._hdf._v_file.\
                create_project(name=name, where=where,
                               participants=self.participants[name])
        self.subprojects[name] = sp
        return self.subprojects[name]

    def create_participant(self, name, where=None):
        if where is None:
            where = self._hdf._v_pathname
        ptc = self._hdf._v_file.\
                create_participant(name=name, where=where)
        self.participants[name] = ptc
        return self.participants[name]


class ParticipantGroup(tables.Group, ParticipantIO):
    """ A Pytables group object for use with RADAR participant data.
    Exactly the same as a pytables Group, but has the method 'get_data_dict' to
    get a dictionary of participant data groups/tables.
    """
    def __init__(self, *args, **kwargs):
        super(ParticipantGroup, self).__init__(*args, **kwargs)
        self._dict = get_data_dict()

    def get_data_dict(self):
        data_dict = ParticipantData()
        for node in self._f_iter_nodes():
            if isinstance(node, tables.link.Link):
                node = node()
            if isinstance(node, tables.group.Group):
                node.__class__ = H5DataGroup
            elif isinstance(node, tables.table.Table):
                node.__class__ = H5Table
            data_dict[node._v_name] = node
        return data_dict

    def create_table(self, name, where=None, obj=None, descr=None, *args, **kwargs):
        if where is None:
            where = self._v_pathname
        else:
            where = '/'.join([self._v_pathname, where])
        if (obj is None) and (descr is None):
            raise ValueError('One or both of obj and descr must be given')
        if isinstance(obj, RadarTable):
            obj = obj[:]
        tab = self._v_file.create_radar_table(name=name, where=where, obj=obj,
                description=descr, *args, **kwargs)
        return tab

    def append_table(self, name, obj, where=None, *args, **kwargs):

        return 0

    def delete_table(self, name, where=None):

        return 0


class H5Table(tables.Table, RadarTable):
    """ A Pytables table object for use with RADAR participant data.
    """
    def __init__(self, *args, **kwargs):
        super(H5Table, self).__init__(*args, **kwargs)
        setattr(self._v_attrs, RADAR_TYPE, DATA)

    def append_dataframe(self, df, *args, **kwargs):
        df = _df_to_usable(df)
        self.append(df, *args, **kwargs)

    def __getitem__(self, key):
        df = super(H5Table, self).__getitem__(key)
        df = pd.DataFrame.from_records(df)
        return df


class H5DataGroup(tables.Group):
    """ A Group object for storing RADAR data.
    Each column (i.e. array) is unrelated. It is recommended that columns are
    written to be the same length and rows correspond to the same timepoint.
    The advantage of a H5DataGroup over a standard H5Table is that it is
    easier to add and remove columns and to write data in an asynchronous
    manner.

    See also
    ________
    tables.Group : For more information on the PyTables Group object
    radar.io.hdf5.H5Table : A table based storage object
    """
    def __init__(self, parentnode, name, filters=_FILTER,
                 obj=None, overwrite=False, **kwargs):
        super(H5DataGroup, self).__init__(parentnode, name,
                                             filters=_FILTER, **kwargs)
        if obj is not None:
            self.insert_dataframe(obj, overwrite=overwrite)

    def _item_parse(self, item):
        cols = None
        index = None

        if not isinstance(item, tuple):
            item = (item,)
        if len(item) > 2:
            raise IndexError(('Only subscriptable with a row index '
                              '(number or slice, numpy indexing allowed), '
                              'and/or column name(s)'))

        for item_x in item:
            if isinstance(item_x, str):
                cols = [item_x]
            elif isinstance(item_x, slice):
                index = item_x
            elif isinstance(item_x, list):
                if isinstance(item_x[0], str):
                    cols = item_x
                elif isinstance(item_x[0], int):
                    index = item_x
                else:
                    raise TypeError(('A given list should only contain '
                                     'column names or row indexes'))
            else:
                raise IndexError(('Only subscriptable with a row index '
                                  '(number or slice, numpy indexing allowed), '
                                  'and/or column name(s)'))
        if cols is None:
            cols = [c._v_name for c in self._f_iter_nodes()]
        if index is None:
            index = slice(None)

        return (cols, index)

    def _col_dtypes(self, column_names=None):
        dtypes = {}
        if column_names is None:
            column_names = [c._v_name for c in self._f_iter_nodes()]
        for col in column_names:
            colp = getattr(self, col)
            if hasattr(colp._v_attrs, 'np_dtype'):
                dtypes[col] = colp._v_attrs.np_dtype
        return dtypes


    def __getitem__(self, item):
        """Returns the columns and/or indexes specified from the RADAR table.
        Parameters
        __________
        item: str, slice, list
            May be a column name, list of column names, a slice, or a
            combination of a slice and column name(s).
            e.g.
            H5DataGroup[0:100] for index 0 to 100 of all columns.
            H5DataGroup['c1'] for the entirety of column c1.
            H5DataGroup[['c1', 'c2']] for the entireties of columns 'c1' and
                'c2'
            H5DataGroup['c1', 5:25] for index 5 to 25 of column 'c1'
            H5DataGroup[['c1', 'c3'], -25:] for the final 25 values of columns
                'c1' and 'c3'.

        Returns
        _______
        table : pandas.DataFrame
            Returns a pandas DataFrame made from the given columns and indexes

        """
        cols, index = self._item_parse(item)
        dtypes = self._col_dtypes(cols)
        return pd.DataFrame({c: self._f_get_child(c)[index]
                             for c in cols}).astype(dtypes)

    def __setitem__(self, item, df):
        """Sets the column(s) at the given index to the values in the given
        dataframe. The dataframe must have the same columns as the table, or
        a subset of table columns must be given.
        Parameters
        __________
        item: slice, list
            Must be a slice or a slice and column name(s)
            e.g.
            H5DataGroup[0:100] = df
            H5DataGroup[0:100, 'c1'] = df
            H5DataGroup[0:100, ['c1', 'c2', 'c3']] = df
        df: pandas.DataFrame
            A dataframe with the same columns as the table/given as the item.
            Alternatively, if only one column is selected, may be a numpy array
            or python list.

        """
        cols, index = self._item_parse(item)
        if len(cols) == 1 and (isinstance(df, np.ndarray) or
                               isinstance(df, list)):
            self._f_get_child(cols[0])[index] = np.ndarray
            return

        if len(df.columns) != len(cols):
            raise ValueError(('DataFrame must have the same number of '
                              'columns as the table, or the table columns '
                              'must be given'))
        if not all([x in cols for x in df]):
            raise ValueError(('The dataframe must have the same column names '
                              'as the table'))

        for col in cols:
            self._f_get_child(col)[index] = df[col].values

    def insert_dataframe(self, df, overwrite=False, attrs=None):
        for col in obj_col_names(df):
            self.insert_array(df[col].values, name=col, attrs=attrs)

    def append_dataframe(self, df, create_columns=True):
        for col in obj_col_names(df):
            self.append_array(df[col].values, name=col,
                              create_column=create_columns)

    def insert_array(self, arr, name, overwrite=False, attrs=None):
        if name in self._v_children:
            if overwrite:
                self._hdf._f_getChild(name).remove()
            else:
                raise ValueError(('There is already a column "{}" in table'
                                  '{}'.format(name, self._v_name)))

        dt = arr.dtype.str
        if attrs is not None:
            attrs['np_dtype'] = dt
        else:
            attrs = {'np_dtype': dt}
        if dt in NP_HDF_TYPES:
            arr = arr.astype(NP_HDF_TYPES[dt])

        self._v_file.create_earray(self, name=name, title=name, obj=arr)
        for k, v in attrs.items():
            setattr(self._f_get_child(name)._v_attrs, k, v)

    def append_array(self, arr, name, create_column=True):
        if name not in self._v_children:
            if create_column:
                self.insert_array(arr, name)
            else:
                raise ValueError(('There is no such column {} '
                                  'and create_column is set to '
                                  'False'.format(name)))
        else:
            self._f_get_child(name).append(arr)


def open_project_file(filename, mode='r', title='', root_uep='/',
                      filters=_FILTER, **kwargs):
    """
    """
    if tables.file._FILE_OPEN_POLICY == 'strict':
        if filename in tables.file._open_files:
            raise ValueError('The file %s is already open', filename)
    else:
        for filehandle in \
        tables.file._open_files.get_handlers_by_name(filename):
            omode = filehandle.mode
            if mode == 'r' and omode != 'r':
                raise ValueError(
                    'The file "%s" is already opened not in ',
                    'read-only mode', filename)
            elif mode in ('a', 'r+') and omode == 'r':
                raise ValueError(
                    'The file "%s" is already opened in ',
                    'read-only mode. Can\'t open append mode.')
            elif mode == 'w':
                raise ValueError(
                    'The file "{}" is already opened. Can\'t ',
                    'reopen in write mode', filename)
    return ProjectFile(filename, mode, title, root_uep, filters, **kwargs)

def _df_to_usable(df, converters=NP_HDF_CONVERTERS):
    if type(df.index) is not pd.RangeIndex:
        rec = df.to_records()
    else:
        rec = df.to_records(index=False)

    dtypes = []
    for name, dt in rec.dtype.descr:
        dtypes.append(converters[dt](rec[name])) if dt in converters \
                else dtypes.append(dt)

    return np.rec.fromrecords(rec, formats=dtypes, names=rec.dtype.names)

