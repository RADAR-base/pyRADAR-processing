#!/usr/bin/env python3
import dask.core as dc
from ..common import to_datetime

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
        sp[name] = ProjectFolder(path=folder_path,
                                 name=name,
                                 subproject_names=sub_sp,
                                 participants=self.participants[name],
                                 parent=self,
                                 **kwargs)
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
        path = os.path.join(where, name)
        fullwhere, basename = os.path.split(path)
        modalities[basename] = data_loader(fullwhere, basename, data_funcs)
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

