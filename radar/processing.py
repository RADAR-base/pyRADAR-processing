#!/usr/bin/env python3
import types
from .io.hdf5 import ProjectFile
def identity(x):
    return x

class ProcessMap():
    def __init__(self,
                 input_modals,
                 functions,
                 output_names,
                 time_col=None,
                 time_func=identity):
        self.input_modals = input_modals
        self.output_names = output_names
        self.functions = functions
        self.time_col = time_col
        self.time_func = time_func

    def __call__(self, participant, output=None):
        if not all([m in participant.data for m in self.input_modals]):
            return
        if output is None:
            output = participant._hdf._v_file
        data = [participant.data[m] for m in self.input_modals]
        for funcs, outname in zip(self.functions, self.output_names):
            f = funcs[0]
            d = f(*data)
            for f in funcs[1:]:
                d = f(d)
            self._output(hdf_file=output,
                         where=participant._hdf._v_pathname,
                         name=outname,
                         obj=d)

    def _output(self, hdf_file, where, name, obj):
        if isinstance(obj, types.GeneratorType):
            d = next(obj)
            tab = ProjectFile.create_radar_table(hdf_file, where, name, obj=d)
            for d in obj:
                tab.append(d)
        else:
            ProjectFile.create_radar_table(hdf_file, where, name, obj=obj)

