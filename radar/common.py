import os
import logging
import numpy as np
from datetime import datetime
from . import config

def abs_path(path):
    if not (os.path.isabs(path) or len(path.split('://')) > 1):
        path = os.path.abspath(path)
    elif path[-1] is '/':
        path = path[:-1]
    return path

def obj_col_names(obj):
    """ Returns a list of column names from a numpy record array or pandas
    dataframe
    """
    if isinstance(obj, np.ndarray):
        return list(obj.dtype.names)
    else:
        return list(obj.columns)

def add_cli_log(log, formatter, level=logging.ERROR):
    clilog = logging.StreamHandler()
    clilog.setLevel(level)
    clilog.setFormatter(formatter)
    log.addHandler(clilog)
    return log

def add_file_log(log, formatter, level=logging.WARNING):
    filename = 'radar_{:%Y%m%d-%H%M}.log'.format(datetime.now()) 
    filelog = logging.FileHandler(filename)
    filelog.setLevel(level)
    filelog.setFormatter(formatter)
    log.addHandler(filelog)
    return log

log = logging.getLogger('radarlog')
formatter = logging.Formatter('%(asctime)s - %(name)s' + \
                              ' - %(levelname)s - %(message)s')
log = add_cli_log(log, formatter)
if config['log_to_file']:
    log = add_file_log(log, formatter)
