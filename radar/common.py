""" Commonly used functions the log -
This should probably be merged with generic.py, and the
log moved to it's own file
"""
import os
import logging
from datetime import datetime

import numpy as np

from . import config


def abs_path(path: str):
    """ Returns an absolute path if the path is not already absolute
    Args:
        path (str): Relative or absolute file path
    Returns:
        str: absolute path
    """
    if not (os.path.isabs(path) or len(path.split('://')) > 1):
        path = os.path.abspath(path)
    elif path[-1] == '/':
        path = path[:-1]
    return path


def obj_col_names(obj):
    """ Returns a list of column names from a numpy record array or pandas
    dataframe
    """
    if isinstance(obj, np.ndarray):
        return list(obj.dtype.names)
    return list(obj.columns)


def add_cli_log(logger: logging.Logger,
                formatter: logging.Formatter,
                level: int = logging.ERROR) -> None:
    """ Creates CLI log of RADAR errors and above, used by default
    """
    clilog = logging.StreamHandler()
    clilog.setLevel(level)
    clilog.setFormatter(formatter)
    logger.addHandler(clilog)


def add_file_log(logger: logging.Logger,
                 formatter: logging.Formatter,
                 level: int = logging.WARNING) -> None:
    """ A file-based logger for warnings, debug, etc.
    Requires log_to_file config flag == True """
    filename = 'radar_{:%Y%m%d-%H%M}.log'.format(datetime.now())
    filelog = logging.FileHandler(filename)
    filelog.setLevel(level)
    filelog.setFormatter(formatter)
    logger.addHandler(filelog)


log = logging.getLogger('radarlog')
formatter = logging.Formatter(config.log.format)
add_cli_log(log, formatter)
if config.log.to_file:
    add_file_log(log, formatter)
