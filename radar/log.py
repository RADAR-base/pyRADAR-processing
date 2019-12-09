""" Commonly used functions the log -
This should probably be merged with generic.py, and the
log moved to it's own file
"""
import logging
from datetime import datetime
from . import config


def add_cli_log(logger: logging.Logger,
                formatter: logging.Formatter,
                level: int = logging.ERROR) -> None:
    """Create a CLI log of RADAR errors and above, used by default."""
    clilog = logging.StreamHandler()
    clilog.setLevel(level)
    clilog.setFormatter(formatter)
    logger.addHandler(clilog)


def add_file_log(logger: logging.Logger,
                 formatter: logging.Formatter,
                 level: int = logging.WARNING) -> None:
    """Create a file-based logger for warnings, debug, etc.
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
