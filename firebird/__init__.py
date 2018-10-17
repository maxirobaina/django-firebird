from .version import get_version

VERSION = (1, 11, 0, 'final', 0)

__version__ = get_version(VERSION)

from . import expressions  # NOQA
