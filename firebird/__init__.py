from .version import get_version

VERSION = (1, 11, 0, 'beta', 1)

__version__ = get_version(VERSION)

from . import expressions  # NOQA
