from .version import get_version

VERSION = (2, 0, 0, 'alpha', 1)

__version__ = get_version(VERSION)

from . import expressions  # NOQA
