from .version import get_version

VERSION = (2, 2, 0, 'alpha', 2)

__version__ = get_version(VERSION)

from . import expressions  # NOQA
