from .version import get_version

VERSION = (3, 0, 0, 'alpha', 0)

__version__ = get_version(VERSION)

from . import expressions  # NOQA
