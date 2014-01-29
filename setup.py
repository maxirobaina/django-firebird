from setuptools import setup, find_packages
from firebird import VERSION


with open('README.rst') as readme:
    __doc__ = readme.read()


# Provided as an attribute, so you can append to these instead
# of replicating them:
standard_exclude = ('*.py', '*.pyc', '*$py.class', '*~', '.*', '*.bak')
standard_exclude_directories = ('.*', 'CVS', '_darcs', './build',
                                        './dist', 'EGG-INFO', '*.egg-info')

setup(
    name='django-firebird',
    version=".".join(map(str, VERSION)),
    description='Firebird backend for Django 1.6.',
    long_description=__doc__,
    author='Maximiliano Robaina',
    author_email='maxirobaina@gmail.com',
    url='https://github.com/maxirobaina/django-firebird',
    packages=find_packages(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Topic :: Database',
        'Topic :: Internet :: WWW/HTTP',
    ],
    zip_safe=False,
    install_requires=[],
)



