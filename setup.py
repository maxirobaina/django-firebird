from setuptools import setup, find_packages

with open('README.rst') as readme:
    __doc__ = readme.read()


# Provided as an attribute, so you can append to these instead
# of replicating them:
standard_exclude = ('*.py', '*.pyc', '*$py.class', '*~', '.*', '*.bak', '*.orig')
standard_exclude_directories = ('.*', 'CVS', '_darcs', './build', './dist', 'EGG-INFO', '*.egg-info')


# Dynamically calculate the version based on firebird.VERSION.
version = __import__('firebird').get_version()

setup(
    name='django-firebird',
    version=version,
    description='Firebird backend for Django web framework',
    long_description=__doc__,
    license='BSD',
    author='Maximiliano Robaina',
    author_email='maxirobaina@gmail.com',
    url='https://github.com/maxirobaina/django-firebird',
    packages=find_packages(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Database',
        'Topic :: Internet :: WWW/HTTP',
    ],
    zip_safe=False,
    install_requires=[],
)
