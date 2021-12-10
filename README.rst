===============
django-firebird
===============

.. image:: https://img.shields.io/pypi/v/django-firebird.svg
    :target: https://pypi.python.org/pypi/django-firebird


Firebird SQL backend for django
-------------------------------

**Repo Note**:
The ``master`` branch is an *in development* version of django-firebird. This may be substantially different from the latest
`release of django-firebird`_

.. _release of django-firebird: https://github.com/maxirobaina/django-firebird/releases


This version of django-firebird is working with *fbd* [1], therefore it will work only with firebird 2.x and later.
The stable version corresponds with django 2.2 and live into ``stable/2.2.x`` branch.
The current master branch of this repository is being developed under django 3.x. For previous Django stable version check
the branch list of this repository.
*fbd* is the official stable python-firebird driver, also it has support for python 3.


[1] http://pypi.python.org/pypi/fdb/


Requirements
------------
  * Python 3.x
  * Django 2.2.x
  * fdb (http://pypi.python.org/pypi/fdb/)

Installation
------------

**Using pip**

    pip install django-firebird

**From repository**

    git clone git://github.com/maxirobaina/django-firebird.git

    cd django-firebird

    sudo python setup.py install

**Manual Installation**

Instructions for Ubuntu/Debian
I assume you have installed django from source with python setup.py install


    cd /usr/local/lib/python3.8/dist-packages

    sudo git clone git://github.com/maxirobaina/django-firebird.git

    sudo ln -s django-firebird/firebird firebird

    cd /usr/local/lib/python3.8/dist-packages/django/db/backends

    sudo ln -s /usr/local/lib/python3.8/dist-packages/django-firebird/firebird

Configuration
-------------

Modify your setting.py ::

    DATABASES = {
        'default': {
            'ENGINE' : 'django.db.backends.firebird',
            'NAME' : '/var/lib/firebird/3.0/data/django_firebird.fdb', # Path to database or db alias
            'USER' : 'SYSDBA',           # Your db user
            'PASSWORD' : '*****',    # db user password
            'HOST' : '127.0.0.1',        # Your host machine
            'PORT' : '3050',             # If is empty, use default 3050
            #'OPTIONS' : {'charset':'ISO8859_1'}
        }
    }

Known bugs and issues
---------------------

* Some database migrations doesn't work by default. Sometimes is better make intermediate migrations for solve problems.
* Some Query Expressions doesn't work by default. We need to make some workaround, ie: Use Cast().
* Combined duration expressions need more research. No all combination of expressions works.


Contributing
------------

Code and issues is in GitHub:

    https://github.com/maxirobaina/django-firebird

We also have a mailing list:

    http://groups.google.com/group/django-firebird-dev
