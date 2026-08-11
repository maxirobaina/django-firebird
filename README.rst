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


This version of django-firebird is working with *firebird-driver* [1], therefore it will work only with firebird 3 and later.
The current master branch of this repository is being developed under django 5.x. For previous Django stable version check
the branch list of this repository.
*firebird-driver* is the official python driver for Firebird.

Since version 5.0, the python package is called ``django_firebird`` to avoid a namespace conflict with
*firebird-driver*, which also uses the ``firebird`` package name.


[1] https://pypi.org/project/firebird-driver/


Requirements
------------
  * Python 3.11+
  * Django 5.x
  * firebird-driver 2.x (https://pypi.org/project/firebird-driver/)

Installation
------------

**Using pip**

    pip install django-firebird

**From repository**

    git clone git://github.com/maxirobaina/django-firebird.git

    cd django-firebird

    sudo python setup.py install

Configuration
-------------

Modify your setting.py ::

    DATABASES = {
        'default': {
            'ENGINE' : 'django_firebird',
            'NAME' : '/var/lib/firebird/3.0/data/mydb.fdb', # Path to database or db alias
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