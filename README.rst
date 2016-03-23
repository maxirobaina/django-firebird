===============
django-firebird
===============

.. image:: https://img.shields.io/pypi/v/django-firebird.svg
    :target: https://pypi.python.org/pypi/django-firebird

.. image:: https://img.shields.io/pypi/dm/django-firebird.svg
    :target: https://pypi.python.org/pypi/django-firebird

.. image:: https://caniusepython3.com/project/django-firebird.svg
    :target: https://caniusepython3.com/project/django-firebird


Firebird SQL backend for django
-------------------------------

This version of django-firebird is working with *fbd* [1] .Therefore it will work only with firebird 2.x and later.
Also, the current master version of this repository is being developed under django 1.7.x. For previous Django stable version there is the stable/1.6.x branch and we consider the driver stable.
fbd is the official stable python-firebird driver, also it has support for python 3.


[1] http://pypi.python.org/pypi/fdb/

*Update*: django-firebird beta 1 status with support for django 1.7

Requirements
------------
  * Python 2.6+ or Python 3.x
  * Django 1.7.x
  * fdb (http://pypi.python.org/pypi/fdb/)

Instalation
-----------

**Using pip**

    pip install django-firebird

**From repository**

    git clone git://github.com/maxirobaina/django-firebird.git

    cd django-firebird

    sudo python setup.py install

**Manual Instalation**

Instructions for Ubuntu/Debian
I assume you have installed django from source with python setup.py install


    cd /usr/local/lib/python2.7/dist-packages

    sudo git clone git://github.com/maxirobaina/django-firebird.git

    sudo ln -s django-firebird/firebird firebird

    cd /usr/local/lib/python2.7/dist-packages/django/db/backends

    sudo ln -s /usr/local/lib/python2.7/dist-packages/django-firebird/firebird

Configuration
-------------

Modify your setting.py ::

    DATABASES = {
        'default': {
            'ENGINE' : 'firebird',
            'NAME' : '/var/lib/firebird/2.5/data/django_firebird.fdb', # Path to database or db alias
            'USER' : 'SYSDBA',           # Your db user
            'PASSWORD' : '*****',    # db user password
            'HOST' : '127.0.0.1',        # Your host machine
            'PORT' : '3050',             # If is empty, use default 3050
            #'OPTIONS' : {'charset':'ISO8859_1'}
        }
    }

Contributing
------------


Code and issues is in GitHub:

    https://github.com/maxirobaina/django-firebird

We also have a mailing list:

    http://groups.google.com/group/django-firebird-dev

Legacy driver
-------------

Why the change from kinterbasdb to fdb?

If you want to know more about the differences between *fdb* and *kinterbasdb* you can look at:

http://thread.gmane.org/gmane.comp.db.firebird.python/185/focus=187

http://pythonhosted.org//fdb/differences-from-kdb.html

If you still use *kinterbasdb*, the original google code repository has an updated django-firebird 1.4.x LTS version.

https://github.com/mariuz/django-firebird-1.4
