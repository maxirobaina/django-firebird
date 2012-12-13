django-firebird
===============

Firebird SQL backend for django

This version of django-firebird is working with fbd [1] instead of kinterbasdb. Therefore it will work with firebird 2.x and later.
Also, the current master version of this repository is being developed under django 1.5. This is why it's actually in alpha stage.

Why the change?
fbd will be the next official python-firebird driver, also it has support for python 3.

If you want to know more about the diferrences between fdb and kinterbasdb you can look at:
http://thread.gmane.org/gmane.comp.db.firebird.python/185/focus=187


If you still use kinterbasdb, the original google code repository has an updated django-firebird 1.4 version.

http://code.google.com/p/django-firebird/


[1] http://pypi.python.org/pypi/fdb/


Instructions for Ubuntu/Debian 
I assume you have installed django from source with python setup.py install 


    cd /usr/local/lib/python2.7/dist-packages

    sudo git clone git://github.com/maxirobaina/django-firebird.git

    sudo ln -s django-firebird/firebird firebird

    cd /usr/local/lib/python2.7/dist-packages/django/db/backends

    sudo ln -s /usr/local/lib/python2.7/dist-packages/django-firebird/firebird

    DATABASES = {
    'default': {
      'ENGINE' : 'django.db.backends.firebird',
    'NAME' : '/var/lib/firebird/2.5/data/rdbgraph.fdb', # Path to database or db alias
    'USER' : 'SYSDBA',           # Your db user
    'PASSWORD' : '*****',    # db user password
    'HOST' : '127.0.0.1',        # Your host machine
    'PORT' : '3050',             # If is empty, use default 3050
    #'OPTIONS' : {'charset':'ISO8859_1'}  
    }
    }
