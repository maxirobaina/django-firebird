django-firebird
===============

Firebird SQL backend for django

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