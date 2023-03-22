## Installing

Obtain the latest binaries for your OS from [Github Releases](https://github.com/sesam-community/sesam-py/releases/), unpack and make sure it is accessible in your "PATH". Create optional configuration files as described below.

Verify your installation by running `sesam -version` command.

If you want to run from the source or build locally:
To install and run the sesam client with python on Linux/OSX (python 3.5+ required):
```
$ cd sesam
$ virtualenv --python=python3 venv
$ . venv/bin/activate
$ pip install -r requirements.txt
$ python sesam.py -version
sesam version 2.5.19
```


To create a sesam client binary with pyinstaller on Linux/OSX (python 3.5+ required):
```
$ cd sesam
$ virtualenv --python=python3 venv
$ . venv/bin/activate
$ pip install -r requirements.txt
$ pyinstaller --onefile sesam.py
$ dist/sesam -version
sesam version 2.5.19
```

### [Back to main page](./README.md)