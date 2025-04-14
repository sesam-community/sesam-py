## Installing

Obtain the latest binaries for your OS from [Github Releases](https://github.com/sesam-community/sesam-py/releases/), unpack and make sure it is accessible in your "PATH". Create optional configuration files as described below.

Verify your installation by running `sesam -version` command.

If you want to run from the source or build locally:
To install and run the sesam client with python on Linux/OSX (python 3.5+ required):
```
$ cd sesam-py
$ virtualenv --python=python3 venv
$ . venv/bin/activate
$ pip install -r requirements.txt
$ python sesam.py -version
sesam version x.y.z
```


To create a sesam client binary with pyinstaller on Linux/OSX (python 3.5+ required):
```
$ cd sesam-py
$ virtualenv --python=python3 venv
$ . venv/bin/activate
$ pip install -r requirements.txt
$ pip install pyinstaller
$ pyinstaller --onefile --add-data "connector_cli:connector_cli" sesam.py
$ dist/sesam -version
sesam version x.y.z
```

Or you can create the binary by running `docker compose up -d`, this will run all of the unit tests and build the binary.
The binary build will take a little extra time after the docker command finished as it runs the build at the end once the docker container is ready.
Just check the dist folder and wait for the sesam binary to appear.

### [Back to main page](./README.md)
