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
sesam version 2.5.3
```


To create a sesam client binary with pyinstaller on Linux/OSX (python 3.5+ required):
```
$ cd sesam
$ virtualenv --python=python3 venv
$ . venv/bin/activate
$ pip install -r requirements.txt
$ pyinstaller --onefile sesam.py
$ dist/sesam -version
sesam version 2.5.3
```

## Configuring

#### 1- syncconfig
Sesam client needs the node to operate on and jwt to authenticate. 'NODE' and 'JWT' can be specified in 3 ways: environment variables, command line args or syncconfig file.
To specify via syncconfig file create a file named `.syncconfig` in the your repos top directory, paste and edit the following:
```
#why not specify the subscription name here as comment to avoid confusions?
NODE="<sesam node name for instance 'datahub-asdfasdf.sesam.cloud'>"
JWT="<jwt to authenticate against node>"
```

P.S. Optionally, you can use another filename and location, and then specify it as a command line argument.

#### 2- sesamconfig

Sesam client can read an optional sesamconfig file to change the default behaviour. To utilize sesamconfig create `.sesamconfig.json` in the your repos top directory, paste `{}` and add item(s) among the followings:

* **formatstyle** : add and customize following item to customize formatting. The options here correspond to 'Editor Options' on pipe/system configuration page in the sesam portal. It is sufficient to specify the non-default items only.
```json
  "formatstyle": {
    "spaces_for_indent": 2,
    "use_tab_for_indent": false,
    "space_after_colon": true,
    "space_after_comma": true,
    "new_line_before_dict_as_value": false,
    "newline_before_dict_in_array": false,
    "close_nested_array_on_new_line": true,
    "collapse_indent_for_dict_inside_array": true,
    "elements_of_array_as_value_on_separate_lines": true
  }
```
P.S. Optionally, you can use another filename and location, and then specify it as a command line argument.


#### An example sesamconfig file content:
```json
{
  "formatstyle": {
    "spaces_for_indent": 4,
    "elements_of_array_as_value_on_separate_lines": true
  }
}
```

### [Back to main page](./readme.md)