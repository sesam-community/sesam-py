## Configuring

#### 1- syncconfig

The sesam client will use the directory you launch it from as its working directory.

Sesam client needs the node to operate on and jwt to authenticate. 'NODE' and 'JWT' can be specified in 3 ways: environment variables, command line args or syncconfig file.
To specify via syncconfig file create a file named `.syncconfig` in your repo's top directory, paste and edit the following:
```
#why not specify the subscription name here as comment to avoid confusions?
NODE="<sesam node name for instance 'datahub-asdfasdf.sesam.cloud'>"
JWT="<jwt to authenticate against node>"
```

If the `.syncconfig` file isn't found in the current directory, the sesam client will traverse the parent directory 
upwards to look for one. Tip: if you have multiple repos with config you can share the `.syncconfig` file between them by
placing it into the topmost parent directory of your repo.

Optionally, you can use another filename and location, and then specify it as a command line argument.

#### 2- authconfig

When working on a connector repository (e.g. Hubspot connector), Sesam client uses `.authconfig` file to authenticate via OAuth2 or 
Tripletex and update secrets and environment variables. To utilize authconfig create `.authconfig` in the working 
directory, paste and edit the following:

for OAuth2 authentication:
```bash
CLIENT_ID=<client id for OAuth2 authentication>
CLIENT_SECRET=<client secret for OAuth2 authentication>
```
for Tripletex authentication:
```bash
CONSUMER_TOKEN=<client id for Tripletex authentication>
EMPLOYEE_TOKEN=<client secret for Tripletex authentication>
```

* The secrets and environment variables will be updated in the node specified in the `syncconfig` file.
* It is recommended to clone a connector repo and add the `.authconfig` file to the working directory.
* Optionally the secrets can also be passed as command line arguments specified below.
* To upload additional secrets and environment variables, add them to the profile file (e.g. test-env.json).
* sesam-py automatically detects the login service required for the connector.

#### 3- sesamconfig

Sesam client can read an optional sesamconfig file to change the default behaviour. To utilize sesamconfig create `.sesamconfig.json` in your repo's top directory, paste `{}` and add item(s) among the followings:

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

## Usage

```
usage: sesam [-h] [-version] [-v] [-vv] [-vvv] [-skip-tls-verification] [-sync-config-file <string>] [-dont-remove-scheduler] [-dump]
             [-print-scheduler-log] [-use-internal-scheduler] [-custom-scheduler] [-scheduler-image-tag <string>] [-node <string>]
             [-scheduler-node <string>] [-jwt <string>] [-single <string>] [-no-large-int-bugs] [-disable-user-pipes] [-enable-user-pipes]
             [-compact-execution-datasets] [-unicode-encoding] [-disable-json-html-escape] [-profile <string>] [-scheduler-id <string>]
             [-scheduler-zero-runs <int>] [-scheduler-max-runs <int>] [-scheduler-max-run-time <int>] [-restart-timeout <int>] [-runs <int>]
             [-logformat <string>] [-scheduler-poll-frequency <int>] [-sesamconfig-file <string>] [-add-test-entities] [-force-add]
             [command]

Commands:
  authenticate   Authenticates against the external service of the connector and updates secrets and environment variables (available only when working on a connector)
  wipe      Deletes all the pipes, systems, user datasets and environment variables in the node
  restart   Restarts the target node (typically used to release used resources if the environment is strained)
  upload    Replace node config with local config. Also tries to upload testdata if 'testdata' folder present and updates secrets and environment variables when working on a connector (might ask for authentication).
  download  Replace local config with node config
  dump      Create a zip archive of the config and store it as 'sesam-config.zip'
  status    Compare node config with local config (requires external diff command)
  run       Run configuration until it stabilizes
  update    Store current output as expected output
  convert   Convert embedded sources in input pipes to http_endpoints and extract data into files
  verify    Compare output against expected output
  test      Upload, run and verify output
  stop      Stop any running schedulers (for example if the client was permaturely terminated or disconnected)
  init      Add conditional sources to input pipes with a "test" and "prod" alternative. Also initializes the connector if used.

positional arguments:
  command               a valid command from the list above

optional arguments:
  -h, --help            show this help message and exit
  -version              print version number
  -v                    be verbose
  -vv                   be extra verbose
  -vvv                  be extra extra verbose
  -skip-tls-verification
                        skip verifying the TLS certificate
  -sync-config-file <string>
                        sync config file to use, the default is '.syncconfig' in the current directory
  -dont-remove-scheduler
                        don't remove scheduler after failure (DEPRECATED)
  -dump                 dump zip content to disk
  -print-scheduler-log  print scheduler log during run
  -use-internal-scheduler
                        use the built-in scheduler in sesam instead of a microservice (DEPRECATED)
  -custom-scheduler     by default a scheduler system will be added, enable this flag if you have configured a custom scheduler as part of the config
                        (DEPRECATED)
  -scheduler-image-tag <string>
                        the scheduler image tag to use (DEPRECATED)
  -node <string>        service url
  -scheduler-node <string>
                        service url for scheduler
  -jwt <string>         authorization token
  -single <string>      update or verify just a single pipe
  -no-large-int-bugs    don't reproduce old large int bugs
  -disable-user-pipes   turn off user pipe scheduling in the target node (DEPRECATED)
  -enable-user-pipes    turn on user pipe scheduling in the target node
  -compact-execution-datasets
                        compact all execution datasets when running scheduler
  -unicode-encoding     store the 'expected output' json files using unicode encoding ('\uXXXX') - the default is UTF-8
  -disable-json-html-escape
                        turn off escaping of '<', '>' and '&' characters in 'expected output' json files
  -profile <string>     env profile to use <profile>-env.json
  -scheduler-id <string>
                        system id for the scheduler system (DEPRECATED)
  -scheduler-zero-runs <int>
                        the number of runs that has to yield zero changes for the scheduler to finish
  -scheduler-max-runs <int>
                        maximum number of runs that scheduler can do to before exiting (internal scheduler only)
  -scheduler-max-run-time <int>
                        the maximum time the internal scheduler is allowed to use to finish (in seconds, internal scheduler only)
  -restart-timeout <int>
                        the maximum time to wait for the node to restart and become available again (in seconds). The default is 15 minutes. A value of 0
                        will skip the back-up-again verification.
  -runs <int>           number of test cycles to check for stability
  -logformat <string>   output format (normal, log or azure)
  -scheduler-poll-frequency <int>
                        milliseconds between each poll while waiting for the scheduler
  -sesamconfig-file <string>
                        sesamconfig file to use, the default is '.sesamconfig.json' in the current directory
  -add-test-entities
                        use with the init command to add test entities to input pipes
  -force-add
                        use with the '-add-test-entities' option to overwrite test entities that exist locally
  --system-placeholder <string>
                        Name of the system _id placeholder (available only when working on a connector)
  -d <string>           
                        Connector folder to work with (available only when working on a connector)
  -e <string>           
                        Directory to expand the config into (available only when working on a connector)
  --client_id <string>  
                        OAuth2 client id (available only when working on a connector)
  --client_secret <string>
                        OAuth2 client secret (available only when working on a connector)
  --service_url <string>
                        url to service api (include /api) (available only when working on a connector)
  --service_jwt <string>
                        jwt token to the service api (available only when working on a connector)
  --consumer_token <string>
                        consumer token (available only when working on a connector)
  --employee_token <string>
                        employee token (available only when working on a connector)
  --base_url <string>   
                        override to use prod env (available only when working on a connector)
  --days <string>       
                        number of days until the token should expire (available only when working on a connector)
```

### Preparing input pipes for testing

If the node has separate environments for production and testing, input pipes can automatically be configured to use 
a static, embedded source during testing. This can be done using the `init` command (see additional options above):
```
$ sesam init
```
This assumes that the node already has the variable `node-env` defined, where the value must be either `"prod"` for
production or `"test"` for testing. Note that this command only modifies the local pipe configurations.

### Typical workflow
```
$ sesam upload
Node config replaced with local config.
## edit stuff in Sesam Management Studio
$ sesam download
Local config replaced by node config.
$ sesam status
Node config is up-to-date with local config.
$ sesam run
Run completed.
$ sesam update
Current output stored as expected output.
$ sesam verify
Verifying output...passed!
```

Or run the full test cycle (typical CI setup):

```
$ sesam test
Node config replaced with local config.
Run completed.
Verifying output (1/3)...passed!
Run completed.
Verifying output (2/3)...passed!
Run completed.
Verifying output (3/3)...passed!
```

## Configuring tests

| Property       | Description                                                                                            | Type | Required | Default                                                                           |
|----------------|--------------------------------------------------------------------------------------------------------|------------------|----------|-----------------------------------------------------------------------------------|
| _id            | Name of the test.                                                                                      | string | No | Name of the .test.json file                                                       |
| type           | Config type so that this later can just be part of the rest of the config.                             | String | No | test                                                                              |
| description    | A description of the test.                                                                             | string | No |                                                                                   |
| ignore         | If the output should be ignored during tests.                                                          | boolean | No | ``false``                                                                         |
| ignore_deletes | If the test should ignore deleted entities in the output.                                              | boolean | No | ``true``                                                                          |
| endpoint       | If the output should be fetched from a published endpoint instead.                                     | string | No | By default the json is grabbed from ``/pipes/<my-pipe>/entities``                 
| stage          | In which pipe stage to get the entities (source/before-transform/after-transform/sink)                 | string | No | By default the stage is ``sink``                                                  
| file           | File that contains the expected results.                                                               | string | No | Name of the .test.json file without .test (e.g. foo.test.json looks for foo.json) 
| pipe           | Pipe that contains the output to test.                                                                 | string | No | Same as above                                                                     |
| blacklist      | Properties to ignore in the output.                                                                    | Array of strings | No | ``[]``                                                                            |
| parameters     | Which parameters to pass as bound parameters. Note that parameters only works for published endpoints. | Object | No | ``{}``                                                                            |


Example:
```
$ cat foo.test.json
{
  "_id": "foo",
  "type": "test",
  "file": "foo.json"
  "blacklist": ["my-last-updated-ts"],
  "ignore": false
}
```

### DTL parameters

If you need to pass various variations of bound parameters to the DTL, you just create multiple .test.json files for each combination of parameters.

Example:
```
$ cat foo-A.test.json
{
  "pipe": "foo",
  "file": "foo-A.xml",
  "endpoint": "xml",
  "parameters": {
    "my-param": "A"
  }
}
$ cat foo-B.test.json
{
  "pipe": "foo",
  "file": "foo-B.xml",
  "endpoint": "xml",
  "parameters": {
    "my-param": "B"
  }
}
```
This will compare the output of ``/publishers/foo/xml?my-param=A`` with the contents of ``foo-A.xml`` and ``/publishers/foo/xml?my-param=B`` with the contents of ``foo-B.xml``.

### Internal properties

All internal properties except ``_id`` and ``_deleted`` are removed from the output. Entities that has ``_deleted`` set to ``false`` will have this property removed.

### Endpoints

By default the entities are fetched from ``/pipes/<my-pipe>/entities``, but if endpoint is set it will be fetched from
``/publishers/<my-pipe>/<endpoint-type>`` based on the endpoint type specified. Note that the pipe needs to be configured to publish to this endpoint.

Example:
```
{
  "_id": "foo",
  "type": "test",
  "endpoint": "xml",
  "file": "foo.xml"
}
```
This will compare the output of ``/publishers/foo/xml`` with the contents of ``foo.xml``.


Example:
```
{
  "_id": "foo",
  "type": "test",
  "endpoint": "json",
  "stage": "source"
}
```
This will compare the output of ``/pipes/foo/entities?stage=source`` with the contents of ``foo.json``, useful
when the pipe's sink strips away the "_id" property for example.


### Blacklisting

If the data contains values that are not deterministic (e.g. timestamp added during the run) they can be filtered out using the blacklist.

Example:
```
{
  "_id": "foo",
  "type": "test",
  "blacklist": ["foo", "ns1:bar"]
}
```

This will filter out properties called ``foo`` and ``ns1:bar`` (namespaced).

If the data is not located at the top level, a dotted notation is supported ``foo.bar``. This will remove the ``bar`` property from the object (or list of objects) located under the ``foo`` property. If you need to blacklist a property that actually contains a dot, the dot can be escaped like this ``foo\.bar``

If you need to ignore a property on a list of objects, you can also use this notation ``foos.*.bar``. This will remove the ``bar`` property from all the objects located under ``foos``.

Example:
```
{
  "_id": "foo",
  "foos": {
    "A": {
      "bar": "baz",
      "foobar": "foo"
    }
  }
}
```

Will end up as the following (with ``"blacklist": ["foos.*.bar"]``):
```
{
  "_id": "foo",
  "foos": {
    "A": {
      "foobar": "foo"
    }
  }
}
```

### Avoid ignore and blacklist

It is recommended to avoid ignoring or blacklisting as much as possible as this creates a false sense of correctness. Tests will pass, but deviations are silently ignored. A better solution is to avoid these properties in the output if possible.

### Uploading test data
There is a `sesam convert` command which takes all the pipes with conditional embedded sources, and modifies the case alternative which corresponds to the current profile env (usually "test") so that it is not an embedded source, but rather an http_endpoint source. At the same time, it takes the entities found in the original embedded source and stores them in separate files under a new `testdata` directory. This command should be necessary to run only once. It can take a `-dump` option that will first backup the entire config into a zip file.

When doing `sesam upload` or `sesam test`, the CLI will also upload testdata to any input pipes based on what it finds in a folder called `testdata`.

### Using Profiles
A profile file is a json file with the variables list as its content. The name has to follow "<profile>-env.json" convention.

Profile files are applicable to `upload`, `download`, `status` commands.

Default profile is 'test', thus, 'test-env.json' is the default profile file. It is expected to be at the same directory as the sesam CLI is executed from.
To use any other profile, create a profile file and use `-profile` argument.

e.g. Given that profile `profiles/prod-env.json` file exists, one can
  * upload with `sesam upload -profile profiles/prod`
  * download with `sesam download -profile profiles/prod`
  * see status with `sesam status -profile profiles/prod`


### Using whitelist
The optional argument `-whitelist-file` can effectively be used when running the following commands:
```
upload, verify, test
```
The parameter used with this argument should be the path to a whitelist file which will be used as a list of which pipes & systems to upload, as well which node-metadata file to use - or only verify certain pipes.

The whitelist file should be formatted as follows:
```
node-metadata.conf.json
pipes/input-pipe-1.conf.json
systems/email-system.conf.json
```

Please note the path separator, it should always be given as a forward slash - even if you're running on Windows.

Example: `sesam -whitelist-file whitelist.txt test`

### [Back to main page](./README.md)