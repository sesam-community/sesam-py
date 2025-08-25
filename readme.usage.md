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
client_id=<client id for OAuth2 authentication>
client_secret=<client secret for OAuth2 authentication>
account_id=<optional account_id override if not possible to get from the OAuth2 api>
```
for Tripletex authentication:
```bash
consumer_token=<client id for Tripletex authentication>
employee_token=<client secret for Tripletex authentication>
```

for API key authentication:
```bash
api_key=<api key>
```

* The secrets and environment variables will be updated in the node specified in the `.syncconfig` file.
* It is recommended to clone a connector repo and add the `.authconfig` file to the working directory.
* Optionally the secrets can also be passed as command line arguments specified below.
* To upload additional secrets and environment variables, add them to the profile file (e.g. test-env.json).
* Additional parameters and their values can be provided in a file `.additional_parameters.json` in the root directory 
of the connector. These parameters will be uploaded as environment variables to the node.
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

#### 4- jinja_vars (regular sesam configuration only)
In order to support custom template parameters in sesampy with json transit encoding, you can add a file named `.jinja_vars`
in the root directory, paste and edit the following:

```pythonverboseregexp
custom_param1=custom_value1
custom_param2=custom_value2
...
```

for example:
```pythonverboseregexp
connected_ts=2023-03-21T13:17:22Z
uid=12688f21-c4f5-481d-9b07-dd6a88b738f3
...
```
* Note that the value must be of the corresponding parameter datatype, though the value itself can be arbitrary.
It is then used to parse the json with transit encoding when running "upload" command. A reverse lookup takes place
when running "download" command, and the value is replaced with the corresponding parameter name. So it is recommended to 
use a near-unique value for each parameter to avoid collisions.
* The .jinja_vars file is only used when the sesam commands are running on a regular sesam configuration (no connector).

## Usage

```
usage: sesam [-h] [-version] [-v] [-vv] [-vvv] [-skip-tls-verification]
             [-sync-config-file <string>] [-whitelist-file <string>]
             [-dont-remove-scheduler] [-dump] [-print-scheduler-log]
             [-output-run-statistics] [-use-internal-scheduler] [-custom-scheduler]
             [-scheduler-image-tag <string>] [-scheduler-mode <string>] [-node <string>]
             [-scheduler-node <string>] [-jwt <string>] [-single <string>]
             [-no-large-int-bugs] [-disable-user-pipes] [-enable-eager-ms]
             [-enable-user-pipes] [-compact-execution-datasets] [-disable-cpp-extensions]
             [-unicode-encoding] [-disable-json-html-escape]
             [-upload-delete-sink-datasets] [-profile <string>] [-scheduler-id <string>]
             [-scheduler-request-mode <string>] [-scheduler-zero-runs <int>]
             [-scheduler-max-runs <int>] [-scheduler-max-run-time <int>]
             [-scheduler-check-input-pipes]
             [-scheduler-dont-reset-pipes-or-delete-sink-datasets]
             [-restart-timeout <int>] [-runs <int>] [-logformat <string>]
             [-scheduler-poll-frequency <int>] [-sesamconfig-file <string>] [-diff]
             [-add-test-entities] [-force-add] [-force] [-run-pytest <string>]
             [-pytest-args <string>] [-skip-auth] [--system-placeholder <string>]
             [-d <string>] [-e <string>] [--client_id <string>]
             [--client_secret <string>] [--account_id <string>] [--ignore-refresh-token]
             [--api_key <string>] [--service_url <string>] [--service_jwt <string>]
             [--consumer_token <string>] [--employee_token <string>]
             [--base_url <string>] [--days <string>] [--use-client-secret]
             [--do-float-as-decimal] [--auth <string>] [--datatype [<string>]] [--share]
             [command ...]

Commands:
  authenticate    Authenticates against the external service of the connector and updates secrets and environment variables (available only when working on a connector)
  wipe            Deletes all the pipes, systems, user datasets and environment variables in the node
  restart         Restarts the target node (typically used to release used resources if the environment is strained)
  reset           Deletes the entire node database and restarts the node (this is a more thorough version than "wipe" - requires the target node to be a designated developer node, contact support@sesam.io for help)
  init            Add conditional sources with testing and production alternatives to all input pipes in the local config.
  validate        Validate local config for proper formatting and internal consistency
  upload          Replace node config with local config. Also tries to upload testdata if 'testdata' folder present and updates secrets and environment variables when working on a connector (might ask for authentication).
  download        Replace local config with node config
  dump            Create a zip archive of the config and store it as 'sesam-config.zip'
  status          Compare node config with local config (requires external diff command)
  run             Run configuration until it stabilizes
  update          Store current output as expected output
  convert         Convert embedded sources in input pipes to http_endpoints and extract data into files
  verify          Compare output against expected output
  test            Upload, run and verify output
  stop            Stop any running schedulers (for example if the client was prematurely terminated or disconnected)
  update-schemas  Generate schemas for all datatypes (only works in connector development context)
  connector_init  Initialize a connector in the working directory with a sample manifest, template and system
  expand          Expand a connector without running other operations (upload or validate).
  run-pytest      Runs Python tests in the specified folder using the pytest framework. The folder must be placed on the same level as the pipes and systems.
  format          Formats pipes, systems, testdata, and expected files in the same way that the portal does, just offline now instead.

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
                        sync config file to use, the default is '.syncconfig' in the
                        current directory
  -whitelist-file <string>
                        whitelist file to use, the default is none
  -dont-remove-scheduler
                        don't remove scheduler after failure (DEPRECATED)
  -dump                 dump zip content to disk
  -print-scheduler-log  print scheduler log during run
  -output-run-statistics
                        output detailed pipe run statistics after scheduler run
  -use-internal-scheduler
                        use the built-in scheduler in sesam instead of a microservice
                        (DEPRECATED)
  -custom-scheduler     by default a scheduler system will be added, enable this flag if
                        you have configured a custom scheduler as part of the config
                        (DEPRECATED)
  -scheduler-image-tag <string>
                        the scheduler image tag to use (DEPRECATED)
  -scheduler-mode <string>
                        the scheduler mode to use ('active' or 'poll') - the default is
                        'active'
  -node <string>        service url
  -scheduler-node <string>
                        service url for scheduler
  -jwt <string>         authorization token
  -single <string>      update or verify just a single pipe
  -no-large-int-bugs    don't reproduce old large int bugs
  -disable-user-pipes   turn off user pipe scheduling in the target node (DEPRECATED)
  -enable-eager-ms      run all microservices even if they are not in use (note:
                        multinode only)
  -enable-user-pipes    turn on user pipe scheduling in the target node
  -compact-execution-datasets
                        compact all execution datasets when running scheduler
  -disable-cpp-extensions
                        turns off cpp extensions which saves dtl compile time at the
                        expense of possibly slower dtl exeution time
  -unicode-encoding     store the 'expected output' json files using unicode encoding
                        ('\uXXXX') - the default is UTF-8
  -disable-json-html-escape
                        turn off escaping of '<', '>' and '&' characters in 'expected
                        output' json files including 'sesam format expected'
  -upload-delete-sink-datasets
                        If specified with the 'upload' command, the 'upload' command will
                        delete all existing sink datasets before uploading the new
                        config. In some cases, this can be quicker than doing a 'sesam
                        wipe' or 'sesam reset' command when running ci-tests. The
                        downside is that there is a larger risk of data and/or config
                        from previous tests influencing the new test-run.
  -profile <string>     env profile to use <profile>-env.json
  -scheduler-id <string>
                        system id for the scheduler system (DEPRECATED)
  -scheduler-request-mode <string>
                        run the scheduler in 'sync' or 'async' mode, long running tests
                        should run in 'async' mode
  -scheduler-zero-runs <int>
                        the number of runs that has to yield zero changes for the
                        scheduler to finish
  -scheduler-max-runs <int>
                        maximum number of runs that scheduler can do to before exiting
                        (internal scheduler only)
  -scheduler-max-run-time <int>
                        the maximum time the internal scheduler is allowed to use to
                        finish (in seconds, internal scheduler only)
  -scheduler-check-input-pipes
                        controls whether failing input pipes should make the scheduler
                        run fail
  -scheduler-dont-reset-pipes-or-delete-sink-datasets
                        controls whether the scheduler should reset any pipes or delete
                        their sink-datasets
  -restart-timeout <int>
                        the maximum time to wait for the node to restart and become
                        available again (in seconds). The default is 15 minutes. A value
                        of 0 will skip the back-up-again verification.
  -runs <int>           number of test cycles to check for stability
  -logformat <string>   output format (normal, log or azure)
  -scheduler-poll-frequency <int>
                        milliseconds between each poll while waiting for the scheduler
  -sesamconfig-file <string>
                        sesamconfig file to use, the default is '.sesamconfig.json' in
                        the current directory
  -diff                 use with the status command to show the diff of the files
  -add-test-entities    use with the init command to add test entities to input pipes
  -force-add            use with the '-add-test-entities' option to overwrite test
                        entities that exist locally
  -force                force the command to run (only for 'upload' and 'download'
                        commands) for non-dev subscriptions
  -run-pytest <string>  specifies a folder containing Python tests that sesam-py should
                        run. These tests will run after the command (e.g. upload, run)
                        has finished. Uses the pytest framework. The folder should be
                        placed on the same level as 'pipes', 'systems' etc.
  -pytest-args <string>
                        specify the options that sesam-py should use when running pytest.
                        The arguments must be provided inside double quotes with each
                        argument separated by a space, e.g. -pytest-args="-vv -x"
  -skip-auth            skips the authentication step after upload command.
  --system-placeholder <string>
                        Name of the system _id placeholder (available only when working
                        on connectors)
  -d <string>           Connector folder to work with (available only when working on
                        connectors)
  -e <string>           Directory to expand the config into (available only when working
                        on connectors)
  --client_id <string>  OAuth2 client id (available only when working on connectors)
  --client_secret <string>
                        OAuth2 client secret (available only when working on connectors)
  --account_id <string>
                        OAuth2 account_id variable override (available only when working
                        on connectors)
  --ignore-refresh-token
                        use with sesam upload/authenticate to ignore refresh tokens for
                        systems that don't have them
  --api_key <string>    api_key secret (available only when working on connectors)
  --service_url <string>
                        url to service api (include /api) (available only when working on
                        connectors)
  --service_jwt <string>
                        jwt token to the service api (available only when working on
                        connectors)
  --consumer_token <string>
                        consumer token (available only when working on connectors)
  --employee_token <string>
                        employee token (available only when working on connectors)
  --base_url <string>   override to use prod env (available only when working on
                        connectors)
  --days <string>       number of days until the token should expire(available only when
                        working on connectors)
  --use-client-secret   use with sesam upload/authenticate to send add the client_secret
                        parameter to the /authorize URL
  --do-float-as-decimal
                        use with sesam upload/test to maintain full precision of decimals
                        instead of converting them to floats
  --auth <string>       auth scheme (oauth2, api_key, jwt)
  --datatype [<string>]
                        datatype to add
  --share               set this flag to enable sharing
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
* "upload" command is tied together with validate (before upload) and authenticate (after upload), 
so if you have a local config that does not pass the validation criteria, it will not be uploaded.
Above that, "upload" will set the necessary environment variables and secrets through the authentication process.
For the case of using webhook pipes, "upload" command sets the correct permissions for the pipe as well.

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
upload, verify, test, update
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
