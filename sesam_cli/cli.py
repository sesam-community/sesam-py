import os
import sys

from connector_cli import connectorpy

VALID_COMMANDS = [
    "authenticate",
    "expand",
    "validate",
    "upload",
    "download",
    "status",
    "init",
    "connector-init",
    "add-datatype",
    "update",
    "verify",
    "test",
    "run",
    "wipe",
    "restart",
    "reset",
    "dump",
    "stop",
    "convert",
    "update-schemas",
    "run-pytest",
    "format",
]

ALLOWED_NON_DEV_SUBSCRIPTION_COMMANDS = ["upload", "download"]


def execute_command(command, command_args, args, sesam_cmd_client, logger):
    if command == "authenticate":
        sesam_cmd_client.authenticate()
    elif command == "expand":
        connectorpy.expand_connector(args.system_placeholder, args.expanded_dir, args.profile)
    elif command == "validate":
        connectorpy.expand_connector(args.system_placeholder, args.expanded_dir, args.profile)
        sesam_cmd_client.validate()
    elif command == "upload":
        if not args.is_connector:
            sesam_cmd_client.upload()
        else:
            os.chdir(args.connector_dir)
            connectorpy.expand_connector(args.system_placeholder, args.expanded_dir, args.profile)
            sesam_cmd_client.validate()
            os.chdir(args.expanded_dir)
            sesam_cmd_client.upload()
            os.chdir(os.pardir) if args.connector_dir == "." else os.chdir(
                os.path.join(os.pardir, os.pardir)
            )
            if not args.skip_auth:
                sesam_cmd_client.authenticate()
    elif command == "download":
        sesam_cmd_client.download()
    elif command == "update-schemas":
        os.chdir(args.connector_dir)
        connectorpy.update_schemas(sesam_cmd_client.sesam_node)
    elif command == "status":
        if not args.is_connector:
            sesam_cmd_client.status()
        else:
            if os.path.exists(os.path.join(args.connector_dir, args.expanded_dir)):
                os.chdir(os.path.join(args.connector_dir, args.expanded_dir))
                sesam_cmd_client.status()
                os.chdir(os.pardir) if args.connector_dir == "." else os.chdir(
                    os.path.join(os.pardir, os.pardir)
                )
            else:
                logger.error(
                    "expanded directory not found. Please upload the "
                    "configs first or check the input args."
                )
    elif command == "init":
        sesam_cmd_client.init()
    elif command == "connector-init":
        sesam_cmd_client.connector_init()
    elif command == "add-datatype":
        sesam_cmd_client.add_datatype()
    elif command == "update":
        sesam_cmd_client.update()
    elif command == "verify":
        sesam_cmd_client.verify()
    elif command == "test":
        sesam_cmd_client.test()
    elif command == "stop":
        sesam_cmd_client.stop()
    elif command == "run":
        if args.enable_user_pipes is True:
            logger.warning(
                "Note that the -enable-user-pipes flag has no effect on the "
                "actual sesam instance outside the 'upload' or 'test' commands"
            )

        if args.disable_cpp_extensions is True:
            logger.warning(
                "Note that the -disable-cpp-extensions flag has no effect on "
                "the actual node configuration outside the 'upload' or "
                "'test' commands"
            )

        if args.enable_eager_ms is True:
            logger.warning(
                "Note that the -enable-eager-ms flag has no effect on the "
                "actual node configuration outside the 'upload' or 'test' "
                "commands"
            )

        sesam_cmd_client.run()
    elif command == "wipe":
        sesam_cmd_client.wipe()
    elif command == "restart":
        sesam_cmd_client.restart()
    elif command == "reset":
        sesam_cmd_client.reset()
    elif command == "convert":
        sesam_cmd_client.convert()
    elif command == "dump":
        sesam_cmd_client.dump()
    elif command == "run-pytest":
        if not command_args:
            logger.error(
                f"The name of the folder containing tests must be specified "
                f"when using the '{command}' command."
            )
            sys.exit(1)

        sesam_cmd_client.args.pytest_tests_folder = command_args[0]
        sesam_cmd_client.run_pytest_tests(is_standalone_run=True)
    elif command == "format":
        if not command_args:
            command_args = ["all"]
        sesam_cmd_client.format(command_args[0])
    else:
        logger.error("Unknown command: %s" % command)
        sys.exit(1)

    # Check if we should run pytest after the main command has finished
    if command != "run-pytest" and args.pytest_tests_folder:
        is_standalone_pytest_run = command != "test"
        sesam_cmd_client.run_pytest_tests(is_standalone_pytest_run)
