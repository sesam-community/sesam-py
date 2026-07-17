import json
import os
import sys


def execute_validate(client):
    logger = client.logger
    logger.info("Validating config files")
    # set the current directory when sesam validate is called from root.
    if client.args.command == "validate" and client.args.connector_dir != ".":
        os.chdir(client.args.connector_dir)

    is_valid = client.check_template_sink()

    if os.path.exists(".expanded"):
        for root, _, files in os.walk(".expanded"):
            if root.endswith("/.expanded"):
                for file in files:
                    if file.endswith(".json"):
                        try:
                            with open(os.path.join(root, file), "r") as f:
                                json.load(f)
                        except BaseException:
                            logger.error("Config file '%s' is not valid json" % file)
                            is_valid = False
            elif root.endswith("/systems"):
                for file in files:
                    if file.endswith(".json"):
                        try:
                            with open(os.path.join(root, file), "r") as f:
                                json.load(f)
                        except BaseException:
                            logger.error("Config file '/systems/%s' is not valid json" % file)
                            is_valid = False
            elif root.endswith("/pipes"):

                def extract_datatype(file):
                    return file.split("-")[1]

                # preprocess all files to know which collect pipes has a corresponding share
                shared_datatypes = set()
                for file in files:
                    if file.endswith(".json"):
                        if "share" in file:
                            shared_datatypes.add(extract_datatype(file))

                for file in files:
                    if file.endswith(".json"):
                        datatype = extract_datatype(file)
                        try:
                            with open(os.path.join(root, file), "r") as f:
                                config = json.load(f)
                        except BaseException:
                            logger.error("Config file '/pipes/%s' is not valid json" % file)
                            is_valid = False
                            continue

                        # TODO: change the validation for detecting warnings before
                        # expanding the config files. This could lead to unexpected
                        # behaviour.
                        if "WARNING" in config.get("description", ""):
                            logger.error(
                                f"Config file '/pipes/{file}' has a WARNING "
                                "in the description."
                            )
                            is_valid = False

                        if "collect" in file and datatype in shared_datatypes:
                            found = False
                            # TODO: handle if we have a chained transform
                            if type(config.get("transform")) == list:
                                for transform in config.get("transform"):
                                    if transform.get("template") == "transform-collect-rest":
                                        found = True
                            elif type(config.get("transform")) == dict:
                                if (
                                    config.get("transform").get("template")
                                    == "transform-collect-rest"
                                ):
                                    found = True
                            if not found:
                                logger.error(
                                    f"Config file '/pipes/{file}' has a corresponding share "
                                    "pipe but is missing the 'transform-collect-rest' transform"
                                )
                                is_valid = False

                        if "collect" in file and type(config.get("transform")) == list:
                            for transform in config.get("transform"):
                                share_dataset = transform.get("properties", {}).get(
                                    "share_dataset", {}
                                )
                                if transform.get("template") == "transform-collect-rest":
                                    if "exclude_completeness" not in config.keys():
                                        logger.error(
                                            f"Config file '/pipes/{file}' is "
                                            "missing 'exclude_completeness' "
                                            "property"
                                        )
                                        is_valid = False
                                    elif not transform.get("properties"):
                                        logger.error(
                                            f"Config file '/pipes/{file}' is "
                                            "missing 'properties' property"
                                        )
                                        is_valid = False
                                    elif not share_dataset:
                                        logger.error(
                                            f"Config file '/pipes/{file}' is "
                                            "missing 'share_dataset' property in "
                                            "'properties'"
                                        )
                                        is_valid = False
                                    elif share_dataset not in config.get("exclude_completeness"):
                                        logger.error(
                                            f"Config file '/pipes/{file}' is "
                                            "missing "
                                            f"'{share_dataset}' in "
                                            "'exclude_completeness'"
                                        )
                                        is_valid = False

                        if "share" in file:
                            if type(config.get("transform")) == dict:
                                if (
                                    config.get("transform").get("template")
                                    == "transform-share-rest"
                                ):
                                    if (
                                        "batch_size" not in config.keys()
                                        or config.get("batch_size") != 1
                                    ):
                                        logger.error(
                                            f"Config file '{file}' is missing "
                                            "'batch_size' property with value: 1"
                                        )
                                        is_valid = False
                            elif type(config.get("transform")) == list:
                                for transform in config.get("transform"):
                                    if transform.get("template") == "transform-share-rest":
                                        if (
                                            "batch_size" not in config.keys()
                                            or config.get("batch_size") != 1
                                        ):
                                            logger.error(
                                                f"Config file '{file}' is missing "
                                                "'batch_size' property with "
                                                "value: 1"
                                            )
                                            is_valid = False
        if is_valid:
            logger.warning("All config files are valid")
        else:
            logger.error(
                "One or more config files are not valid. " "Check the log for more information"
            )
            sys.exit(1)
    else:
        logger.error("Failed to validate. Config files are not expanded.")
        sys.exit(1)
