import logging

import sesam
from tests.args import Args

logger = logging.getLogger("sesam")


def test_parse_json_config():
    args = Args()
    cmdClient = sesam.SesamCmdClient(args, logger)
    parsed_config = cmdClient.parse_config_file(
        "./tests/test_funcs/test_parse_config_file/config.json"
    )

    assert type(parsed_config) == dict
    assert parsed_config.get("test", "") == "Some test config"
    assert parsed_config.get("is_testing", "") == "true"


def test_parse_syncconfig():
    args = Args()

    cmdClient = sesam.SesamCmdClient(args, logger)
    parsed_config = cmdClient.parse_config_file(
        "./tests/test_funcs/test_parse_config_file/.syncconfig"
    )

    assert type(parsed_config) == dict
    assert parsed_config.get("test", "") == "Some test config"
    assert parsed_config.get("is_testing", "") == "true"
