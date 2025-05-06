import logging

import sesam
from tests.args import Args

logger = logging.getLogger("sesam")

expected_output = {"test": "Some test config", "is_testing": "true"}


def test_parse_json_config():
    args = Args()
    cmdClient = sesam.SesamCmdClient(args, logger)
    parsed_config = cmdClient.parse_config_file(
        "./tests/test_funcs/test_parse_config_file/config.json"
    )

    assert isinstance(parsed_config, dict)
    assert parsed_config == expected_output


def test_parse_syncconfig():
    args = Args()

    cmdClient = sesam.SesamCmdClient(args, logger)
    parsed_config = cmdClient.parse_config_file(
        "./tests/test_funcs/test_parse_config_file/.syncconfig"
    )

    assert isinstance(parsed_config, dict)
    assert parsed_config == expected_output
