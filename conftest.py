import pytest
import sesamclient
import json
import logging

logger = logging.getLogger("node_unit_tests")


def pytest_addoption(parser):
    parser.addoption('--additional_arguments', action='store', help='some helptext')


def pytest_configure(config):
    global args
    args_str = config.getoption('additional_arguments')
    try:
        args = json.loads(args_str)
    except BaseException as e:
        args = {}
        logger.error(f"Failed parsing arguments: {e}")


@pytest.fixture(scope='session')
def connection_fixture():
    # print(args)
    node_url = args.get('node_url')
    jwt = args.get('jwt')

    return sesamclient.Connection(sesamapi_base_url=node_url,
                                  jwt_auth_token=jwt,
                                  timeout=60 * 10,
                                  verify_ssl=True)
