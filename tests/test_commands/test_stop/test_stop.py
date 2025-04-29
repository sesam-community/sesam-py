import logging
from unittest import mock

import sesam
from tests.args import Args

logger = logging.getLogger("sesam")


@mock.patch("sesam.SesamNode.stop_internal_scheduler")
def test_stop(mock_connection):
    args = Args()

    node = sesam.SesamNode(args.node_url, args.jwt, logger)
    cmdClient = sesam.SesamCmdClient(args, logger)
    cmdClient.sesam_node = node

    # Simple test to check mocking
    mock_connection.return_value = {"message": "Mock resp"}
    out = cmdClient.stop()

    assert out == {"message": "Mock resp"}
