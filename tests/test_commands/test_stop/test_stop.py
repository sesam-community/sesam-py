from unittest import mock

import sesam
from tests.args import Args


@mock.patch("sesam.SesamNode.stop_internal_scheduler")
def test_stop(mock_connection):
    args = Args()

    node = sesam.SesamNode(args.node_url, args.jwt, args.logger)
    cmdClient = sesam.SesamCmdClient(args, args.logger)
    cmdClient.sesam_node = node

    # Simple test to check mocking
    mock_connection.return_value = {"message": "Mock resp"}
    out = cmdClient.stop()

    assert out == {"message": "Mock resp"}
