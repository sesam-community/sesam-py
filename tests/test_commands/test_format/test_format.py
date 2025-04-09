import logging

from sesam import SesamCmdClient

logger = logging.getLogger("sesam")


class Args:
    def __init__(self):
        self.whitelist_file = None


def test_all():
    args = Args()
    cmdClient = SesamCmdClient(args, logger)

    cmdClient.format("all")
