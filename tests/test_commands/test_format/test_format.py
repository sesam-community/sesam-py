import json
import logging
import os
from unittest import mock

from jsonformat import format_json
from sesam import SesamCmdClient
from tests.args import Args

logger = logging.getLogger("sesam")


class TestFileCollection:
    # Kinda hacky, but best I could come up with rn
    gathered_files = list()

    def capture_files(self, file, folder):
        self.gathered_files.append(file)

    @mock.patch("sesam._format_file")
    def test_all(self, mock_format):
        os.chdir(os.path.dirname(os.path.realpath(__file__)))
        expected_files = ["pipes/hr-person.conf.json", "expected/hr-person.json"]
        mock_format.side_effect = self.capture_files

        args = Args()
        cmdClient = SesamCmdClient(args, logger)

        cmdClient.format("all")

        assert self.gathered_files == expected_files
        os.chdir("../../..")
        self.gathered_files.clear()

    @mock.patch("sesam._format_file")
    def test_pipes(self, mock_format):
        os.chdir(os.path.dirname(os.path.realpath(__file__)))
        expected_files = ["pipes/hr-person.conf.json"]
        mock_format.side_effect = self.capture_files

        args = Args()
        cmdClient = SesamCmdClient(args, logger)

        cmdClient.format("pipes")

        assert self.gathered_files == expected_files
        os.chdir("../../..")
        self.gathered_files.clear()

    @mock.patch("sesam._format_file")
    def test_expected(self, mock_format):
        os.chdir(os.path.dirname(os.path.realpath(__file__)))
        expected_files = ["expected/hr-person.json"]
        mock_format.side_effect = self.capture_files

        args = Args()
        cmdClient = SesamCmdClient(args, logger)

        cmdClient.format("expected")

        assert self.gathered_files == expected_files
        os.chdir("../../..")
        self.gathered_files.clear()


def test_formatter():
    os.chdir(os.path.dirname(os.path.realpath(__file__)))
    expected_json = '{\n  "_id": "hr-person",\n  "type": "pipe",\n  "source": {\n    "type": "embedded",\n    "entities": [{\n      "_id": "23072451376",\n      "Country": "NO",\n      "EmailAddress": "TorjusSand@einrot.com",\n      "Gender": "male",\n      "GivenName": "Torjus",\n      "MiddleInitial": "M",\n      "Number": "1",\n      "SSN": "23072451376",\n      "StreetAddress": "Helmers vei 242",\n      "Surname": "Sand",\n      "Title": "Mr.",\n      "Username": "Unjudosely",\n      "ZipCode": "5031"\n    }, {\n      "_id": "09046987892",\n      "Country": "NO",\n      "EmailAddress": "LarsEvjen@rhyta.com",\n      "Gender": "male",\n      "GivenName": "Lars",\n      "Number": "2",\n      "SSN": "09046987892",\n      "StreetAddress": "Frognerveien 60",\n      "Surname": "Evjen",\n      "Title": "Mr.",\n      "Username": "Wimen1979",\n      "ZipCode": "3121"\n    }]\n  },\n  "transform": {\n    "type": "dtl",\n    "rules": {\n      "default": [\n        ["copy", "*"],\n        ["add", "rdf:type",\n          ["ni", "hr", "person"]\n        ]\n      ]\n    }\n  }\n}\n'  # noqa: E501
    with open("./pipes/hr-person.conf.json") as f:
        input_json = json.loads(f.read())

    formatted_json = format_json(input_json)
    assert formatted_json == expected_json

    # Switch back to the root of the project so the other tests pass
    os.chdir("../../..")
