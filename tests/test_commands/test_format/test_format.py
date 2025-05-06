import json
import logging
import os
from unittest import mock

from jsonformat import format_json
from sesam import SesamCmdClient
from tests.args import Args

logger = logging.getLogger("sesam")


# Need to add more files to test with and test the specific options
@mock.patch("sesam._format_file")
def test_file_collector(mock_format):
    # Switch to the test_format dir
    os.chdir(os.path.dirname(os.path.realpath(__file__)))

    expected_files = ["pipes/hr-person.conf.json", "expected/hr-person.json"]
    gathered_files = list()

    def capture_files(file, folder):
        gathered_files.append(file)

    mock_format.side_effect = capture_files
    args = Args()
    cmdClient = SesamCmdClient(args, logger)

    cmdClient.format("all")

    assert gathered_files == expected_files


def test_formatter():
    # Formatting is the same for all files except expected files,
    # and the formatting for those is a basic sort_keys through json.dumps

    # ugly af string, but it's what is returned from the function before it's
    # written to the file.
    expected_json = '{\n  "_id": "hr-person",\n  "type": "pipe",\n  "source": {\n    "type": "embedded",\n    "entities": [{\n      "_id": "23072451376",\n      "Country": "NO",\n      "EmailAddress": "TorjusSand@einrot.com",\n      "Gender": "male",\n      "GivenName": "Torjus",\n      "MiddleInitial": "M",\n      "Number": "1",\n      "SSN": "23072451376",\n      "StreetAddress": "Helmers vei 242",\n      "Surname": "Sand",\n      "Title": "Mr.",\n      "Username": "Unjudosely",\n      "ZipCode": "5031"\n    }, {\n      "_id": "09046987892",\n      "Country": "NO",\n      "EmailAddress": "LarsEvjen@rhyta.com",\n      "Gender": "male",\n      "GivenName": "Lars",\n      "Number": "2",\n      "SSN": "09046987892",\n      "StreetAddress": "Frognerveien 60",\n      "Surname": "Evjen",\n      "Title": "Mr.",\n      "Username": "Wimen1979",\n      "ZipCode": "3121"\n    }]\n  },\n  "transform": {\n    "type": "dtl",\n    "rules": {\n      "default": [\n        ["copy", "*"],\n        ["add", "rdf:type",\n          ["ni", "hr", "person"]\n        ]\n      ]\n    }\n  }\n}\n'  # noqa: E501
    with open("./pipes/hr-person.conf.json") as f:
        input_json = json.loads(f.read())

    formatted_json = format_json(input_json)
    assert formatted_json == expected_json

    # Switch back to the root of the project so the other tests pass
    os.chdir("../../..")
