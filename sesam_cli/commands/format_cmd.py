import json
from glob import glob

from jsonformat import format_json


def execute_format(client, option):
    def _format_file(file, folder):
        with open(file, "r") as f:
            if folder == "expected":
                expected_in = json.loads(f.read())
                formatted = (
                    json.dumps(
                        expected_in,
                        indent="  ",
                        sort_keys=True,
                        ensure_ascii=client.args.unicode_encoding,
                    )
                    + "\n"
                )

                if client.args.disable_json_html_escape is False:
                    formatted = formatted.replace("<", "\\u003c")
                    formatted = formatted.replace(">", "\\u003e")
                    formatted = formatted.replace("&", "\\u0026")
            else:
                formatted = format_json(json.loads(f.read()))
        with open(file, "w") as f:
            f.writelines(formatted)

    options = {
        "all": {
            "glob": [
                "pipes/*.json",
                "testdata/*.json",
                "systems/*.json",
                "expected/*.json",
            ]
        },
        "pipes": {"glob": ["pipes/*.json"]},
        "testdata": {"glob": ["testdata/*.json"]},
        "systems": {"glob": ["systems/*.json"]},
        "expected": {"glob": ["expected/*.json"]},
    }

    if option not in options and not option.endswith(".json"):
        client.logger.info(
            f"[!] {option} is not a valid type to format... "
            "Try pipes, systems, testdata, or expected. "
            "Alternatively you can pass in a json file"
        )
        return

    if option.endswith(".json"):
        dirs = option.split("/")
        file_folder = ""
        for dir in dirs:
            if dir in options.keys():
                file_folder = dir
                break

        if not file_folder:
            file_folder = dirs[-1]

        if file_folder.endswith(".json"):
            client.logger.warning(
                "[!] Unknown directory for file, formatting as normal. "
                "If this file is expected data, please make sure it has "
                "the directory in the path."
            )

        client.logger.info(f"[*] Formatting {option}.")
        _format_file(option, file_folder)
        return

    for path in options[option]["glob"]:
        folder = path.split("/")[0]
        client.logger.info(f"[*] Formatting {folder} files. Search query is {path}")
        for file in glob(path):
            if folder == "expected" and ".test.json" in file:
                continue

            if client.args.extra_extra_verbose:
                client.logger.info(f"[+] Formatting {file}")
            _format_file(file, folder)
