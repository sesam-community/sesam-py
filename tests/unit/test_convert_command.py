import json
import logging
import os
from pathlib import Path
from types import SimpleNamespace

from sesam_cli.commands.convert import execute_convert


def test_convert_rewrites_embedded_source_and_extracts_testdata(tmp_path: Path):
    pipes_dir = tmp_path / "pipes"
    pipes_dir.mkdir()
    pipe_path = pipes_dir / "input-pipe-1.conf.json"
    pipe_path.write_text(
        json.dumps(
            {
                "_id": "input-pipe-1",
                "type": "pipe",
                "source": {
                    "type": "conditional",
                    "alternatives": {
                        "prod": {"type": "dataset", "dataset": "prod-source"},
                        "test": {
                            "type": "embedded",
                            "entities": [{"_id": "1", "name": "A"}],
                        },
                    },
                    "condition": "test",
                },
            }
        ),
        encoding="utf-8",
    )

    args = SimpleNamespace(profile="test", dump=False)
    old_cwd = Path.cwd()
    try:
        # command expects to run from the config root
        os.chdir(tmp_path)
        execute_convert(args=args, logger=logging.getLogger("test"), dump_callback=lambda: None)
    finally:
        os.chdir(old_cwd)

    converted_pipe = json.loads(pipe_path.read_text(encoding="utf-8"))
    assert converted_pipe["source"]["alternatives"]["test"] == {"type": "http_endpoint"}

    testdata_path = tmp_path / "testdata" / "input-pipe-1.json"
    assert testdata_path.exists()
    assert json.loads(testdata_path.read_text(encoding="utf-8")) == [{"_id": "1", "name": "A"}]
