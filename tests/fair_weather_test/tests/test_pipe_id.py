import json
import glob
import os


def test_pipe_id():
    pipe_files_path = os.path.join(os.path.dirname(__file__), "..", "pipes", "*.conf.json")
    for pipe_path in glob.glob(pipe_files_path):
        filename = os.path.basename(pipe_path)
        assert "_" not in filename, (f"The file '{filename}' contains an underscore ('_') character - "
                                     f"this should not happen")
        with open(pipe_path) as infile:
            pipe_config = json.load(infile)
            pipe_id = pipe_config["_id"]

            assert "_" not in pipe_id, (f"The file '{filename}' contains a pipe id '{pipe_id}' that contain an "
                                        f"underscore ('_') character - this should not happen")
