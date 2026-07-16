import json
import os
from glob import glob

from jsonformat import format_json


def execute_convert(args, logger, dump_callback):
    def has_conditional_embedded_source(pipe_config, env):
        source_config = pipe_config.get("source", {})
        source_type = source_config.get("type", "")
        if source_type == "conditional":
            alternatives = source_config.get("alternatives")
            current_profile_alternative = alternatives.get(env, {})
            if current_profile_alternative.get("type", "") == "embedded":
                return True
        return False

    def convert_pipe_config(pipe_config):
        entities = None
        modified_pipe_config = None
        if has_conditional_embedded_source(pipe_config, args.profile):
            alternatives = pipe_config["source"]["alternatives"]
            entities = alternatives[args.profile]["entities"]
            # rewrite the case which corresponds to env profile
            alternatives[args.profile] = {"type": "http_endpoint"}
            modified_pipe_config = pipe_config

        return modified_pipe_config, entities

    def save_testdata_file(pipe_id, entities):
        os.makedirs("testdata", exist_ok=True)
        with open(f"testdata{os.sep}{pipe_id}.json", "w", encoding="utf-8") as testdata_file:
            testdata_file.write(format_json(entities))

    def save_modified_pipe(pipe_json, path):
        with open(path, "w", encoding="utf-8") as pipe_file:
            pipe_file.write(format_json(pipe_json))

    logger.info("Starting converting conditional embedded sources")

    if args.dump:
        logger.info("Dumping config for backup")
        dump_callback()

    for filepath in glob("pipes%s*.conf.json" % os.sep):
        with open(filepath, "r", encoding="utf-8") as pipe_file:
            pipe = json.load(pipe_file)
            pipe_to_rewrite, entities = convert_pipe_config(pipe)

        if pipe_to_rewrite is not None:
            save_modified_pipe(pipe_to_rewrite, filepath)

        if entities is not None:
            save_testdata_file(pipe["_id"], entities)

    logger.info("Successfully converted pipes and created testdata folder")
