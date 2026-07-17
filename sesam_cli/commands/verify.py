import json
import os
from copy import deepcopy
from pprint import pformat


def execute_verify(client, base_dir, git_root):
    client.logger.info("Verifying that expected output matches current output...")
    output_pipes = {}
    failed = False

    for pipe in client.sesam_node.get_output_pipes() + client.sesam_node.get_endpoint_pipes():
        if pipe.runtime.get("is-valid-config", False) is False:
            client.logger.error("The pipe '%s' has invalid config, cannot verify pipe!" % pipe.id)
            client.logger.error(
                "The error(s) reported was: %s" % pipe.runtime.get("config-errors", "unknown")
            )
        else:
            output_pipes[pipe.id] = pipe

    test_specs = client.load_test_specs(output_pipes)

    if not test_specs:
        # IS-8560: no test files should result in a warning, not an error
        client.logger.warning("Found no tests (*.test.json) to run")
        return

    failed_tests = []
    missing_tests = []
    for pipe in output_pipes.values():
        if client.whitelisted_pipes and pipe.id not in client.whitelisted_pipes:
            client.logger.warning(
                f"Skipping verify for pipe '{pipe.id}' - add it to the whitelist "
                "if this is not "
                f"correct!"
            )
            continue

        client.logger.debug("Verifying pipe '%s'.." % pipe.id)

        if pipe.id in test_specs:
            # Verify all tests specs for this pipe
            for test_spec in test_specs[pipe.id]:
                if test_spec.ignore is True:
                    client.logger.debug(
                        "Skipping test spec '%s' because it was marked as 'ignore'" % test_spec.name
                    )
                    continue

                entity_sorter_func = test_spec.get_entity_sorter_func(client.args.unicode_encoding)

                if test_spec.endpoint == "json" or test_spec.endpoint == "excel":
                    # Get current entities from pipe in json form
                    expected_entities = test_spec.expected_entities

                    expected_output = sorted(
                        expected_entities,
                        key=entity_sorter_func,
                    )

                    if test_spec.ignore_deletes:
                        # Gather any expected deletes
                        expected_deletes = [
                            _e["_id"]
                            for _e in expected_entities
                            if _e.get("_deleted", False) is True
                        ]

                        # Filter away any unexpected deleted from the current output
                        current_entities = []
                        for entity in [
                            client.filter_entity(_e, test_spec)
                            for _e in client.sesam_node.get_pipe_entities(
                                pipe, stage=test_spec.stage
                            )
                        ]:
                            if (
                                entity.get("_deleted", False) is True
                                and entity["_id"] not in expected_deletes
                            ):
                                continue
                            current_entities.append(entity)
                    else:
                        current_entities = [
                            client.filter_entity(_e, test_spec)
                            for _e in client.sesam_node.get_pipe_entities(
                                pipe, stage=test_spec.stage
                            )
                        ]

                    current_output = sorted(current_entities, key=entity_sorter_func)

                    fixed_current_output = client._fix_decimal_to_ints(deepcopy(current_output))

                    fixed_current_output = sorted(
                        fixed_current_output,
                        key=entity_sorter_func,
                    )

                    if len(fixed_current_output) != len(expected_output):
                        file_path = os.path.join(
                            os.path.relpath(base_dir, git_root), test_spec.file
                        )
                        msg = (
                            "Pipe verify failed! Length mismatch for "
                            "test spec '%s': "
                            "expected %d got %d"
                            % (
                                test_spec.spec_file,
                                len(expected_output),
                                len(fixed_current_output),
                            )
                        )
                        client.logger.error(msg, {"file_path": file_path})

                        client.logger.info("Expected output:\n%s", pformat(expected_output))

                        if client.args.extra_extra_verbose:
                            client.logger.info(
                                "Got raw output:\n%s",
                                pformat(current_output),
                            )

                        client.logger.info("Got output:\n%s", pformat(fixed_current_output))

                        diff = client.get_diff_string(
                            json.dumps(
                                expected_output,
                                indent=2,
                                ensure_ascii=False,
                                sort_keys=True,
                            ),
                            json.dumps(
                                fixed_current_output,
                                indent=2,
                                ensure_ascii=False,
                                sort_keys=True,
                            ),
                            test_spec.file,
                            "current-output.json",
                        )
                        client.logger.info("Diff:\n%s" % diff)
                        failed_tests.append(test_spec)
                        failed = True
                    else:
                        expected_json = json.dumps(
                            expected_output,
                            ensure_ascii=False,
                            indent=2,
                            sort_keys=True,
                        )
                        current_json = json.dumps(
                            fixed_current_output,
                            ensure_ascii=False,
                            indent=2,
                            sort_keys=True,
                        )

                        if expected_json != current_json:
                            file_path = os.path.join(
                                os.path.relpath(base_dir, git_root), test_spec.file
                            )
                            client.logger.error(
                                "Pipe verify failed! "
                                "Content mismatch for test spec '%s'" % test_spec.file,
                                {"file_path": file_path},
                            )

                            client.logger.info("Expected output:\n%s" % pformat(expected_output))

                            if client.args.extra_extra_verbose:
                                client.logger.info("Expected output JSON:\n%s" % expected_json)
                                client.logger.info("Got raw output:\n%s" % pformat(current_output))
                                client.logger.info("Got output JSON:\n%s" % current_json)

                            client.logger.info("Got output:\n%s" % pformat(fixed_current_output))

                            diff = client.get_diff_string(
                                expected_json,
                                current_json,
                                test_spec.file,
                                "current-output.json",
                            )

                            client.logger.info("Diff:\n%s" % diff)
                            failed_tests.append(test_spec)
                            failed = True

                elif test_spec.endpoint == "xml":
                    # Special case: download and format xml document as a string
                    client.logger.debug("Comparing XML output..")
                    expected_output = test_spec.expected_data
                    current_output = client.sesam_node.get_published_data(
                        pipe, "xml", params=test_spec.parameters, binary=True
                    )

                    try:
                        expected_output = client.bytes_to_xml_string(expected_output)
                        current_output = client.bytes_to_xml_string(current_output)

                        if expected_output != current_output:
                            failed_tests.append(test_spec)
                            failed = True

                            client.logger.info(
                                "Pipe verify failed! Content mismatch:\n%s"
                                % client.get_diff_string(
                                    expected_output,
                                    current_output,
                                    test_spec.file,
                                    "current_data.xml",
                                )
                            )

                    except BaseException:
                        # Unable to parse expected/current as XML; compare byte-by-byte.
                        client.logger.debug(
                            "Failed to parse expected output and/or current output as XML"
                        )
                        client.logger.debug(
                            "Falling back to byte-level comparison. Note that this might generate "
                            "false differences for XML data."
                        )

                        if expected_output != current_output:
                            failed_tests.append(test_spec)
                            failed = True
                            client.logger.error("Pipe verify failed! Content mismatch!")
                else:
                    # Download contents as-is as a byte buffer
                    expected_output = test_spec.expected_data
                    current_output = client.sesam_node.get_published_data(
                        pipe,
                        test_spec.endpoint,
                        params=test_spec.parameters,
                        binary=True,
                    )

                    if expected_output != current_output:
                        failed_tests.append(test_spec)
                        failed = True

                        # Try to show diff - first try utf-8 encoding
                        try:
                            expected_output = str(expected_output, encoding="utf-8")
                            current_output = str(current_output, encoding="utf-8")
                        except UnicodeDecodeError:
                            try:
                                expected_output = str(expected_output, encoding="latin-1")
                                current_output = str(current_output, encoding="latin-1")
                            except UnicodeDecodeError:
                                client.logger.error("Pipe verify failed! Content mismatch!")
                                client.logger.warning(
                                    "Unable to read expected and/or output data as "
                                    "unicode text so I can't show diff"
                                )
                                continue

                        client.logger.error(
                            "Pipe verify failed! Content mismatch:\n%s"
                            % client.get_diff_string(
                                expected_output,
                                current_output,
                                test_spec.file,
                                "current_data.txt",
                            )
                        )
        else:
            client.logger.error("No tests references pipe '%s'" % pipe.id)
            missing_tests.append(pipe.id)
            failed = True

    if failed:
        if len(failed_tests) > 0:
            client.logger.error(
                "Failed %s of %s tests!" % (len(failed_tests), len(list(test_specs.keys())))
            )
            client.logger.error("Failed pipe id (spec file):")
            for failed_test_spec in failed_tests:
                client.logger.error(
                    "%s (%s)" % (failed_test_spec.pipe, failed_test_spec.spec_file)
                )

        if len(missing_tests) > 0:
            client.logger.error("Missing %s tests!" % len(missing_tests))
            client.logger.error("Missing test for pipe:")
            for pipe_id in missing_tests:
                client.logger.error(pipe_id)

        raise RuntimeError("Verify failed")
    else:
        client.logger.info("All tests passed! Ran %s tests." % len(list(test_specs.keys())))
