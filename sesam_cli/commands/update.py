import json

from lxml import etree


def execute_update(client):
    client.logger.info("Updating expected output from current output...")
    output_pipes = {}

    for pipe in client.sesam_node.get_output_pipes() + client.sesam_node.get_endpoint_pipes():
        output_pipes[pipe.id] = pipe

    test_specs = client.load_test_specs(output_pipes, update=True)

    if not test_specs:
        raise AssertionError("Found no tests (*.test.json) to update")

    updated = 0
    for pipe in output_pipes.values():
        if pipe.id in test_specs:
            if client.whitelisted_pipes and pipe.id not in client.whitelisted_pipes:
                client.logger.warning(
                    f"Skipping updating expected output for pipe '{pipe.id}' "
                    "- add it to the whitelist if this is not correct!"
                )
                continue

            client.logger.debug("Updating pipe '%s'.." % pipe.id)

            # Process all tests specs for this pipe
            for test_spec in test_specs[pipe.id]:
                if test_spec.ignore is True:
                    client.logger.debug(
                        "Skipping test spec '%s' because it was marked as 'ignore'" % test_spec.name
                    )
                    continue

                client.logger.debug(
                    "Updating spec '%s' for pipe '%s'.." % (test_spec.name, pipe.id)
                )
                if test_spec.endpoint == "json" or test_spec.endpoint == "excel":
                    # Get current entities from pipe in json form
                    current_output = client._fix_decimal_to_ints(
                        [
                            client.filter_entity(entity, test_spec)
                            for entity in client.sesam_node.get_pipe_entities(
                                pipe, stage=test_spec.stage
                            )
                        ]
                    )

                    if test_spec.ignore_deletes:
                        # Filter away any deletes from the current output
                        current_output = [
                            entity
                            for entity in current_output
                            if entity.get("_deleted", False) is False
                        ]

                    current_output = sorted(
                        current_output,
                        key=test_spec.get_entity_sorter_func(client.args.unicode_encoding),
                    )

                    current_output = (
                        json.dumps(
                            current_output,
                            indent="  ",
                            sort_keys=True,
                            ensure_ascii=client.args.unicode_encoding,
                        )
                        + "\n"
                    ).encode("utf-8")

                    if client.args.disable_json_html_escape is False:
                        current_output = current_output.replace(b"<", b"\\u003c")
                        current_output = current_output.replace(b">", b"\\u003e")
                        current_output = current_output.replace(b"&", b"\\u0026")

                elif test_spec.endpoint == "xml":
                    # Special case: download and format xml document as a string
                    xml_data = client.sesam_node.get_published_data(
                        pipe, "xml", params=test_spec.parameters, binary=True
                    )
                    xml_doc_root = etree.fromstring(xml_data)

                    xml_declaration, standalone = client.find_xml_header_settings(xml_data)

                    current_output = etree.tostring(
                        xml_doc_root,
                        encoding="utf-8",
                        xml_declaration=xml_declaration,
                        standalone=standalone,
                        pretty_print=True,
                    )
                else:
                    # Download contents as-is as a string
                    current_output = client.sesam_node.get_published_data(
                        pipe,
                        test_spec.endpoint,
                        params=test_spec.parameters,
                        binary=True,
                    )

                test_spec.update_expected_data(current_output)
                updated += 1

    client.logger.info("%s tests updated!" % updated)
