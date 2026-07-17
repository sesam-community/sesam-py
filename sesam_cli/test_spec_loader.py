import os
from glob import glob

from sesam_cli.test_specs import TestSpec


def load_test_specs(client, existing_output_pipes, update=False):
    test_specs = {}
    failed = False

    # Load test specifications
    for filename in glob("expected%s*.test.json" % os.sep):
        client.logger.debug("Processing spec file '%s'" % filename)

        test_spec = TestSpec(filename)

        pipe_id = test_spec.pipe
        client.logger.log(
            client.loglevel_trace, "Pipe id for spec '%s' is '%s" % (filename, pipe_id)
        )

        if client.whitelisted_pipes and pipe_id not in client.whitelisted_pipes:
            client.logger.warning(
                f"Skipping test spec for non-whitelisted pipe '{pipe_id} - "
                "add it to the whitelist if "
                f"this is not correct!'"
            )
            continue

        if pipe_id not in existing_output_pipes:
            if update is False:
                client.logger.error(
                    "Test spec '%s' references a non-exisiting output "
                    "pipe '%s' - please remove '%s'"
                    % (test_spec.spec_file, pipe_id, test_spec.spec_file)
                )
                failed = True
            else:
                if test_spec.ignore is False:
                    # Remove the test spec file
                    if os.path.isfile("%s" % test_spec.spec_file):
                        client.logger.warning(
                            "Test spec '%s' references a non-exisiting output "
                            "pipe '%s' - removing '%s'.."
                            % (test_spec.spec_file, pipe_id, test_spec.spec_file)
                        )
                        os.remove(test_spec.spec_file)
                        continue
                else:
                    client.logger.warning(
                        "Test spec '%s' references a non-exisiting output "
                        "pipe '%s' but is marked as 'ignore' - consider "
                        "removing '%s'.." % (test_spec.spec_file, pipe_id, test_spec.spec_file)
                    )

        if test_spec.ignore is False and not os.path.isfile("%s" % test_spec.file):
            client.logger.warning(
                "Test spec '%s' references non-exisiting 'expected' output "
                "file '%s'" % (test_spec.spec_file, test_spec.file)
            )
            if update is True:
                client.logger.info("Creating empty 'expected' output file '%s'..." % test_spec.file)
                with open(test_spec.file, "w") as fp:
                    fp.write("[]\n")
            else:
                failed = True

        # If spec says 'ignore' then the corresponding output file should not exist
        if failed is False and test_spec.ignore is True:
            output_filename = test_spec.file

            if os.path.isfile(output_filename):
                if update:
                    client.logger.debug("Removing existing output file '%s'" % output_filename)
                    os.remove(output_filename)
                else:
                    client.logger.warning(
                        "pipe '%s' is ignored, but output file '%s' still exists"
                        % (pipe_id, filename)
                    )

        if pipe_id not in test_specs:
            test_specs[pipe_id] = []

        test_specs[pipe_id].append(test_spec)

    if failed:
        client.logger.error("Test specs verify failed, correct errors and retry")
        raise RuntimeError("Test specs verify failed, correct errors and retry")

    if update:
        for pipe in existing_output_pipes.values():
            if client.whitelisted_pipes and pipe.id not in client.whitelisted_pipes:
                client.logger.warning(
                    f"Not updating non-whitelisted pipe '{pipe.id} - add "
                    "it to the whitelist if "
                    f"this is not correct!'"
                )
                continue

            client.logger.debug("Updating pipe '%s" % pipe.id)

            if pipe.id not in test_specs:
                client.logger.warning(
                    "Found no spec for pipe %s - creating empty spec file" % pipe.id
                )

                filename = os.path.join("expected", "%s.test.json" % pipe.id)
                with open(filename, "w") as fp:
                    fp.write("{\n}")
                test_specs[pipe.id] = [TestSpec(filename)]

    return test_specs
