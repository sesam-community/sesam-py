import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from sesam_cli.runtime.performance import profile_phase


def execute_upload(client, upload_exception_class, testdata_upload_exception_class):
    # Find env vars to upload
    profile_file = "%s-env.json" % client.args.profile
    with profile_phase(client, "upload.read_profile"):
        try:
            with open(profile_file, "r", encoding="utf-8-sig") as fp:
                json_data = json.load(fp)
        except FileNotFoundError as e:
            client.logger.error("Cannot locate profile file '%s'" % profile_file)
            raise e
        except BaseException as e:
            client.logger.error("Failed to parse profile: '%s'" % profile_file)
            raise e

    if client.args.upload_delete_sink_datasets:
        with profile_phase(client, "upload.delete_sink_datasets"):
            try:
                delete_datasets_starttime = time.monotonic()
                client.logger.info("Deleting all sink datasets...")
                response = client.sesam_node.api_connection.delete_all_sink_datasets()
                delete_datasets_elapsed_time = time.monotonic() - delete_datasets_starttime
                client.logger.info(
                    f"Finished deleting all sink datasets. "
                    f"elapsed_time={delete_datasets_elapsed_time:.1f}s."
                    f"response={response}"
                )
            except BaseException as e:
                client.logger.error("Failed to delete the sink datasets")
                raise e

    additional_parameters_path = os.path.join(os.pardir, ".additional_parameters.json")
    with profile_phase(client, "upload.load_additional_parameters"):
        if os.path.isfile(additional_parameters_path):
            try:
                with open(additional_parameters_path, encoding="utf-8") as f:
                    additional_parameters = json.load(f)
            except BaseException as e:
                client.logger.error("Failed to parse additional parameters file")
                raise e
        else:
            additional_parameters = {}

    for param, value in additional_parameters.items():
        if param in json_data.keys():
            client.logger.warning(
                f"Value for parameter '{param}' set in {profile_file} will be replaced with "
                f"the corresponding value set in {additional_parameters_path} before upload."
            )
        json_data[param] = value

    with profile_phase(client, "upload.put_env"):
        try:
            client.sesam_node.put_env(json_data)
        except BaseException as e:
            client.logger.error("Failed to replace environment variables in Sesam")
            raise e

    # Zip the relevant directories and upload to Sesam
    with profile_phase(client, "upload.prepare_zip"):
        try:
            zip_config = client.get_zip_config(remove_zip=client.args.dump is False)

            # Modify the node-metadata.conf.json to stop the pipe scheduler
            if os.path.isfile("node-metadata.conf.json"):
                with open("node-metadata.conf.json", "r", encoding="utf-8") as infile:
                    node_metadata = json.load(infile)
            else:
                node_metadata = {}

            node_metadata.setdefault("task_manager", {})["disable_user_pipes"] = (
                not client.args.enable_user_pipes
            )

            global_defaults = node_metadata.setdefault("global_defaults", {})
            if client.args.disable_cpp_extensions:
                global_defaults["enable_cpp_extensions"] = False
            global_defaults["eager_load_microservices"] = bool(client.args.enable_eager_ms)

            zip_config = client.replace_file_in_zipfile(
                zip_config,
                "node-metadata.conf.json",
                json.dumps(node_metadata).encode("utf-8"),
            )
        except BaseException as e:
            client.logger.error("Failed to create zip archive of config")
            raise e

    with profile_phase(client, "upload.put_config"):
        try:
            client.sesam_node.put_config(zip_config, force=client.args.force)
        except BaseException as e:
            client.logger.error("Failed to upload config to sesam")
            if hasattr(e, "parsed_response"):
                raise upload_exception_class(e.parsed_response.get("validation_errors", []))
            else:
                raise e

    with profile_phase(client, "upload.wait_for_deploy"):
        client.sesam_node.wait_for_all_pipes_to_deploy()

    client.logger.info("Config uploaded successfully")

    if os.path.isdir("testdata"):
        with profile_phase(client, "upload.scan_testdata_jobs"):
            testdata_jobs = list(client.get_testdata_jobs())
        total_jobs = len(testdata_jobs)
        if total_jobs == 0:
            client.logger.info("No test data found to upload")
            return

        uploaded = 0
        failed = 0
        started_at = time.monotonic()

        if not client.args.single_thread_upload:
            max_workers = min(client.args.upload_workers, total_jobs)
            if max_workers < 1:
                max_workers = 1

            failures = []
            with profile_phase(client, "upload.parallel_testdata_upload"):
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_job = {
                        executor.submit(client.upload_testdata, root, filename, pipe_id): (
                            root,
                            filename,
                            pipe_id,
                        )
                        for root, filename, pipe_id in testdata_jobs
                    }
                    for future in as_completed(future_to_job):
                        root, filename, pipe_id = future_to_job[future]
                        try:
                            future.result()
                            uploaded += 1
                        except BaseException as e:
                            failed += 1
                            failures.append(
                                {
                                    "path": os.path.join(root, filename),
                                    "pipe_id": pipe_id,
                                    "error": str(e),
                                }
                            )
                        finally:
                            client.log_testdata_upload_progress(uploaded, failed, total_jobs)

            client.log_testdata_upload_summary(uploaded, failed, total_jobs, started_at)
            if failures:
                raise testdata_upload_exception_class(failures)

            client.logger.info(
                "Test data uploaded successfully. Waiting 5 seconds before proceeding..."
            )
        else:
            with profile_phase(client, "upload.single_thread_testdata_upload"):
                try:
                    for root, filename, pipe_id in testdata_jobs:
                        try:
                            client.upload_testdata(root, filename, pipe_id)
                            uploaded += 1
                        except BaseException:
                            failed += 1
                            raise
                        finally:
                            client.log_testdata_upload_progress(uploaded, failed, total_jobs)
                finally:
                    client.log_testdata_upload_summary(uploaded, failed, total_jobs, started_at)
    else:
        client.logger.info("No test data found to upload")
