import json
import os


def remove_task_manager_settings(client, zip_data):
    node_metadata = {}
    if os.path.isfile("node-metadata.conf.json"):
        with open("node-metadata.conf.json", "r") as infile:
            node_metadata = json.load(infile)

    remote_data = client.get_zipfile_data_by_filename(zip_data, "node-metadata.conf.json")
    if remote_data:
        remote_metadata = json.loads(str(remote_data, encoding="utf-8"))

        if (
            "task_manager" in remote_metadata
            and "disable_user_pipes" in remote_metadata["task_manager"]
            and remote_metadata["task_manager"]["disable_user_pipes"] is True
        ):
            if "disable_user_pipes" in node_metadata.get("task_manager", {}):
                # Restore the original, if present
                remote_metadata["task_manager"]["disable_user_pipes"] = node_metadata[
                    "task_manager"
                ]["disable_user_pipes"]
            else:
                # Not present originally, so just remove it from remote
                remote_metadata["task_manager"].pop("disable_user_pipes")
                # Remove the entire task_manager section if its empty
                if len(remote_metadata["task_manager"]) == 0:
                    remote_metadata.pop("task_manager")

        if "global_defaults" in remote_metadata:
            if (
                "enable_cpp_extensions" in remote_metadata["global_defaults"]
                and remote_metadata["global_defaults"]["enable_cpp_extensions"] is False
            ):
                if "enable_cpp_extensions" in node_metadata.get("global_defaults", {}):
                    # Restore the original, if present
                    remote_metadata["global_defaults"]["enable_cpp_extensions"] = node_metadata[
                        "global_defaults"
                    ]["enable_cpp_extensions"]
                else:
                    # Not present originally, so just remove it from remote
                    remote_metadata["global_defaults"].pop("enable_cpp_extensions")
                    # Remove the entire global_defaults section if its empty
                    if len(remote_metadata["global_defaults"]) == 0:
                        remote_metadata.pop("global_defaults")

            if (
                "eager_load_microservices" in remote_metadata["global_defaults"]
                and remote_metadata["global_defaults"]["eager_load_microservices"] is False
            ):
                if "eager_load_microservices" in node_metadata.get("global_defaults", {}):
                    # Restore the original, if present
                    remote_metadata["global_defaults"]["eager_load_microservices"] = node_metadata[
                        "global_defaults"
                    ]["eager_load_microservices"]
                else:
                    # Not present originally, so just remove it from remote
                    remote_metadata["global_defaults"].pop("eager_load_microservices")
                    # Remove the entire global_defaults section if its empty
                    if len(remote_metadata["global_defaults"]) == 0:
                        remote_metadata.pop("global_defaults")

        # Replace the file and return the new zipfile
        return client.replace_file_in_zipfile(
            zip_data,
            "node-metadata.conf.json",
            json.dumps(remote_metadata, indent=2, ensure_ascii=False).encode("utf-8"),
        )

    return zip_data
