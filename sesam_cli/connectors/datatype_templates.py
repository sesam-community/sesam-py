def get_datatype_template(args, datatype):
    share_operations = {}
    share_pipe_template_obj = {}

    all_pipe_template_obj = {
        "_id": "{{@ system @}}-{{@ datatype @}}-all",
        "add_namespaces": False,
        "source": {
            "operation": "{{@ datatype @}}-list",
            "system": "{{@ system @}}",
            "type": "rest",
        },
        "type": "pipe",
    }

    collect_pipe_template_obj = {
        "_id": "{{@ system @}}-{{@ datatype @}}-collect",
        "namespaced_identifiers": False,
        "source": {"dataset": "{{@ system @}}-{{@ datatype @}}-all", "type": "dataset"},
        "transform": [
            {
                "rules": {
                    "default": [
                        ["copy", "*"],
                        ["add", "$last-modified", ["datetime-parse", "<FORMATSTRING>", "<VALUES>"]],
                    ]
                },
                "type": "dtl",
            },
            {
                "properties": {
                    "primary_key": "id",
                    "operation_lookup_delete": "{{@ datatype @}}-lookup",
                },
                "template": "transform-collect-rest",
                "type": "template",
            },
        ],
        "type": "pipe",
    }

    if args.share:
        collect_pipe_template_obj["exclude_completeness"] = "{{@ system @}}-{{@ datatype @}}-share"
        collect_pipe_template_obj["transform"][1]["properties"][
            "share_dataset"
        ] = "{{@ system @}}-{{@ datatype @}}-share"

        share_pipe_template_obj = {
            "_id": "{{@ system @}}-{{@ datatype @}}-share",
            "batch_size": 1,
            "namespaced_identifiers": False,
            "sink": {"set_initial_offset": "onload"},
            "source": {
                "dataset": "{{@ system @}}-{{@ datatype @}}-transform",
                "type": "dataset",
            },
            "transform": {
                "properties": {
                    "operation_delete": "{{@ datatype @}}-delete",
                    "operation_insert": "{{@ datatype @}}-insert",
                    "operation_lookup": "{{@ datatype @}}-lookup",
                    "operation_update": "{{@ datatype @}}-update",
                    "primary_key": "id",
                    "rest_system": "{{@ system @}}",
                    "share_dataset": "{{@ system @}}-{{@ datatype @}}-share",
                },
                "template": "transform-share-rest",
                "type": "template",
            },
            "type": "pipe",
        }

        share_operations = {
            f"{datatype}-delete": {"method": "DELETE", "url": ""},
            f"{datatype}-insert": {"method": "POST", "url": ""},
            f"{datatype}-lookup": {"method": "GET", "url": ""},
            f"{datatype}-update": {"method": "PUT", "url": ""},
        }

    operations_obj = {
        f"{datatype}-list": {
            "id_expression": "{{ <primary-key> }}",
            "method": "GET",
            "next_page_link": (
                "{%if (headers.<link-location> is defined)%}"
                "{{headers.<link-location>}}{%endif%}"
            ),
            "next_page_termination_strategy": ["<strategy>"],
            "page_size": "<INT>",
            "payload_property": "",
            "since_property_name": "",
            "since_property_location": "",
            "updated_expression": "",
            "url": "",
        }
    }

    datatype_template_obj = [all_pipe_template_obj, collect_pipe_template_obj]
    if args.share:
        datatype_template_obj.append(share_pipe_template_obj)
        operations_obj.update(share_operations)

    return datatype_template_obj, operations_obj
