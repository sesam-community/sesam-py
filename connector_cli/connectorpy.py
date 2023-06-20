import json
import logging
import os
import re
import shutil
from collections import defaultdict
from copy import deepcopy
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, PackageLoader, select_autoescape

logger = logging.getLogger("sesam.connector")


def render(template, props):
    # Workaround for inserting bools during rendering: set the property to a
    # dummy value so that the render doesnot fail, then do .replace() later
    # on the rendered config and replace the dummy values with JSON bools
    booleans = {}
    for prop, value in props.items():
        if isinstance(value, bool):
            props[prop] = "{{@ %s @}}" % prop
            booleans[prop] = str(value).lower()

    config = json.loads(template.render(**props))
    if not isinstance(config, list):
        config = [config]

    if booleans:
        _config = []
        for cfg in config:
            cfg_str = json.dumps(cfg)
            for param, _bool in booleans.items():
                cfg_str = cfg_str.replace('"{{@ %s @}}"' % param, str(_bool).lower())

            _config.append(json.loads(cfg_str))

        config = _config

    return config


node_metadata = {
    "_id": "node",
    "type": "metadata",
    "task_manager": {"disable_user_pipes": True},
}


def expand_connector_config(system_placeholder):
    output = []

    def jinja_env(loader):
        return Environment(
            loader=loader,
            autoescape=select_autoescape(),
            variable_start_string="{{@",
            variable_end_string="@}}",
            block_start_string="{{%",
            block_end_string="%}}",
        )

    main_env = jinja_env(loader=PackageLoader("connector_cli.connectorpy"))

    shim_template = main_env.get_template("shim.json")

    with open(os.path.join("manifest.json"), "r") as f:
        system_env = jinja_env(loader=FileSystemLoader("."))

        manifest = json.load(f)

        subst = {
            **{"system": system_placeholder},
            **{
                key: "$ENV(%s)" % key
                for key in list(manifest.get("additional_parameters", {}).keys())
            },
        }

        system_template = manifest.get("system-template")
        if system_template:
            # generate system config
            template = system_env.get_template(manifest["system-template"])
            output.extend(render(template, subst))

        for datatype, datatype_manifest in manifest.get("datatypes").items():
            template = datatype_manifest["template"]
            template_name = os.path.splitext(os.path.basename(template))[0]
            datatype_template = system_env.get_template(template)
            datatype_parameters = datatype_manifest.get("parameters", {})
            subst.update(datatype_parameters)

            if "parent" in datatype_manifest:
                datatype_pipes = render(
                    datatype_template,
                    {
                        **subst,
                        **{
                            "datatype": datatype,
                            "parent": datatype_manifest.get("parent"),
                        },
                    },
                )
            else:
                datatype_pipes = render(
                    datatype_template, {**subst, **{"datatype": datatype}}
                )
            if template_name != datatype:
                for pipe in datatype_pipes:
                    pipe["comment"] = (
                        "WARNING! This pipe is generated from the template "
                        "of the '%s' datatype and "
                        "changes will be silently ignored during collapse. "
                        "For more information see the connectorpy README."
                        % template_name
                    )
            output.extend(datatype_pipes)
            output.extend(
                render(
                    shim_template, {"system": system_placeholder, "datatype": datatype}
                )
            )
    return output, manifest


def expand_connector(
    system_placeholder="xxxxxx", expanded_dir=".expanded", profile="test"
):
    # put the expanded configuration into a subfolder in the connector directory
    # in a form that can be used by sesam-py
    output, manifest = expand_connector_config(system_placeholder)
    dirpath = Path(expanded_dir)
    if dirpath.exists() and dirpath.is_dir():
        shutil.rmtree(dirpath)
    os.makedirs(dirpath)
    os.makedirs(dirpath / "pipes")
    os.makedirs(dirpath / "systems")
    with open(dirpath / "node-metadata.conf.json", "w") as f:
        json.dump(node_metadata, f, indent=2, sort_keys=True)
    profile_file = "%s-env.json" % profile
    # get the existing profile file if it exists
    if os.path.exists(profile_file):
        with open(profile_file, "r", encoding="utf-8-sig") as f:
            new_manifest = json.load(f)
    else:
        new_manifest = {
            **{"node-env": "test"},
            **{
                key: ""
                for key in list(manifest.get("additional_parameters", {}).keys())
            },
        }
    with open(dirpath / profile_file, "w") as f:
        json.dump(new_manifest, f, indent=2, sort_keys=True)
    for component in output:
        if component["type"] == "pipe":
            if (
                component.get("source").get("type") == "http_endpoint"
                and component.get("_id").endswith("event")
                and manifest.get("use_webhook_secret")
            ):
                endpoint_permissions = [["allow", ["group:Anonymous"], ["write_data"]]]
                if component.get("permissions"):
                    logger.warning(
                        "Permissions are already set for endpoint pipe "
                        f"'{component['_id']}'. They will be "
                        f"overwritten with: {endpoint_permissions}"
                    )

                component["permissions"] = endpoint_permissions
                logger.warning(
                    "Set permissions for endpoint pipe"
                    f"'{component['_id']}' to: {endpoint_permissions}"
                )

            with open(dirpath / f"pipes/{component['_id']}.conf.json", "w") as f:
                json.dump(component, f, indent=2, sort_keys=True)

        elif component["type"].startswith("system:"):
            with open(dirpath / f"systems/{component['_id']}.conf.json", "w") as f:
                json.dump(component, f, indent=2, sort_keys=True)


def collapse_connector(
    connector_dir=".", system_placeholder="xxxxxx", expanded_dir=".expanded"
):
    # reconstruct the templates
    input = Path(connector_dir, expanded_dir)
    templates = defaultdict(list)
    for system in Path(input, "systems").glob("*.json"):
        with open(system, "r") as f:
            templates["system"].append(json.load(f))
    for pipe in Path(input, "pipes").glob("*.json"):
        datatype = pipe.name.split("-", 1)[1].rsplit("-", 1)[0]
        if pipe.name.endswith("-transform.json"):  # shim pipe
            continue
        with open(pipe, "r") as f:
            pipe = json.load(f)
            # skip shim
            if not pipe["_id"] == "%s-%s-transform" % (system_placeholder, datatype):
                templates[datatype].append(pipe)

    dirpath = Path(connector_dir)
    os.makedirs(dirpath / "templates", exist_ok=True)

    # read manifest
    manifest_path = Path(connector_dir, "manifest.json")
    existing_manifest = {}
    if manifest_path.exists():
        with open(manifest_path, "r") as f:
            existing_manifest = json.load(f)

    # find parents
    datatypes_with_parent = {}
    for datatype, datatype_manifest in existing_manifest.get("datatypes", {}).items():
        if "parent" in datatype_manifest:
            datatypes_with_parent[datatype] = datatype_manifest["parent"]

    # ignore templates that doesn't match the name of the datatype (re-used templates)
    datatypes_with_no_master_template = set()
    for datatype, datatype_manifest in existing_manifest.get("datatypes", {}).items():
        template = datatype_manifest["template"]
        template_name = os.path.splitext(os.path.basename(template))[0]
        if template_name != datatype:
            datatypes_with_no_master_template.add(datatype)

    # write the datatype templates
    env_parameters = set()
    p = re.compile("\$ENV\(\w+\)")

    for template_name, components in templates.items():
        components = sorted(components, key=lambda x: x["_id"])
        if template_name in datatypes_with_no_master_template:
            continue
        datatype_parameters = (
            existing_manifest.get("datatypes", {})
            .get(template_name, {})
            .get("parameters", {})
        )
        should_warn = False
        param_values = []
        for param_name, value in datatype_parameters.items():
            if value not in str(components):
                param_values.append(value.upper())
                should_warn = True
        if should_warn:
            warning_text = (
                "WARNING! There is no use for template parameter(s) "
                f"{param_values} in template: {template_name.upper()}"
            )
            if len(components) > 0:
                if "description" not in components[0].keys():
                    components[0]["description"] = warning_text
                else:
                    components[0]["description"] += warning_text
            else:
                if "description" not in components.keys():
                    components["description"] = warning_text
                else:
                    components["description"] += warning_text
            logger.error(warning_text)
        template = json.dumps(
            components if len(components) > 1 else components[0],
            indent=2,
            sort_keys=True,
        )
        fixed = template.replace(system_placeholder, "{{@ system @}}")
        envs = p.findall(fixed)
        for env in envs:
            e = env.replace("$ENV(", "{{@ ").replace(")", " @}}")
            env_parameters.add(e.replace("{{@ ", "").replace(" @}}", ""))
            fixed = fixed.replace(env, e)
        if template_name != "system":
            fixed = fixed.replace(template_name, "{{@ datatype @}}")
        if template_name in datatypes_with_parent:
            fixed = fixed.replace(
                datatypes_with_parent[template_name], "{{@ parent @}}"
            )
        for (
            param_name,
            value,
        ) in (
            datatype_parameters.items()
        ):  # TODO: best effort, might result in unintended replacements
            fixed = fixed.replace(value, "{{@ %s @}}" % param_name)
        with open(Path(dirpath, "templates", "%s.json" % template_name), "w") as f:
            f.write(fixed)

    # create manifest
    datatypes = list(templates.keys()) + list(datatypes_with_no_master_template)
    new_manifest = {
        "additional_parameters": {
            key: existing_manifest.get("additional_parameters", {}).get(key, {})
            for key in env_parameters
        },
        "system-template": "templates/system.json",
        "datatypes": {
            datatype: {
                **{"template": "templates/%s.json" % datatype},
                **existing_manifest.get("datatypes", {}).get(datatype, {}),
            }
            for datatype in datatypes
            if datatype != "system"
        },
    }
    manifest = {**existing_manifest, **new_manifest}
    with open(dirpath / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)


def update_schemas(connection, connector_dir=".", system_placeholder="xxxxxx"):
    dirpath = Path(connector_dir)
    os.makedirs(dirpath / "schemas", exist_ok=True)

    # Set 'infer_pipe_entity_types' to true if not already set
    node_metadata = connection.get_metadata().get("config", {}).get("effective", {})
    global_defaults = node_metadata.get("global_defaults", {})
    if global_defaults.get("infer_pipe_entity_types") is not True:
        global_defaults["infer_pipe_entity_types"] = True
        connection.set_metadata(node_metadata)

    # Find datatypes defined for the connector and run the corresponding collect pipes
    manifest_path = Path(connector_dir, "manifest.json")
    manifest = {}
    if manifest_path.exists():
        with open(manifest_path, "r") as f:
            manifest = json.load(f)

    incomplete_schema_info = {}
    datatypes = [datatype for datatype in manifest.get("datatypes", {}).keys()]
    collect_pipe_ids = [
        f"{system_placeholder}-{datatype}-collect" for datatype in datatypes
    ]
    for pipe_id, datatype in zip(collect_pipe_ids, datatypes):
        # Fetch inferred schema
        pipe = connection.get_pipe(pipe_id)
        if pipe:
            logger.info(f"Running pipe '{pipe_id}'")
            pump = pipe.get_pump()
            pump.run_pump_until_there_are_no_pending_work()
        else:
            logger.warning(
                f"Expected to find collect pipe '{pipe_id}' in "
                f"subscription, but it was not found. Maybe you need "
                f"to do a 'sesam upload' first?"
            )

        endpoint = f"{connection.pipes_url}/{pipe_id}/entity-types/sink"
        r = connection.do_get_request(endpoint, retries=20, retry_delay=3)

        live_schema = r.json()
        incomplete_schema_path = dirpath / "schemas" / f"{datatype}.json"

        incomplete_schema = deepcopy(live_schema)
        datatype_properties = live_schema.get("properties", {})
        num_nulls = 0

        # Check which properties are null and write those properties to a separate
        # 'incomplete' schema
        if not os.path.exists(incomplete_schema_path):
            for prop_name, _property in datatype_properties.items():
                is_null = True
                if prop_name.startswith("$"):  # ignore internal properties
                    incomplete_schema["properties"].pop(prop_name, None)
                    continue

                if _property.get("anyOf"):
                    for alternative in _property["anyOf"]:
                        # Set '' as default since we can't use the 'None' default here
                        if alternative.get("subtype") not in [
                            "null",
                            None,
                        ] or alternative.get("type") not in ["null", None]:
                            is_null = False
                            break
                else:
                    if _property.get("subtype") not in ["null", None] or _property.get(
                        "type"
                    ) not in ["null", None]:
                        is_null = False

                if is_null is True:
                    num_nulls += 1
                    incomplete_schema_info[datatype] = num_nulls
                    incomplete_schema["properties"][prop_name]["type"] = None
                else:
                    incomplete_schema["properties"].pop(prop_name, None)

            logger.info(
                f"Writing incomplete schema with {num_nulls} null-properties "
                f"to {incomplete_schema_path}"
            )
            with open(incomplete_schema_path, "w") as f:
                json.dump(incomplete_schema, f, indent=2, sort_keys=True)

        # If the auto-generated schema already exists, check if the properties can
        # be merged. They will only be merged if the property type is not null (i.e.
        # manually set by the user).
        logger.info(
            f"Checking if properties in {incomplete_schema_path} can be merged..."
        )
        with open(incomplete_schema_path, "r") as f:
            existing_schema = json.load(f)

        merged_schema = deepcopy(live_schema)
        datatype_properties = existing_schema.get("properties", {})
        for prop_name, _property in datatype_properties.items():
            if _property.get("type") is None:
                logger.info(
                    f"Skipping merge of '{datatype}.{prop_name}' "
                    f"because the type has not been set"
                )
            else:
                # The type has been set manually, so use the properties from this
                # schema in the merged version
                merged_schema["properties"][prop_name] = _property

        with open(dirpath / "schemas" / f"{datatype}.merged.json", "w") as f:
            json.dump(merged_schema, f, indent=2, sort_keys=True)

    if incomplete_schema_info:
        schemas_str = ",".join(
            {
                schema_id: num_nulls
                for schema_id, num_nulls in incomplete_schema_info.items()
            }
        )
        logger.info(
            f"Finished writing schemas. There are null-type properties in "
            f"the following schemas that need "
            f"to be manually inserted: {schemas_str}"
        )

    import glob

    schemas = {}
    for filename in glob.glob("schemas/*.merged.json"):
        with open(filename, "r") as infile:
            datatype = filename.replace("schemas/", "").split(".merged.json")[0]
            schema = json.load(infile)
            schemas[datatype] = schema

    with open("manifest.json", "r") as infile:
        manifest = json.load(infile)

    system = manifest.get("system", "unknown")

    def write_property(outfile, datatype, parent, property_name, property_schema):
        if parent is not None:
            property_name = parent + "." + property_name

        if "anyOf" in property_schema:
            if len(property_schema["anyOf"]) == 2:
                is_null = (
                    len(
                        [e for e in property_schema["anyOf"] if e.get("type") == "null"]
                    )
                    > 0
                )
                if is_null:
                    property_schema = [
                        e for e in property_schema["anyOf"] if e.get("type") != "null"
                    ][0]

        property_type = property_schema.get("subtype", property_schema.get("type"))

        if property_type == "object":
            for subproperty_name, subschema in property_schema["properties"].items():
                write_property(
                    outfile, datatype, property_name, subproperty_name, subschema
                )
        elif property_type == "array":
            items_schema = property_schema["items"]
            if items_schema.get("type", "") == "object":
                for subproperty_name, subschema in items_schema["properties"].items():
                    description = subschema.get("description", "").replace('"', '"')
                    subproperty_type = subschema.get("type")
                    outfile.write(
                        f'"{system}","{datatype}","{property_name}",'
                        f'"{subproperty_name}","{subproperty_type}",'
                        f'"{description}"\n'
                    )
            else:
                description = property_schema.get("description", "").replace('"', '"')
                outfile.write(
                    f'"{system}","{datatype}","{property_name}","",'
                    f'"{property_type}","{description}"\n'
                )
        else:
            description = property_schema.get("description", "").replace('"', '"')
            outfile.write(
                f'"{system}","{datatype}","{property_name}","",'
                f'"{property_type}","{description}"\n'
            )

    with open("schema.csv", "w") as outfile:
        outfile.write('"System","Type","Name","SubName","Datatype","Description"\n')
        schema_items = list(schemas.items())
        schema_items.sort()

        for datatype, schema in schema_items:
            for property_name, property_schema in schema["properties"].items():
                if property_name.startswith("$"):
                    continue

                write_property(outfile, datatype, None, property_name, property_schema)

    logger.info("Wrote updated connector schema to 'schema.csv'")
