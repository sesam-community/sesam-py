import json
import os
import re
from pathlib import Path
import shutil
from collections import defaultdict
from jinja2 import Environment, PackageLoader, select_autoescape, FileSystemLoader

import logging

logger = logging.getLogger(__name__)


def render(template, props):
    # Workaround for inserting bools during rendering: set the property to a dummy value so that the render does
    # not fail, then do .replace() later on the rendered config and replace the dummy values with JSON bools
    booleans = {}
    for prop, value in props.items():
        if isinstance(value, bool):
            props[prop] = '{{@ %s @}}' % prop
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
    "task_manager": {
        "disable_user_pipes": True
    }
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

        subst = {**{"system": system_placeholder},
                 **{key: "$ENV(%s)" % key for key in list(manifest.get("additional_parameters", {}).keys())}}

        system_template = manifest.get("system-template")
        if system_template:
            # generate system config
            template = system_env.get_template(manifest["system-template"])
            output.extend(render(template, subst))

        for datatype, datatype_manifest in manifest.get("datatypes").items():
            template = datatype_manifest["template"]
            template_name = os.path.splitext(os.path.basename(template))[0]
            datatype_template = system_env.get_template(template)
            datatype_parameters = datatype_manifest.get('parameters', {})
            subst.update(datatype_parameters)

            if "parent" in datatype_manifest:
                datatype_pipes = render(datatype_template, {**subst, **{"datatype": datatype,"parent":datatype_manifest.get("parent")}})
            else:
                datatype_pipes = render(datatype_template, {**subst, **{"datatype": datatype}})
            if template_name != datatype:
                for pipe in datatype_pipes:
                    pipe["comment"] = "WARNING! This pipe is generated from the template of the '%s' datatype and " \
                                      "changes will be silently ignored during collapse. " \
                                      "For more information see the connectorpy README." % template_name
            output.extend(datatype_pipes)
            output.extend(render(shim_template, {"system": system_placeholder, "datatype": datatype}))
    return output, manifest


def expand_connector(system_placeholder="xxxxxx", expanded_dir=".expanded", profile="test"):
    # put the expanded configuration into a subfolder in the connector directory in a form that can be used by sesam-py
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
        new_manifest = {**{"node-env": "test"},
                        **{key: "" for key in list(manifest.get("additional_parameters", {}).keys())}}
    with open(dirpath / profile_file, "w") as f:
        json.dump(new_manifest, f, indent=2, sort_keys=True)
    for component in output:
        if component["type"] == "pipe":
            if component.get("source").get("type") == 'http_endpoint' and component.get("_id").endswith('event') and \
                    manifest.get('use_webhook_secret'):
                endpoint_permissions = [["allow", ["group:Anonymous"], ["write_data"]]]
                if component.get("permissions"):
                    logger.warning(
                        f"Permissions are already set for endpoint pipe '{component['_id']}'. They will be "
                        f"overwritten with: {endpoint_permissions}")

                component['permissions'] = endpoint_permissions
                logger.warning(f"Set permissions for endpoint pipe '{component['_id']}' to: {endpoint_permissions}")

            with open(dirpath / f"pipes/{component['_id']}.conf.json", "w") as f:
                json.dump(component, f, indent=2, sort_keys=True)



        elif component["type"].startswith("system:"):
            with open(dirpath / f"systems/{component['_id']}.conf.json", "w") as f:
                json.dump(component, f, indent=2, sort_keys=True)


def collapse_connector(connector_dir=".", system_placeholder="xxxxxx", expanded_dir=".expanded"):
    # reconstruct the templates
    input = Path(connector_dir, expanded_dir)
    templates = defaultdict(list)
    for system in Path(input, "systems").glob('*.json'):
        with open(system, "r") as f:
            templates["system"].append(json.load(f))
    for pipe in Path(input, "pipes").glob('*.json'):
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
    p = re.compile('\$ENV\(\w+\)')

    for template_name, components in templates.items():
        components = sorted(components, key=lambda x: x["_id"])
        # print(template_name)
        # if components[0]["type"] == "pipe": #TODO: change the condition to check if no use of template param found
        #     components[0]["description"] = "WARNING! There is no use for template parameter"
        if template_name in datatypes_with_no_master_template:
            continue
        template = json.dumps(components if len(components) > 1 else components[0], indent=2, sort_keys=True)
        datatype_parameters = existing_manifest.get('datatypes', {}).get(template_name, {}).get('parameters', {})
        for comp in components:
            if "description" in comp.keys() and comp.get("description").startswith("WARNING!"):
                continue
        fixed = template.replace(system_placeholder, "{{@ system @}}")
        envs = p.findall(fixed)
        for env in envs:
            e = env.replace("$ENV(", "{{@ ").replace(")", " @}}")
            env_parameters.add(e.replace("{{@ ", "").replace(" @}}", ""))
            fixed = fixed.replace(env, e)
        if template_name != "system":
            fixed = fixed.replace(template_name, "{{@ datatype @}}")
        if template_name in datatypes_with_parent:
            fixed = fixed.replace(datatypes_with_parent[template_name], "{{@ parent @}}")
        for param_name, value in datatype_parameters.items():  # TODO: best effort, might result in unintended replacements
            fixed = fixed.replace(value, "{{@ %s @}}" % param_name)
        with open(Path(dirpath, "templates", "%s.json" % template_name), "w") as f:
            f.write(fixed)

    # create manifest
    datatypes = list(templates.keys()) + list(datatypes_with_no_master_template)
    new_manifest = {
        "additional_parameters": {key: existing_manifest.get("additional_parameters", {}).get(key, {}) for key in
                                  env_parameters},
        "system-template": "templates/system.json",
        "datatypes": {datatype: {**{"template": "templates/%s.json" % datatype},
                                 **existing_manifest.get("datatypes", {}).get(datatype, {})} for datatype in datatypes
                      if datatype != "system"}
    }
    manifest = {**existing_manifest, **new_manifest}
    with open(dirpath / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
