import json
import os
import re
from pathlib import Path
import shutil
from collections import defaultdict
from jinja2 import Environment, PackageLoader, select_autoescape, FileSystemLoader


def render(template, props, wrap=True):
    config = json.loads(template.render(** props))
    if type(config) is list or not wrap:
        return config
    else:
        # template is a single component, wrap it a list to make it consistent
        return [config]

node_metadata = {
  "_id": "node",
  "type": "metadata",
  "task_manager": {
    "disable_user_pipes": True
  }
}

def expand_connector_config(connector_dir, system_placeholder):
    output = []
    main_env = Environment(
        loader=PackageLoader("connector_cli.connectorpy"),
        autoescape=select_autoescape(),
        variable_start_string="{{@",
        variable_end_string="@}}"
    )
    shim_template = main_env.get_template("shim.json")

    with open(os.path.join(connector_dir, "manifest.json"), "r") as f:
        system_env = Environment(
            loader=FileSystemLoader(connector_dir),
            autoescape=select_autoescape(),
            variable_start_string="{{@",
            variable_end_string="@}}"
        )

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
            datatype_pipes = render(datatype_template, {**subst, **{"datatype": datatype}})
            if template_name != datatype:
                for pipe in datatype_pipes:
                    pipe["comment"] = "WARNING! This pipe is generated from the template of the '%s' datatype and " \
                                      "changes will be silently ignored during collapse. " \
                                      "For more information see the connectorpy README." % template_name
            output.extend(datatype_pipes)
            output.extend(render(shim_template, {"system": system_placeholder, "datatype": datatype}))
    return output, manifest

def expand_connector(connector_dir=".", system_placeholder="xxxxxx",expanded_dir=".expanded"):
    # put the expanded configuration into a subfolder in the connector directory in a form that can be used by sesam-py
    output, manifest = expand_connector_config(connector_dir, system_placeholder)
    dirpath = Path(connector_dir, expanded_dir)
    if dirpath.exists() and dirpath.is_dir():
        shutil.rmtree(dirpath)
    os.makedirs(dirpath)
    os.makedirs(dirpath / "pipes")
    os.makedirs(dirpath / "systems")
    with open(dirpath / "node-metadata.conf.json", "w") as f:
        json.dump(node_metadata, f, indent=2, sort_keys=True)
    with open(dirpath / "test-env.json", "w") as f:
        new_manifest = {**{"node-env": "test"},
                        **{key: "" for key in list(manifest.get("additional_parameters", {}).keys())}}
        json.dump(new_manifest, f, indent=2, sort_keys=True)
    for component in output:
        if component["type"] == "pipe":
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
        datatype = pipe.name.split("-")[1]
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
        if template_name in datatypes_with_no_master_template:
            continue
        template = json.dumps(components if len(components) > 1 else components[0], indent=2, sort_keys=True)
        fixed = template.replace(system_placeholder, "{{@ system @}}")
        envs = p.findall(fixed)
        for env in envs:
            e = env.replace("$ENV(", "{{@ ").replace(")", " @}}")
            env_parameters.add(e.replace("{{@ ", "").replace(" @}}", ""))
            fixed = fixed.replace(env, e)
            if template_name != "system":
                fixed = fixed.replace(template_name, "{{@ datatype @}}")
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