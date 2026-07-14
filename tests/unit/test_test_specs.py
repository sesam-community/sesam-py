import json

from sesam_cli import test_specs


def test_normalize_path_converts_windows_separators():
    assert test_specs.normalize_path(r"pipes\foo.conf.json") == "pipes/foo.conf.json"


def test_test_spec_loads_defaults_and_paths(tmp_path):
    expected_dir = tmp_path / "expected"
    expected_dir.mkdir()
    spec_path = expected_dir / "output.test.json"
    data_path = expected_dir / "output.json"

    spec_path.write_text(json.dumps({"pipe": "output-pipe"}), encoding="utf-8")
    data_path.write_text("[]", encoding="utf-8")

    spec = test_specs.TestSpec(str(spec_path))
    assert spec.pipe == "output-pipe"
    assert spec.file.endswith("expected/output.json")
    assert spec.expected_entities == []


def test_entity_sorter_supports_dict_fields(tmp_path):
    spec_path = tmp_path / "output.test.json"
    spec_path.write_text(json.dumps({"fields_to_sort_by": ["meta"]}), encoding="utf-8")

    spec = test_specs.TestSpec(str(spec_path))
    sorter = spec.get_entity_sorter_func()
    entities = [{"meta": {"b": 1, "a": 2}, "_id": "x"}, {"meta": {"a": 2, "b": 1}, "_id": "y"}]

    # Dict field sort keys are normalized to deterministic JSON strings.
    assert sorter(entities[0])[0] == sorter(entities[1])[0]
