import json
import logging
import os
from fnmatch import fnmatch
from typing import Any


def normalize_path(filename: str) -> str:
    # Normalize windows paths to linux
    return filename.replace("\\", "/")


class TestSpec:
    """Test specification"""

    def __init__(self, filename: str, logger: logging.Logger | None = None):
        self._logger = logger or logging.getLogger("sesam")
        self._spec: dict[str, Any] = {}
        self._spec_file = filename

        self._spec["name"] = filename[: -len(".test.json")]
        self._spec["file"] = self.name + ".json"
        self._spec["endpoint"] = "json"

        if self.name.find(os.sep) > -1:
            self._spec["pipe"] = self.name.split(os.sep)[-1]
        else:
            self._spec["pipe"] = self.name

        with open(filename, "r", encoding="utf-8-sig") as fp:
            spec_dict = json.load(fp)
            if isinstance(spec_dict, dict):
                self._spec.update(spec_dict)
            else:
                self._logger.error("Test spec '%s' not in correct json format", filename)
                raise AssertionError("Test spec not a json object")

    @property
    def spec(self) -> dict[str, Any]:
        return self._spec

    @property
    def spec_file(self) -> str:
        filename = self._spec_file
        if not filename.startswith("expected" + os.sep):
            filename = os.path.join("expected", filename)
        return filename

    @property
    def file(self) -> str:
        filename = self._spec.get("file")
        if not filename.startswith("expected" + os.sep):
            filename = os.path.join("expected", filename)
        return filename

    @property
    def name(self) -> str:
        return self._spec.get("name")

    @property
    def ignore_deletes(self) -> bool:
        return self._spec.get("ignore_deletes", True) is True

    @property
    def endpoint(self) -> str:
        return self._spec.get("endpoint")

    @property
    def pipe(self) -> str:
        return self._spec.get("pipe")

    @property
    def stage(self) -> str | None:
        return self._spec.get("stage")

    @property
    def blacklist(self) -> list[str] | None:
        return self._spec.get("blacklist")

    @property
    def id(self) -> str | None:
        return self._spec.get("_id")

    def get_entity_sorter_func(self, json_ensure_ascii: bool = False):
        """By default we sort by '_id', but some pipes require custom deterministic fields."""
        fields_to_sort_by = self._spec.get("fields_to_sort_by", ["_id"])

        def entity_sorter_func(entity: dict[str, Any]):
            sort_key = []
            for field in fields_to_sort_by:
                fieldvalue = entity.get(field)
                if type(fieldvalue) is dict:
                    fieldvalue = json.dumps(
                        fieldvalue, ensure_ascii=json_ensure_ascii, sort_keys=True
                    )
                sort_key.append(fieldvalue)
            sort_key.append(json.dumps(entity, ensure_ascii=json_ensure_ascii, sort_keys=True))
            return sort_key

        return entity_sorter_func

    @property
    def ignore(self) -> bool:
        return self._spec.get("ignore", False) is True

    @property
    def parameters(self) -> dict[str, Any] | None:
        return self.spec.get("parameters")

    @property
    def expected_data(self) -> bytes:
        filename = self.file
        if not filename.startswith("expected" + os.sep):
            filename = os.path.join("expected", filename)

        with open(filename, "rb") as fp:
            return fp.read()

    @property
    def expected_entities(self) -> list[dict[str, Any]]:
        filename = self.file
        if not filename.startswith("expected" + os.sep):
            filename = os.path.join("expected", filename)

        with open(filename, "r", encoding="utf-8-sig") as fp:
            return json.load(fp)

    def update_expected_data(self, data: bytes) -> None:
        filename = self.file
        if not filename.startswith("expected" + os.sep):
            filename = os.path.join("expected", filename)

        if os.path.isfile(filename) is False:
            self._logger.debug("Creating new expected data file '%s'", filename)

        with open(filename, "wb") as fp:
            fp.write(data)

    def is_path_blacklisted(self, path: list[str]) -> bool:
        blacklist = self.blacklist
        if blacklist and isinstance(blacklist, list):
            prop_path = ".".join(path).replace(r"\.", ".")

            for pattern in blacklist:
                if fnmatch(prop_path, pattern.replace("[].", ".*.")):
                    return True

        return False
