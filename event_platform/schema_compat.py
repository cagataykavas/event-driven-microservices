from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any


class SchemaError(ValueError):
    """Raised when a schema artifact is malformed."""


class FieldType(StrEnum):
    STRING = "string"
    INTEGER = "integer"
    NUMBER = "number"
    BOOLEAN = "boolean"
    OBJECT = "object"
    ARRAY = "array"


@dataclass(frozen=True, slots=True)
class FieldRule:
    type: FieldType
    required: bool
    has_default: bool = False

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> FieldRule:
        if not isinstance(value, Mapping):
            raise SchemaError("field rule must be an object")
        unknown = set(value) - {"type", "required", "default"}
        if unknown:
            raise SchemaError(f"unknown field-rule keys: {sorted(unknown)}")
        try:
            field_type = FieldType(value["type"])
        except (KeyError, ValueError) as exc:
            raise SchemaError("field type is missing or unsupported") from exc
        required = value.get("required")
        if not isinstance(required, bool):
            raise SchemaError("field required flag must be boolean")
        return cls(field_type, required, "default" in value)


@dataclass(frozen=True, slots=True)
class EventSchema:
    event_type: str
    version: int
    fields: Mapping[str, FieldRule]

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> EventSchema:
        if not isinstance(value, Mapping):
            raise SchemaError("schema must be an object")
        unknown = set(value) - {"event_type", "version", "fields"}
        if unknown:
            raise SchemaError(f"unknown schema keys: {sorted(unknown)}")
        event_type = value.get("event_type")
        version = value.get("version")
        fields = value.get("fields")
        if not isinstance(event_type, str) or not event_type.strip():
            raise SchemaError("event_type must be a non-empty string")
        if not isinstance(version, int) or isinstance(version, bool) or version < 1:
            raise SchemaError("version must be a positive integer")
        if not isinstance(fields, Mapping) or not fields:
            raise SchemaError("fields must be a non-empty object")
        parsed: dict[str, FieldRule] = {}
        for name, rule in sorted(fields.items()):
            if not isinstance(name, str) or not name.strip():
                raise SchemaError("field names must be non-empty strings")
            parsed[name] = FieldRule.from_dict(rule)
        return cls(event_type.strip(), version, parsed)


@dataclass(frozen=True, slots=True)
class CompatibilityReport:
    compatible: bool
    event_type: str
    current_version: int
    proposed_version: int
    reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "compatible": self.compatible,
            "event_type": self.event_type,
            "current_version": self.current_version,
            "proposed_version": self.proposed_version,
            "reasons": list(self.reasons),
        }


def check_backward_compatibility(
    current: EventSchema, proposed: EventSchema
) -> CompatibilityReport:
    reasons: list[str] = []
    if proposed.event_type != current.event_type:
        reasons.append("event_type_changed")
    if proposed.version != current.version + 1:
        reasons.append("version_must_increment_by_one")

    for name, old in current.fields.items():
        new = proposed.fields.get(name)
        if new is None:
            reasons.append(f"field_removed:{name}")
            continue
        if new.type != old.type:
            reasons.append(f"field_type_changed:{name}:{old.type}->{new.type}")
        if not old.required and new.required:
            reasons.append(f"optional_field_became_required:{name}")

    for name, new in proposed.fields.items():
        if name not in current.fields and new.required and not new.has_default:
            reasons.append(f"required_field_added_without_default:{name}")

    ordered = tuple(sorted(reasons))
    return CompatibilityReport(
        compatible=not ordered,
        event_type=current.event_type,
        current_version=current.version,
        proposed_version=proposed.version,
        reasons=ordered,
    )


def check_files(current_path: str | Path, proposed_path: str | Path) -> CompatibilityReport:
    def load(path: str | Path) -> EventSchema:
        try:
            value = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SchemaError(f"cannot load schema {path}: {exc}") from exc
        return EventSchema.from_dict(value)

    return check_backward_compatibility(load(current_path), load(proposed_path))
