import json

import pytest

from event_platform.schema_compat import (
    EventSchema,
    SchemaError,
    check_backward_compatibility,
    check_files,
)


def schema(version=1, fields=None, event_type="order.created"):
    return EventSchema.from_dict(
        {
            "event_type": event_type,
            "version": version,
            "fields": fields
            or {
                "order_id": {"type": "string", "required": True},
                "coupon": {"type": "string", "required": False},
            },
        }
    )


def test_accepts_optional_and_defaulted_required_additions():
    proposed = schema(
        version=2,
        fields={
            "order_id": {"type": "string", "required": True},
            "coupon": {"type": "string", "required": False},
            "channel": {"type": "string", "required": False},
            "currency": {"type": "string", "required": True, "default": "USD"},
        },
    )
    report = check_backward_compatibility(schema(), proposed)
    assert report.compatible
    assert report.reasons == ()


def test_reports_all_breaking_changes_deterministically():
    proposed = schema(
        version=4,
        event_type="order.accepted",
        fields={
            "coupon": {"type": "integer", "required": True},
            "tenant_id": {"type": "string", "required": True},
        },
    )
    report = check_backward_compatibility(schema(), proposed)
    assert not report.compatible
    assert report.reasons == tuple(sorted(report.reasons))
    assert set(report.reasons) == {
        "event_type_changed",
        "field_removed:order_id",
        "field_type_changed:coupon:string->integer",
        "optional_field_became_required:coupon",
        "required_field_added_without_default:tenant_id",
        "version_must_increment_by_one",
    }


def test_removing_even_an_optional_field_is_breaking_for_old_consumers():
    proposed = schema(
        version=2,
        fields={"order_id": {"type": "string", "required": True}},
    )
    assert check_backward_compatibility(schema(), proposed).reasons == ("field_removed:coupon",)


@pytest.mark.parametrize(
    "value",
    [
        {
            "event_type": "x",
            "version": True,
            "fields": {"x": {"type": "string", "required": True}},
        },
        {
            "event_type": "x",
            "version": 1,
            "fields": {"x": {"type": "bytes", "required": True}},
        },
        {
            "event_type": "x",
            "version": 1,
            "fields": {"x": {"type": "string", "required": "yes"}},
        },
        {
            "event_type": "x",
            "version": 1,
            "fields": {"x": {"type": "string", "required": True, "typo": 1}},
        },
    ],
)
def test_malformed_schema_fails_closed(value):
    with pytest.raises(SchemaError):
        EventSchema.from_dict(value)


def test_file_check_is_json_ready(tmp_path):
    current = tmp_path / "v1.json"
    proposed = tmp_path / "v2.json"
    current.write_text(
        json.dumps(
            {
                "event_type": "x",
                "version": 1,
                "fields": {"id": {"type": "string", "required": True}},
            }
        )
    )
    proposed.write_text(
        json.dumps(
            {
                "event_type": "x",
                "version": 2,
                "fields": {"id": {"type": "string", "required": True}},
            }
        )
    )
    assert check_files(current, proposed).to_dict()["compatible"] is True
