"""RF-01, RF-02 — the row filter as ds puts it on the wire.

The filter travels whole — handler, args and both allow-lists — and an unknown
field in it is refused rather than skipped. The drift that matters is one-way: a
control plane that adds a narrowing an older data plane ignores serves rows it
was told to withhold, and nothing on either side notices.
"""
from __future__ import annotations

import pytest
from fastapi import HTTPException

from celine.dataset.security.edr import DataPlaneDecision, DataplaneRowFilter

POD_KEY = "pod:EX000E00000001"


def _decision(row_filter) -> DataPlaneDecision:
    return DataPlaneDecision(
        allowed=True,
        datasets=[
            {"dataset_id": "readings", "decision": "allow", "row_filter": row_filter}
        ],
    )


# ---------------------------------------------------------------------------
# RF-01 — the filter travels whole
# ---------------------------------------------------------------------------


def test_the_filter_carries_handler_args_principals_and_keys():
    decision = _decision(
        {
            "handler": "subject_key_match",
            "args": {"column": "pod", "key_type": "pod"},
            "principals": ["member-a"],
            "keys": [POD_KEY],
        }
    )

    row_filter = decision.row_filter_for("readings")

    assert isinstance(row_filter, DataplaneRowFilter)
    assert row_filter.handler == "subject_key_match"
    assert row_filter.args == {"column": "pod", "key_type": "pod"}
    assert row_filter.principals == ["member-a"]
    assert row_filter.keys == [POD_KEY]


def test_a_filter_from_a_control_plane_that_sends_no_keys_still_parses():
    """The field is new; a decision without it is a decision with no keys."""
    row_filter = _decision(
        {"handler": "direct_user_match", "args": {"column": "owner"},
         "principals": ["member-a"]}
    ).row_filter_for("readings")

    assert row_filter.keys == []


def test_no_filter_means_every_row_may_leave():
    """`None` is *no filter applies*, never *a filter could not be built*."""
    assert _decision(None).row_filter_for("readings") is None


def test_a_dataset_the_decision_does_not_name_has_no_filter():
    assert _decision({"handler": "direct_user_match"}).row_filter_for("other") is None


def test_a_filter_for_another_dataset_is_not_parsed():
    """A query that never touches that dataset cannot be refused by its filter."""
    decision = DataPlaneDecision(
        allowed=True,
        datasets=[
            {"dataset_id": "readings", "decision": "allow", "row_filter": None},
            {"dataset_id": "other", "decision": "allow",
             "row_filter": {"handler": "x", "narrow_to": ["something"]}},
        ],
    )

    assert decision.row_filter_for("readings") is None


# ---------------------------------------------------------------------------
# RF-02 — an unknown field refuses the request
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "row_filter",
    [
        {"handler": "direct_user_match", "subject_ids": ["member-a"]},
        {"handler": "subject_key_match", "args": {"column": "pod"},
         "keys": [POD_KEY], "exclude": ["member-b"]},
        {"handler": "direct_user_match", "column": "owner"},
    ],
)
def test_an_unknown_field_is_a_refusal_not_a_field_to_skip(row_filter):
    with pytest.raises(HTTPException) as exc:
        _decision(row_filter).row_filter_for("readings")

    # 502, not 403: the consumer did nothing wrong and can do nothing about it —
    # the two ends of the contract disagree.
    assert exc.value.status_code == 502
    assert "readings" in exc.value.detail


def test_a_filter_with_no_handler_is_a_refusal():
    with pytest.raises(HTTPException) as exc:
        _decision({"args": {"column": "pod"}, "keys": [POD_KEY]}).row_filter_for(
            "readings"
        )

    assert exc.value.status_code == 502


def test_the_refusal_does_not_echo_the_keys():
    """Keys are personal data, and an error detail is read by the consumer."""
    with pytest.raises(HTTPException) as exc:
        _decision(
            {"handler": "subject_key_match", "keys": [POD_KEY], "unknown": 1}
        ).row_filter_for("readings")

    assert "EX000E" not in exc.value.detail
