import pytest
from cpr_cpr_sdk.utils import (
    dig,
    is_sensitive_query,
    load_sensitive_query_terms,
    remove_key_if_all_nested_vals_none,
    unflatten_json,
)

TEST_SENSITIVE_QUERY_TERMS = (
    "word",
    "test term",
    "another phrase example",
)


@pytest.mark.parametrize(
    argnames="expected, text",
    argvalues=(
        [False, ""],
        [False, "ordinary query"],
        [False, "word but outnumbered"],
        [False, "word another phrase example but with many other items"],
        [True, "word"],
        [False, "wordle"],
        [True, "test term"],
        [True, "test term word"],
        [True, "test term and"],
        [True, "test term and some"],
        [True, "another phrase example"],
        [True, "another phrase example word short"],
        [True, "another phrase example with other items"],
    ),
)
def test_is_sensitive_query(expected, text):
    assert (
        is_sensitive_query(text, sensitive_terms=TEST_SENSITIVE_QUERY_TERMS) == expected
    )


def test_load_sensitive_query_terms():
    terms = load_sensitive_query_terms()
    assert terms
    assert len(terms) > 2000


@pytest.mark.parametrize(
    "fields, default, expected",
    [
        (["field"], None, "parent"),
        (["children", 0, "name"], None, "child_one"),
        (["children", 1, "items", 2], None, "two"),
        (["children", 2, "sub", "sub_sub", "sub_sub_sub", 2], None, "c"),
        (["children", 5], "default_1", "default_1"),
        (["children", 2, "sub", "sub_sub", "NO"], "default_2", "default_2"),
    ],
)
def test_dig(fields, default, expected):
    obj = {
        "field": "parent",
        "children": [
            {"name": "child_one"},
            {"name": "child_two", "items": ["zero", "one", "two"]},
            {
                "name": "child_three",
                "sub": {"sub_sub": {"sub_sub_sub": ["a", "b", "c"]}},
            },
        ],
    }

    assert dig(obj, *fields, default=default) == expected


def test_unflatten_json() -> None:
    """Test unflatten_json function."""
    data = {
        "a.b.c": 1,
        "a.b.d": 2,
        "a.e": 3,
        "f": 4,
    }

    expected = {
        "a": {
            "b": {"c": 1, "d": 2},
            "e": 3,
        },
        "f": 4,
    }

    assert unflatten_json(data) == expected


def test_remove_key_if_all_nested_vals_none() -> None:
    """Test remove_key_if_all_nested_vals_none function."""
    assert remove_key_if_all_nested_vals_none({}, "key") == {}
    assert remove_key_if_all_nested_vals_none({"key": None}, "key") == {"key": None}
    assert remove_key_if_all_nested_vals_none({"key": {"nested": None}}, "key") == {}
    assert remove_key_if_all_nested_vals_none({"key": {"nested": None}}, "no_key") == {
        "key": {"nested": None}
    }
    assert remove_key_if_all_nested_vals_none(
        {
            "key": {"nested": None},
            "key2": {"nested": "value"},
        },
        "key",
    ) == {"key2": {"nested": "value"}}
