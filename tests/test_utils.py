import pytest

from cpr_data_access.utils import dig, is_sensitive_query, load_sensitive_query_terms


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
        [True, "wordle"],
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
