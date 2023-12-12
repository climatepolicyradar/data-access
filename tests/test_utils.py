import pytest

from cpr_data_access.utils import is_sensitive_query, load_sensitive_query_terms


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
