import pytest
from vespa.exceptions import VespaError

from cpr_data_access.models.search import KeywordFilters, SearchParameters
from cpr_data_access.vespa import VespaErrorDetails
from cpr_data_access.yql_builder import YQLBuilder


def test_whether_single_filter_values_and_lists_of_filter_values_appear_in_yql():
    keyword_filters = {
        "family_geography": ["SWE"],
        "family_category": ["Executive"],
        "document_languages": ["English", "Swedish"],
        "family_source": ["CCLW"],
    }
    params = SearchParameters(
        query_string="test",
        keyword_filters=KeywordFilters(**keyword_filters),
    )
    yql = YQLBuilder(params).to_str()
    assert isinstance(params.keyword_filters, KeywordFilters)

    for key, values in keyword_filters.items():
        for value in values:
            assert key in yql
            assert value in yql


@pytest.mark.parametrize(
    "year_range, expected_include, expected_exclude",
    [
        ((2000, 2020), [">= 2000", "<= 2020"], []),
        ((2000, None), [">= 2000"], ["<="]),
        ((None, 2020), ["<= 2020"], [">="]),
    ],
)
def test_whether_year_ranges_appear_in_yql(
    year_range, expected_include, expected_exclude
):
    params = SearchParameters(query_string="test", year_range=year_range)
    yql = YQLBuilder(params).to_str()
    for include in expected_include:
        assert include in yql
    for exclude in expected_exclude:
        assert exclude not in yql


def test_vespa_error_details():
    # With invalid query parameter code
    err_object = [
        {
            "code": 4,
            "summary": "test_summary",
            "message": "test_message",
            "stackTrace": None,
        }
    ]
    err = VespaError(err_object)
    details = VespaErrorDetails(err)

    assert details.code == err_object[0]["code"]
    assert details.summary == err_object[0]["summary"]
    assert details.message == err_object[0]["message"]
    assert details.is_invalid_query_parameter

    # With other code
    err_object = [{"code": 1}]
    err = VespaError(err_object)
    details = VespaErrorDetails(err)
    assert not details.is_invalid_query_parameter


def test_filter_profiles_return_different_queries():
    exact_yql = YQLBuilder(
        params=SearchParameters(
            query_string="test", year_range=(2000, 2023), exact_match=True
        ),
        sensitive=False,
    ).to_str()
    assert "stem: false" in exact_yql
    assert "nearestNeighbor" not in exact_yql

    hybrid_yql = YQLBuilder(
        params=SearchParameters(
            query_string="test", year_range=(2000, 2023), exact_match=False
        ),
        sensitive=False,
    ).to_str()
    assert "nearestNeighbor" in hybrid_yql

    sensitive_yql = YQLBuilder(
        params=SearchParameters(
            query_string="test", year_range=(2000, 2023), exact_match=False
        ),
        sensitive=True,
    ).to_str()
    assert "nearestNeighbor" not in sensitive_yql

    queries = [exact_yql, hybrid_yql, sensitive_yql]
    assert len(queries) == len(set(queries))


def test_yql_builder_build_where_clause():
    query_string = "climate"
    params = SearchParameters(query_string=query_string)
    where_clause = YQLBuilder(params).build_where_clause()
    # raw user input should NOT be in the where clause
    # We send this in the body so its cleaned by vespa
    assert query_string not in where_clause

    params = SearchParameters(
        query_string="climate", keyword_filters={"family_geography": ["SWE"]}
    )
    where_clause = YQLBuilder(params).build_where_clause()
    assert "SWE" in where_clause
    assert "family_geography" in where_clause

    params = SearchParameters(
        query_string="test",
        family_ids=("CCLW.family.i00000003.n0000", "CCLW.family.10014.0"),
    )
    where_clause = YQLBuilder(params).build_where_clause()
    assert "CCLW.family.i00000003.n0000" in where_clause
    assert "CCLW.family.10014.0" in where_clause

    params = SearchParameters(
        query_string="test",
        document_ids=("CCLW.document.i00000004.n0000", "CCLW.executive.10014.4470"),
    )
    where_clause = YQLBuilder(params).build_where_clause()
    assert "CCLW.document.i00000004.n0000" in where_clause
    assert "CCLW.executive.10014.4470" in where_clause

    params = SearchParameters(query_string="climate", year_range=(2000, None))
    where_clause = YQLBuilder(params).build_where_clause()
    assert "2000" in where_clause
    assert "family_publication_year" in where_clause

    params = SearchParameters(query_string="climate", year_range=(None, 2020))
    where_clause = YQLBuilder(params).build_where_clause()
    assert "2020" in where_clause
    assert "family_publication_year" in where_clause
