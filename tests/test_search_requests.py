from unittest.mock import patch

import pytest

from vespa.exceptions import VespaError

from cpr_data_access.models.search import SearchParameters
from cpr_data_access.vespa import build_vespa_request_body, VespaErrorDetails
from cpr_data_access.yql_builder import YQLBuilder, sanitize
from cpr_data_access.exceptions import QueryError
from cpr_data_access.embedding import Embedder


@patch("cpr_data_access.vespa.SENSITIVE_QUERY_TERMS", {"sensitive"})
@pytest.mark.parametrize(
    "query_type, params",
    [
        ("hybrid", SearchParameters(query_string="test")),
        ("exact", SearchParameters(query_string="test", exact_match=True)),
        ("hybrid_no_closeness", SearchParameters(query_string="sensitive")),
    ],
)
def test_build_vespa_request_body(query_type, params):
    embedder = Embedder()
    body = build_vespa_request_body(parameters=params, embedder=embedder)
    assert body["ranking.profile"] == query_type
    for key, value in body.items():
        assert (
            len(value) > 0
        ), f"Query type: {query_type} has an empty value for {key}: {value}"


def test_whether_an_empty_query_string_raises_a_queryerror():
    with pytest.raises(QueryError) as excinfo:
        SearchParameters(query_string="")
    assert "query_string must not be empty" in str(excinfo.value)


@pytest.mark.parametrize("year_range", [(2000, 2020), (2000, None), (None, 2020)])
def test_whether_valid_year_ranges_are_accepted(year_range):
    params = SearchParameters(query_string="test", year_range=year_range)
    assert isinstance(params, SearchParameters)


def test_whether_an_invalid_year_range_ranges_raises_a_queryerror():
    with pytest.raises(QueryError) as excinfo:
        SearchParameters(query_string="test", year_range=(2023, 2000))
    assert (
        "The first supplied year must be less than or equal to the second supplied year"
        in str(excinfo.value)
    )


def test_whether_valid_family_ids_are_accepted():
    params = SearchParameters(
        query_string="test",
        family_ids=("CCLW.family.i00000003.n0000", "CCLW.family.10014.0"),
    )
    assert isinstance(params, SearchParameters)


def test_whether_an_invalid_family_id_raises_a_queryerror():
    with pytest.raises(QueryError) as excinfo:
        SearchParameters(
            query_string="test",
            family_ids=("CCLW.family.i00000003.n0000", "invalid_fam_id"),
        )
    assert "id does not seem valid: invalid_fam_id" in str(excinfo.value)


def test_whether_valid_document_ids_are_accepted():
    params = SearchParameters(
        query_string="test",
        document_ids=("CCLW.document.i00000004.n0000", "CCLW.executive.10014.4470"),
    )
    assert isinstance(params, SearchParameters)


def test_whether_an_invalid_document_id_raises_a_queryerror():
    with pytest.raises(QueryError) as excinfo:
        SearchParameters(
            query_string="test",
            document_ids=("invalid_doc_id", "CCLW.document.i00000004.n0000"),
        )
    assert "id does not seem valid: invalid_doc_id" in str(excinfo.value)


@pytest.mark.parametrize("field", ["date", "name"])
def test_whether_valid_sort_fields_are_accepted(field):
    params = SearchParameters(query_string="test", sort_by=field)
    assert isinstance(params, SearchParameters)


def test_whether_an_invalid_sort_field_raises_a_queryerror():
    with pytest.raises(QueryError) as excinfo:
        SearchParameters(query_string="test", sort_by="invalid_field")
    assert "sort_by must be one of" in str(excinfo.value)


@pytest.mark.parametrize("order", ["ascending", "descending"])
def test_whether_valid_sort_orders_are_accepted(order):
    params = SearchParameters(query_string="test", sort_order=order)
    assert isinstance(params, SearchParameters)


def test_whether_an_invalid_sort_order_raises_a_queryerror():
    with pytest.raises(QueryError) as excinfo:
        SearchParameters(query_string="test", sort_order="invalid_order")
    assert "sort_order must be one of" in str(excinfo.value)


@pytest.mark.parametrize(
    "field",
    ["family_geography", "family_category", "document_languages", "family_source"],
)
def test_whether_valid_filter_fields_are_accepted(field):
    params = SearchParameters(query_string="test", keyword_filters={field: "value"})
    assert isinstance(params, SearchParameters)


def test_whether_an_invalid_filter_fields_raises_a_valueerror():
    with pytest.raises(QueryError) as excinfo:
        SearchParameters(
            query_string="test", keyword_filters={"invalid_field": "value"}
        )
    assert "keyword_filters must be a subset of" in str(excinfo.value)


@pytest.mark.parametrize(
    "input_string,expected",
    (
        ['remove "double quotes"', "remove double quotes"],
        ["keep 'single quotes'", "keep 'single quotes'"],
        ["tab\t\tinput", "tab input"],
        ["new \n \n \n lines", "new lines"],
        ["back \\\\\\ slashes", "back slashes"],
        [
            ' " or true or \t \n family_name contains " ',
            "or true or family_name contains",
        ],
    ),
)
def test_whether_malicious_query_strings_are_sanitized(input_string, expected):
    output_string = sanitize(input_string)
    assert output_string == expected


def test_whether_single_filter_values_and_lists_of_filter_values_appear_in_yql():
    params = SearchParameters(
        query_string="test",
        keyword_filters={
            "family_geography": "SWE",
            "family_category": "Executive",
            "document_languages": ["English", "Swedish"],
            "family_source": "CCLW",
        },
    )
    yql = YQLBuilder(params).to_str()
    assert isinstance(params.keyword_filters, dict)
    for key, values in params.keyword_filters.items():
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
    params = SearchParameters(query_string="climate")
    where_clause = YQLBuilder(params).build_where_clause()
    assert "climate" in where_clause

    params = SearchParameters(
        query_string="climate", keyword_filters={"family_geography": "SWE"}
    )
    where_clause = YQLBuilder(params).build_where_clause()
    assert "SWE" in where_clause
    assert "family_geography" in where_clause

    params = SearchParameters(query_string="climate", year_range=(2000, None))
    where_clause = YQLBuilder(params).build_where_clause()
    assert "2000" in where_clause
    assert "family_publication_year" in where_clause

    params = SearchParameters(query_string="climate", year_range=(None, 2020))
    where_clause = YQLBuilder(params).build_where_clause()
    assert "2020" in where_clause
    assert "family_publication_year" in where_clause
