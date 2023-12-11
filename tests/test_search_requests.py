import pytest

from vespa.exceptions import VespaError

from cpr_data_access.models.search import SearchParameters
from cpr_data_access.vespa import build_yql, sanitize, VespaErrorDetails
from cpr_data_access.exceptions import QueryError


def test_whether_an_empty_query_string_raises_a_queryerror():
    with pytest.raises(QueryError) as excinfo:
        SearchParameters(query_string="")
    assert "query_string must not be empty" in str(excinfo.value)


@pytest.mark.parametrize("year_range", [(2000, 2020), (2000, None), (None, 2020)])
def test_whether_valid_year_ranges_are_accepted(year_range):
    request = SearchParameters(query_string="test", year_range=year_range)
    assert isinstance(request, SearchParameters)


def test_whether_an_invalid_year_range_ranges_raises_a_queryerror():
    with pytest.raises(QueryError) as excinfo:
        SearchParameters(query_string="test", year_range=(2023, 2000))
    assert (
        "The first supplied year must be less than or equal to the second supplied year"
        in str(excinfo.value)
    )


@pytest.mark.parametrize("field", ["date", "name"])
def test_whether_valid_sort_fields_are_accepted(field):
    request = SearchParameters(query_string="test", sort_by=field)
    assert isinstance(request, SearchParameters)


def test_whether_an_invalid_sort_field_raises_a_queryerror():
    with pytest.raises(QueryError) as excinfo:
        SearchParameters(query_string="test", sort_by="invalid_field")
    assert "sort_by must be one of" in str(excinfo.value)


@pytest.mark.parametrize("order", ["ascending", "descending"])
def test_whether_valid_sort_orders_are_accepted(order):
    request = SearchParameters(query_string="test", sort_order=order)
    assert isinstance(request, SearchParameters)


def test_whether_an_invalid_sort_order_raises_a_queryerror():
    with pytest.raises(QueryError) as excinfo:
        SearchParameters(query_string="test", sort_order="invalid_order")
    assert "sort_order must be one of" in str(excinfo.value)


@pytest.mark.parametrize(
    "field",
    ["family_geography", "family_category", "document_languages", "family_source"],
)
def test_whether_valid_filter_fields_are_accepted(field):
    request = SearchParameters(query_string="test", keyword_filters={field: "value"})
    assert isinstance(request, SearchParameters)


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
    request = SearchParameters(
        query_string="test",
        keyword_filters={
            "geography": "SWE",
            "category": "Executive",
            "language": ["English", "Swedish"],
            "source": "CCLW",
        },
    )
    yql = build_yql(request)
    for key, values in request.keyword_filters.items():
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
    request = SearchParameters(query_string="test", year_range=year_range)
    yql = build_yql(request)
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
