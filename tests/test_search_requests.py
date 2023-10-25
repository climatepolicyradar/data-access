import pytest

from cpr_data_access.models.search import SearchRequestBody
from cpr_data_access.vespa import build_yql, sanitize


def test_whether_an_empty_query_string_raises_a_valueerror():
    with pytest.raises(ValueError):
        SearchRequestBody(query_string="")


@pytest.mark.parametrize("year_range", [(2000, 2020), (2000, None), (None, 2020)])
def test_whether_valid_year_ranges_are_accepted(year_range):
    request = SearchRequestBody(query_string="test", year_range=year_range)
    assert isinstance(request, SearchRequestBody)


def test_whether_an_invalid_year_range_ranges_raises_a_valueerror():
    with pytest.raises(ValueError):
        SearchRequestBody(query_string="test", year_range=(2023, 2000))


@pytest.mark.parametrize("field", ["date", "name"])
def test_whether_valid_sort_fields_are_accepted(field):
    request = SearchRequestBody(query_string="test", sort_by=field)
    assert isinstance(request, SearchRequestBody)


def test_whether_an_invalid_sort_field_raises_a_valueerror():
    with pytest.raises(ValueError):
        SearchRequestBody(query_string="test", sort_by="invalid_field")


@pytest.mark.parametrize("order", ["ascending", "descending"])
def test_whether_valid_sort_orders_are_accepted(order):
    request = SearchRequestBody(query_string="test", sort_order=order)
    assert isinstance(request, SearchRequestBody)


def test_whether_an_invalid_sort_order_raises_a_valueerror():
    with pytest.raises(ValueError):
        SearchRequestBody(query_string="test", sort_order="invalid_order")


@pytest.mark.parametrize("field", ["geography", "category", "language", "source"])
def test_whether_valid_filter_fields_are_accepted(field):
    request = SearchRequestBody(query_string="test", keyword_filters={field: "value"})
    assert isinstance(request, SearchRequestBody)


def test_whether_an_invalid_filter_fields_raises_a_valueerror():
    with pytest.raises(ValueError):
        SearchRequestBody(
            query_string="test", keyword_filters={"invalid_field": "value"}
        )


def test_whether_malicious_query_strings_are_sanitized():
    input_string = ' " or true or \t \n family_name contains " '
    output_string = sanitize(input_string)
    assert output_string == "or true or family_name contains"


def test_whether_single_filter_values_and_lists_of_filter_values_appear_in_yql():
    request = SearchRequestBody(
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
        values = [values] if not isinstance(values, list) else values
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
    request = SearchRequestBody(query_string="test", year_range=year_range)
    yql = build_yql(request)
    for include in expected_include:
        assert include in yql
    for exclude in expected_exclude:
        assert exclude not in yql
