import pytest

from cpr_data_access.models.search import SearchRequestBody
from cpr_data_access.vespa import _build_yql, sanitize


def test_whether_an_empty_query_string_raises_a_valueerror():
    with pytest.raises(ValueError):
        SearchRequestBody(query_string="")


def test_whether_valid_year_ranges_are_accepted():
    request = SearchRequestBody(query_string="test", year_range=(None, 2000))
    assert isinstance(request, SearchRequestBody)

    request = SearchRequestBody(query_string="test", year_range=(2000, None))
    assert isinstance(request, SearchRequestBody)


def test_whether_an_invalid_year_range_ranges_raises_a_valueerror():
    with pytest.raises(ValueError):
        SearchRequestBody(query_string="test", year_range=(2023, 2000))


def test_whether_valid_sort_fields_are_accepted():
    valid_sort_fields = ["date", "name"]
    for field in valid_sort_fields:
        request = SearchRequestBody(query_string="test", sort_by=field)
        assert isinstance(request, SearchRequestBody)


def test_whether_an_invalid_sort_field_raises_a_valueerror():
    with pytest.raises(ValueError):
        SearchRequestBody(query_string="test", sort_by="invalid_field")


def test_whether_valid_sort_orders_are_accepted():
    valid_sort_orders = ["ascending", "descending"]
    for order in valid_sort_orders:
        request = SearchRequestBody(query_string="test", sort_order=order)
        assert isinstance(request, SearchRequestBody)


def test_whether_an_invalid_sort_order_raises_a_valueerror():
    with pytest.raises(ValueError):
        SearchRequestBody(query_string="test", sort_order="invalid_order")


def test_whether_valid_filter_fields_are_accepted():
    valid_filter_fields = ["geography", "category", "language", "source"]
    for field in valid_filter_fields:
        request = SearchRequestBody(
            query_string="test", keyword_filters={field: "value"}
        )
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
    yql = _build_yql(request)
    for key, values in request.keyword_filters.items():
        if not isinstance(values, list):
            values = [values]
        for value in values:
            assert key in yql
            assert value in yql


def test_whether_year_ranges_appear_in_yql():
    request = SearchRequestBody(query_string="test", year_range=(2000, 2020))
    yql = _build_yql(request)
    assert ">= 2000" in yql
    assert "<= 2020" in yql

    request = SearchRequestBody(query_string="test", year_range=(2000, None))
    yql = _build_yql(request)
    assert ">= 2000" in yql
    assert "<=" not in yql

    request = SearchRequestBody(query_string="test", year_range=(None, 2020))
    yql = _build_yql(request)
    assert "<= 2020" in yql
    assert ">=" not in yql
