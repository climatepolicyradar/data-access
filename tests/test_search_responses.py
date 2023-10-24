import json

import pytest
from vespa.io import VespaResponse

from cpr_data_access.models.search import SearchRequestBody
from cpr_data_access.vespa import _parse_vespa_response


@pytest.fixture
def valid_vespa_response():
    with open("tests/test_data/search_responses/valid_response.json") as f:
        response_json = json.load(f)
        return VespaResponse(
            json=response_json, status_code=200, url="", operation_type=""
        )


@pytest.fixture
def invalid_vespa_response():
    return VespaResponse(
        json={"this json": "is not valid"}, status_code=500, url="", operation_type=""
    )


def test_whether_a_valid_vespa_response_is_parsed(valid_vespa_response):
    request = SearchRequestBody(query_string="test")
    assert _parse_vespa_response(request=request, vespa_response=valid_vespa_response)


def test_whether_an_invalid_vespa_response_raises_a_valueerror(invalid_vespa_response):
    with pytest.raises(ValueError):
        request = SearchRequestBody(query_string="test")
        _parse_vespa_response(request=request, vespa_response=invalid_vespa_response)


def test_whether_sorting_by_ascending_date_works(valid_vespa_response):
    request = SearchRequestBody(
        query_string="test", sort_by="date", sort_order="ascending"
    )
    response = _parse_vespa_response(
        request=request, vespa_response=valid_vespa_response
    )
    for family_i, family_j in zip(response.families[:-1], response.families[1:]):
        date_i = family_i.hits[0].family_publication_ts
        date_j = family_j.hits[0].family_publication_ts
        assert date_i <= date_j


def test_whether_sorting_by_descending_date_works(valid_vespa_response):
    request = SearchRequestBody(
        query_string="test", sort_by="date", sort_order="descending"
    )
    response = _parse_vespa_response(
        request=request, vespa_response=valid_vespa_response
    )
    for family_i, family_j in zip(response.families[:-1], response.families[1:]):
        date_i = family_i.hits[0].family_publication_ts
        date_j = family_j.hits[0].family_publication_ts
        assert date_i >= date_j


def test_whether_sorting_by_ascending_name_works(valid_vespa_response):
    request = SearchRequestBody(
        query_string="test", sort_by="name", sort_order="ascending"
    )
    response = _parse_vespa_response(
        request=request, vespa_response=valid_vespa_response
    )
    for family_i, family_j in zip(response.families[:-1], response.families[1:]):
        name_i = family_i.hits[0].family_name
        name_j = family_j.hits[0].family_name
        assert name_i <= name_j


def test_whether_sorting_by_descending_name_works(valid_vespa_response):
    request = SearchRequestBody(
        query_string="test", sort_by="name", sort_order="descending"
    )
    response = _parse_vespa_response(
        request=request, vespa_response=valid_vespa_response
    )
    for family_i, family_j in zip(response.families[:-1], response.families[1:]):
        name_i = family_i.hits[0].family_name
        name_j = family_j.hits[0].family_name
        assert name_i >= name_j


def test_whether_continuation_token_is_returned_when_present(valid_vespa_response):
    request = SearchRequestBody(query_string="test", limit=1)
    response = _parse_vespa_response(
        request=request, vespa_response=valid_vespa_response
    )
    assert response.continuation_token
