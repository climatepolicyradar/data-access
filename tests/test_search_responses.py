import json

import pytest
from vespa.io import VespaResponse

from cpr_data_access.models.search import Hit, SearchRequestBody
from cpr_data_access.vespa import parse_vespa_response, split_document_id


@pytest.fixture
def valid_vespa_search_response():
    with open("tests/test_data/search_responses/search_response.json") as f:
        response_json = json.load(f)
        return VespaResponse(
            json=response_json, status_code=200, url="", operation_type=""
        )


@pytest.fixture
def invalid_vespa_search_response():
    return VespaResponse(
        json={"this json": "is not valid"}, status_code=500, url="", operation_type=""
    )


@pytest.fixture
def valid_get_document_response():
    with open("tests/test_data/search_responses/get_document_response.json") as f:
        response_json = json.load(f)
        return response_json


@pytest.fixture
def valid_get_passage_response():
    with open("tests/test_data/search_responses/get_passage_response.json") as f:
        response_json = json.load(f)
        return response_json


def test_whether_a_valid_vespa_response_is_parsed(valid_vespa_search_response):
    request = SearchRequestBody(query_string="test")
    assert parse_vespa_response(
        request=request, vespa_response=valid_vespa_search_response
    )


def test_whether_an_invalid_vespa_response_raises_a_valueerror(
    invalid_vespa_search_response,
):
    with pytest.raises(ValueError):
        request = SearchRequestBody(query_string="test")
        parse_vespa_response(
            request=request, vespa_response=invalid_vespa_search_response
        )


def test_whether_sorting_by_ascending_date_works(valid_vespa_search_response):
    request = SearchRequestBody(
        query_string="test", sort_by="date", sort_order="ascending"
    )
    response = parse_vespa_response(
        request=request, vespa_response=valid_vespa_search_response
    )
    for family_i, family_j in zip(response.families[:-1], response.families[1:]):
        date_i = family_i.hits[0].family_publication_ts
        date_j = family_j.hits[0].family_publication_ts
        assert date_i <= date_j


def test_whether_sorting_by_descending_date_works(valid_vespa_search_response):
    request = SearchRequestBody(
        query_string="test", sort_by="date", sort_order="descending"
    )
    response = parse_vespa_response(
        request=request, vespa_response=valid_vespa_search_response
    )
    for family_i, family_j in zip(response.families[:-1], response.families[1:]):
        date_i = family_i.hits[0].family_publication_ts
        date_j = family_j.hits[0].family_publication_ts
        assert date_i >= date_j


def test_whether_sorting_by_ascending_name_works(valid_vespa_search_response):
    request = SearchRequestBody(
        query_string="test", sort_by="name", sort_order="ascending"
    )
    response = parse_vespa_response(
        request=request, vespa_response=valid_vespa_search_response
    )
    for family_i, family_j in zip(response.families[:-1], response.families[1:]):
        name_i = family_i.hits[0].family_name
        name_j = family_j.hits[0].family_name
        assert name_i <= name_j


def test_whether_sorting_by_descending_name_works(valid_vespa_search_response):
    request = SearchRequestBody(
        query_string="test", sort_by="name", sort_order="descending"
    )
    response = parse_vespa_response(
        request=request, vespa_response=valid_vespa_search_response
    )
    for family_i, family_j in zip(response.families[:-1], response.families[1:]):
        name_i = family_i.hits[0].family_name
        name_j = family_j.hits[0].family_name
        assert name_i >= name_j


def test_whether_continuation_token_is_returned_when_present(
    valid_vespa_search_response,
):
    request = SearchRequestBody(query_string="test", limit=1)
    response = parse_vespa_response(
        request=request, vespa_response=valid_vespa_search_response
    )
    assert response.continuation_token


def test_whether_valid_get_document_response_is_parsed(valid_get_document_response):
    assert Hit.from_vespa_response(valid_get_document_response)


def test_whether_valid_get_passage_response_is_parsed(valid_get_passage_response):
    assert Hit.from_vespa_response(valid_get_passage_response)


def test_whether_valid_document_id_is_correctly_split():
    namespace, schema, data_id = split_document_id(
        "id:doc_search:family_document::CCLW.family.11171.0"
    )
    assert namespace == "doc_search"
    assert schema == "family_document"
    assert data_id == "CCLW.family.11171.0"


def test_whether_invalid_document_id_raises_value_error():
    with pytest.raises(ValueError):
        split_document_id("this is not a valid document id")
