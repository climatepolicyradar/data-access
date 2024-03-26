import json

import pytest
from vespa.io import VespaResponse

from cpr_data_access.models.search import Hit, SearchParameters
from cpr_data_access.vespa import parse_vespa_response, split_document_id
from cpr_data_access.exceptions import FetchError


@pytest.fixture
def valid_vespa_search_response():
    with open("tests/test_data/search_responses/search_response.json") as f:
        response_json = json.load(f)
        return VespaResponse(
            json=response_json, status_code=200, url="", operation_type=""
        )


@pytest.fixture
def empty_vespa_search_response():
    with open("tests/test_data/search_responses/empty_search_response.json") as f:
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
    request = SearchParameters(query_string="test")
    assert parse_vespa_response(
        request=request, vespa_response=valid_vespa_search_response
    )


def test_whether_an_invalid_vespa_response_raises_a_valueerror(
    invalid_vespa_search_response,
):
    with pytest.raises(FetchError) as excinfo:
        request = SearchParameters(query_string="test")
        parse_vespa_response(
            request=request, vespa_response=invalid_vespa_search_response
        )
    assert "Received status code 500" in str(excinfo.value)


def test_whether_continuation_token_is_returned_when_present(
    valid_vespa_search_response,
):
    request = SearchParameters(query_string="test", limit=1)
    response = parse_vespa_response(
        request=request, vespa_response=valid_vespa_search_response
    )
    assert response.continuation_token
    assert response.prev_continuation_token


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


def test_whether_empty_response_is_parsed_correctly(empty_vespa_search_response):
    request = SearchParameters(
        query_string="ThisStringShouldNotMatchAnything", exact_match=True
    )
    response = parse_vespa_response(
        request=request, vespa_response=empty_vespa_search_response
    )
    assert response.total_hits == 0
    assert response.families == []
    assert response.continuation_token is None


def test_whether_family_title_search_works(empty_vespa_search_response):
    request = SearchParameters(
        query_string="Low Emissions Growth Strategy", exact_match=True
    )
    response = parse_vespa_response(
        request=request, vespa_response=empty_vespa_search_response
    )

    assert response.total_family_hits > 0
    assert len(response.families) > 0
