from unittest.mock import patch

import pytest

from cpr_data_access.search_adaptors import VespaSearchAdapter
from cpr_data_access.models.search import SearchParameters, SearchResponse

from conftest import VESPA_TEST_SEARCH_URL


def vespa_search(cert_directory: str, request: SearchParameters) -> SearchResponse:
    try:
        adaptor = VespaSearchAdapter(
            instance_url=VESPA_TEST_SEARCH_URL, cert_directory=cert_directory
        )
        response = adaptor.search(request)
    except Exception as e:
        pytest.fail(f"Vespa query failed. {e.__class__.__name__}: {e}")
    return response


@pytest.mark.vespa
def test_vespa_search_adaptor__works(fake_vespa_credentials):
    request = SearchParameters(query_string="the")
    response = vespa_search(fake_vespa_credentials, request)

    assert len(response.families) == response.total_family_hits == 3
    assert response.query_time_ms < response.total_time_ms
    total_passage_count = sum([f.total_passage_hits for f in response.families])
    assert total_passage_count == response.total_hits


@pytest.mark.vespa
@pytest.mark.parametrize(
    "family_ids",
    [
        ["CCLW.family.i00000003.n0000"],
        ["CCLW.family.10014.0"],
        ["CCLW.family.i00000003.n0000", "CCLW.family.10014.0"],
        ["CCLW.family.4934.0"],
    ],
)
def test_vespa_search_adaptor__family_ids(fake_vespa_credentials, family_ids):
    request = SearchParameters(query_string="the", family_ids=family_ids)
    response = vespa_search(fake_vespa_credentials, request)
    got_family_ids = [f.id for f in response.families]
    assert sorted(got_family_ids) == sorted(family_ids)


@pytest.mark.vespa
@pytest.mark.parametrize(
    "document_ids",
    [
        ["CCLW.document.i00000004.n0000"],
        ["CCLW.executive.10014.4470"],
        ["CCLW.document.i00000004.n0000", "CCLW.executive.10014.4470"],
        ["CCLW.executive.4934.1571"],
    ],
)
def test_vespa_search_adaptor__document_ids(fake_vespa_credentials, document_ids):
    request = SearchParameters(query_string="the", document_ids=document_ids)
    response = vespa_search(fake_vespa_credentials, request)

    # As passages are returned we need to collect and deduplicate them to get id list
    got_document_ids = []
    for fam in response.families:
        for doc in fam.hits:
            got_document_ids.append(doc.document_import_id)
    got_document_ids = list(set(got_document_ids))

    assert sorted(got_document_ids) == sorted(document_ids)


@pytest.mark.vespa
def test_vespa_search_adaptor__bad_query_string_still_works(fake_vespa_credentials):
    family_name = ' Bad " query/    '
    request = SearchParameters(query_string=family_name)
    try:
        vespa_search(fake_vespa_credentials, request)
    except Exception as e:
        assert False, f"failed with: {e}"


@pytest.mark.vespa
def test_vespa_search_adaptor__hybrid(fake_vespa_credentials):
    family_name = "Climate Change Adaptation and Low Emissions Growth Strategy by 2035"
    request = SearchParameters(query_string=family_name)
    response = vespa_search(fake_vespa_credentials, request)

    # Was the family searched for in the results.
    # Note that this is a fairly loose test
    got_family_names = []
    for fam in response.families:
        for doc in fam.hits:
            got_family_names.append(doc.family_name)
    assert family_name in got_family_names


@pytest.mark.vespa
def test_vespa_search_adaptor__exact(fake_vespa_credentials):
    query_string = "Environmental Strategy for 2014-2023"
    request = SearchParameters(query_string=query_string, exact_match=True)
    response = vespa_search(fake_vespa_credentials, request)
    got_family_names = []
    for fam in response.families:
        for doc in fam.hits:
            got_family_names.append(doc.family_name)
    # For an exact query where this term only exists in the family name, we'd expect
    # it to be the only result so can be quite specific
    assert len(set(got_family_names)) == 1
    assert got_family_names[0] == query_string

    # Conversely we'd expect nothing if the query string isnt present
    query_string = "no such string as this can be found in the test documents"
    request = SearchParameters(query_string=query_string, exact_match=True)
    response = vespa_search(fake_vespa_credentials, request)
    assert len(response.families) == 0


@pytest.mark.vespa
@patch("cpr_data_access.vespa.SENSITIVE_QUERY_TERMS", {"Government"})
def test_vespa_search_adaptor__sensitive(fake_vespa_credentials):
    request = SearchParameters(query_string="Government")
    response = vespa_search(fake_vespa_credentials, request)

    # Without being too prescriptive, we'd expect something back for this
    assert len(response.families) > 0


@pytest.mark.parametrize(
    "family_limit, max_hits_per_family",
    [
        (1, 1),
        (1, 100),
        (2, 1),
        (2, 5),
        (3, 1000),
    ],
)
@pytest.mark.vespa
def test_vespa_search_adaptor__limits(
    fake_vespa_credentials, family_limit, max_hits_per_family
):
    request = SearchParameters(
        query_string="the",
        family_ids=[],
        limit=family_limit,
        max_hits_per_family=max_hits_per_family,
    )
    response = vespa_search(fake_vespa_credentials, request)

    assert len(response.families) == family_limit
    for fam in response.families:
        assert len(fam.hits) <= max_hits_per_family
