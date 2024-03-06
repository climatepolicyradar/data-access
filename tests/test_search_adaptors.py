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
    request = SearchParameters(query_string="forest fires")
    vespa_search(fake_vespa_credentials, request)


@pytest.mark.vespa
@pytest.mark.parametrize(
    "family_ids",
    [
        ["CCLW.family.i00000003.n0000"],
        ["CCLW.family.10014.0"],
        ["CCLW.family.i00000003.n0000", "CCLW.family.10014.0"],
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
