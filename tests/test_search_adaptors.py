from unittest.mock import patch
from timeit import timeit
from typing import Mapping

import pytest

from cpr_data_access.search_adaptors import VespaSearchAdapter
from cpr_data_access.models.search import (
    SearchParameters,
    SearchResponse,
    sort_fields,
    Document,
    Passage,
)

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


def profile_search(
    fake_vespa_credentials, params: Mapping[str, str], n: int = 25
) -> float:
    t = timeit(
        lambda: vespa_search(fake_vespa_credentials, SearchParameters(**params)),
        number=n,
    )
    avg_ms = (t / n) * 1000
    return avg_ms


@pytest.mark.vespa
def test_vespa_search_adaptor__works(fake_vespa_credentials):
    request = SearchParameters(query_string="the")
    response = vespa_search(fake_vespa_credentials, request)

    assert len(response.families) == response.total_family_hits == 3
    assert response.query_time_ms < response.total_time_ms
    total_passage_count = sum([f.total_passage_hits for f in response.families])
    assert total_passage_count == response.total_hits


@pytest.mark.parametrize(
    "params",
    (
        {"query_string": "the"},
        {"query_string": "climate change"},
        {"query_string": "fuel", "exact_search": True},
        {"all_results": True, "documents_only": True},
        {"query_string": "fuel", "sort_by": "date", "sort_order": "asc"},
        {"query_string": "forest", "filter": {"family_category": "CCLW"}},
    ),
)
@pytest.mark.vespa
def test_vespa_search_adaptor__is_fast_enough(fake_vespa_credentials, params):
    MAX_SPEED_MS = 750

    avg_ms = profile_search(fake_vespa_credentials, params=params)
    assert avg_ms <= MAX_SPEED_MS


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
def test_vespa_search_adaptor__all(fake_vespa_credentials):
    request = SearchParameters(query_string="", all_results=True)
    response = vespa_search(fake_vespa_credentials, request)
    assert len(response.families) == response.total_family_hits

    # Filtering should still work
    family_id = "CCLW.family.i00000003.n0000"
    request = SearchParameters(
        query_string="", all_results=True, family_ids=[family_id]
    )
    response = vespa_search(fake_vespa_credentials, request)
    assert len(response.families) == 1
    assert response.families[0].id == family_id


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


@pytest.mark.vespa
def test_vespa_search_adaptor__continuation_tokens__families(fake_vespa_credentials):
    query_string = "the"
    limit = 2
    max_hits_per_family = 3

    # Make an initial request to get continuation tokens and results
    request = SearchParameters(
        query_string=query_string,
        limit=limit,
        max_hits_per_family=max_hits_per_family,
    )
    response = vespa_search(fake_vespa_credentials, request)
    first_family_ids = [f.id for f in response.families]
    family_continuation = response.continuation_token
    assert len(response.families) == 2
    assert response.total_family_hits == 3

    # Family increment
    request = SearchParameters(
        query_string=query_string,
        limit=limit,
        max_hits_per_family=max_hits_per_family,
        continuation_tokens=[family_continuation],
    )
    response = vespa_search(fake_vespa_credentials, request)
    prev_family_continuation = response.prev_continuation_token
    assert len(response.families) == 1
    assert response.total_family_hits == 3

    # Family should have changed
    second_family_ids = [f.id for f in response.families]
    assert sorted(first_family_ids) != sorted(second_family_ids)
    # As this is the end of the results we also expect no more tokens
    assert response.continuation_token is None

    # Using prev_continuation_token give initial results
    request = SearchParameters(
        query_string=query_string,
        limit=limit,
        max_hits_per_family=max_hits_per_family,
        continuation_tokens=[prev_family_continuation],
    )
    response = vespa_search(fake_vespa_credentials, request)
    prev_family_ids = [f.id for f in response.families]
    assert prev_family_ids == first_family_ids


@pytest.mark.vespa
def test_vespa_search_adaptor__continuation_tokens__passages(fake_vespa_credentials):
    query_string = "the"
    limit = 1
    max_hits_per_family = 10

    # Make an initial request to get continuation tokens and results for comparison
    request = SearchParameters(
        query_string=query_string,
        limit=limit,
        max_hits_per_family=max_hits_per_family,
    )
    initial_response = vespa_search(fake_vespa_credentials, request)

    # Collect family & hits for comparison later
    initial_family_id = initial_response.families[0].id
    initial_passages = [h.text_block_id for h in initial_response.families[0].hits]

    this_continuation = initial_response.this_continuation_token
    passage_continuation = initial_response.families[0].continuation_token

    # Passage Increment
    request = SearchParameters(
        query_string=query_string,
        limit=limit,
        max_hits_per_family=max_hits_per_family,
        continuation_tokens=[this_continuation, passage_continuation],
    )
    response = vespa_search(fake_vespa_credentials, request)
    prev_passage_continuation = response.families[0].prev_continuation_token

    # Family should not have changed
    assert response.families[0].id == initial_family_id

    # But Passages SHOULD have changed
    new_passages = sorted([h.text_block_id for h in response.families[0].hits])
    assert sorted(new_passages) != sorted(initial_passages)

    # Previous passage continuation gives initial results
    request = SearchParameters(
        query_string=query_string,
        limit=limit,
        max_hits_per_family=max_hits_per_family,
        continuation_tokens=[this_continuation, prev_passage_continuation],
    )
    response = vespa_search(fake_vespa_credentials, request)
    assert response.families[0].id == initial_family_id
    prev_passages = sorted([h.text_block_id for h in response.families[0].hits])
    assert sorted(prev_passages) != sorted(new_passages)
    assert sorted(prev_passages) == sorted(initial_passages)


@pytest.mark.vespa
def test_vespa_search_adaptor__continuation_tokens__families_and_passages(
    fake_vespa_credentials,
):
    query_string = "the"
    limit = 1
    max_hits_per_family = 30

    # Make an initial request to get continuation tokens and results for comparison
    request_one = SearchParameters(
        query_string=query_string,
        limit=limit,
        max_hits_per_family=max_hits_per_family,
    )
    response_one = vespa_search(fake_vespa_credentials, request_one)

    # Increment Families
    request_two = SearchParameters(
        query_string=query_string,
        limit=limit,
        max_hits_per_family=max_hits_per_family,
        continuation_tokens=[response_one.continuation_token],
    )
    response_two = vespa_search(fake_vespa_credentials, request_two)

    # Then Increment Passages Twice

    request_three = SearchParameters(
        query_string=query_string,
        limit=limit,
        max_hits_per_family=max_hits_per_family,
        continuation_tokens=[
            response_two.this_continuation_token,
            response_two.families[0].continuation_token,
        ],
    )
    response_three = vespa_search(fake_vespa_credentials, request_three)

    request_four = SearchParameters(
        query_string=query_string,
        limit=limit,
        max_hits_per_family=max_hits_per_family,
        continuation_tokens=[
            response_two.this_continuation_token,
            response_three.families[0].continuation_token,
        ],
    )
    response_four = vespa_search(fake_vespa_credentials, request_four)

    # All of these should have different passages from each other
    assert (
        sorted([h.text_block_id for h in response_one.families[0].hits])
        != sorted([h.text_block_id for h in response_two.families[0].hits])
        != sorted([h.text_block_id for h in response_three.families[0].hits])
        != sorted([h.text_block_id for h in response_four.families[0].hits])
    )


@pytest.mark.parametrize("sort_by", sort_fields.keys())
@pytest.mark.vespa
def test_vespa_search_adapter_sorting(fake_vespa_credentials, sort_by):
    ascend = vespa_search(
        fake_vespa_credentials,
        SearchParameters(query_string="the", sort_by=sort_by, sort_order="ascending"),
    )
    descend = vespa_search(
        fake_vespa_credentials,
        SearchParameters(query_string="the", sort_by=sort_by, sort_order="descending"),
    )

    assert ascend != descend


@pytest.mark.vespa
def test_vespa_search_no_passages_search(fake_vespa_credentials):
    no_passages = vespa_search(
        fake_vespa_credentials,
        SearchParameters(all_results=True, documents_only=True),
    )
    for family in no_passages.families:
        for hit in family.hits:
            assert isinstance(hit, Document)

    with_passages = vespa_search(
        fake_vespa_credentials,
        SearchParameters(all_results=True),
    )
    found_a_passage = False
    for family in with_passages.families:
        for hit in family.hits:
            if isinstance(hit, Passage):
                found_a_passage = True
    assert found_a_passage
