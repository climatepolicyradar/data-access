import pytest

from cpr_data_access.search_adaptors import VespaSearchAdapter
from cpr_data_access.models.search import SearchParameters

from conftest import VESPA_TEST_SEARCH_URL


@pytest.mark.vespa
def test_vespa_search_adapter():
    try:
        adaptor = VespaSearchAdapter(instance_url=VESPA_TEST_SEARCH_URL)
        request = SearchParameters(query_string="forest fires")
        adaptor.search(request)
    except Exception as e:
        pytest.fail(f"Vespa query failed: {e}")
