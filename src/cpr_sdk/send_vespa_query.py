from cpr_sdk.search_adaptors import VespaSearchAdapter
from cpr_sdk.models.search import SearchParameters

search_params = SearchParameters(
    query_string="one one one ",
    exact_match=False,
    limit=150,
    max_hits_per_family=10,
    keyword_filters={
    },
    year_range=(1947, 2023),
    sort_by=None,
    sort_order="descending"
)


vespa_client = VespaSearchAdapter(
    instance_url="https://b40222df.c69aaec0.z.vespa-app.cloud/",
    cert_directory="/Users/markcottam/PycharmProjects/data-access/src/cpr_data_access/pipeline_certs",
    embedder=None
)

response = vespa_client.get_by_id("id:doc_search:family_document::CCLW.family.9744.0")

response = vespa_client.search(parameters=search_params)

breakpoint()
print(response)