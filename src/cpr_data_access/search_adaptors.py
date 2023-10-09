"""Adaptors for searching CPR data"""

import time
from typing import Optional
from cpr_data_access.embedding import ModelName, Embedder
from pydantic import BaseModel
from vespa.application import Vespa

from cpr_data_access.models.search import (
    ResponseMatchBase,
    SearchRequestBody,
    SearchResponse,
    SearchResponseFamily,
)
from cpr_data_access.vespa import (
    _build_yql,
    _find_vespa_cert_directory,
    get_by_id_from_vespa,
)
from cpr_data_access.embedding import embed


class SearchAdapter(BaseModel):
    """
    Base class for all search adapters.
    """

    def search(self, request: SearchRequestBody) -> SearchResponse:
        """
        Search a dataset

        :param request: SearchRequestBody
        :return SearchResponse: a list of families, with metadata
        """
        raise NotImplementedError

    def get_by_id(self, document_id: str) -> SearchResponseFamily:
        """
        Get a single document by its id

        :param document_id: document id
        :return SearchResponseFamily: a single document family
        """
        raise NotImplementedError


class VespaSearchAdapter(SearchAdapter):
    """
    Search within a vespa instance

    :param str instance_url: url of the vespa instance
    :param str cert_directory: path to the directory containing the cert and key files.
    :param str model: name of the model to use for embedding
    """

    def __init__(self, instance_url: str, cert_directory: Optional[str], model_name: ModelName):
        self.instance_url = instance_url
        if cert_directory is None:
            cert_directory = _find_vespa_cert_directory()

        cert_path = list(cert_directory.glob("*cert.pem"))[0]
        key_path = list(cert_directory.glob("*key.pem"))[0]

        self.client = Vespa(url=instance_url, cert=cert_path, key=key_path)
        self.embedder = Embedder(model_name)


    def search(self, client: Vespa, request: SearchRequestBody) -> SearchResponse:
        """
        Search a vespa instance

        :param client: Vespa client
        :param request: SearchRequestBody
        :return SearchResponse: a list of families, with metadata
        """
        total_time_start = time.time() * 1000

        yql_body = _build_yql(request)
        vespa_request_body = {
                "yql": yql_body,
                "hits": request.limit,
                "offset": request.offset,
                "ranking.profile": "exact"
            }
        if not request.exact_match:
            embedding = self.embedder.embed(request.query_string, normalize=True)
            vespa_request_body["ranking.features.query(query_embedding)"] = embedding
            vespa_request_body["ranking.profile"] = "hybrid"
            


        query_time_start = time.time() * 1000
        vespa_response = client.query(body=vespa_request_body,).json
        query_time_end = time.time() * 1000

        families = [
            SearchResponseFamily(
                category="",
                date="",
                description="",
                geography="",
                last_updated_date="",
                metadata={},
                name="",
                slug="",
                source="",
                documents=[
                    ResponseMatchBase(
                        category="",
                        cdn_object="",
                        content_type="",
                        date="",
                        description="",
                        geography="",
                        id="",
                        name="",
                        sectors=[],
                        slug="",
                        source_url="",
                        source="",
                        type="",
                    )
                    for document in family["children"][0]["children"][0]["children"][0][
                        "children"
                    ]
                ],
            )
            for family in vespa_response["root"]["children"][0]["children"]
        ]

        total_time_end = time.time() * 1000

        response = SearchResponse(
            hits=vespa_response["root"]["fields"]["totalCount"],
            total_time_ms=total_time_end - total_time_start,
            query_time_ms=query_time_end - query_time_start,
            families=families,
        )

        return response

    def get_by_id(self, document_id: str) -> SearchResponseFamily:
        """
        Get a single document by its id

        :param document_id: document id
        :return SearchResponseFamily: a single document family
        """
        return get_by_id_from_vespa(self.client, document_id)
