"""Adaptors for searching CPR data"""
from pathlib import Path
import time
from abc import ABC
from typing import Optional

from requests.exceptions import HTTPError
from vespa.application import Vespa

from cpr_data_access.embedding import Embedder, ModelName
from cpr_data_access.models.search import Hit, SearchRequestBody, SearchResponse
from cpr_data_access.vespa import (
    _build_yql,
    _find_vespa_cert_paths,
    _parse_vespa_response,
    _split_document_id,
)


class SearchAdapter(ABC):
    """Base class for all search adapters."""

    def search(self, request: SearchRequestBody) -> SearchResponse:
        """
        Search a dataset

        :param SearchRequestBody request: a search request object
        :return SearchResponse: a list of parent families, each containing relevant
            child documents and passages
        """
        raise NotImplementedError

    def get_by_id(self, document_id: str) -> SearchResponse:
        """
        Get a single document by its id

        :param str document_id: document id
        :return Hit: a single document or passage
        """
        raise NotImplementedError


class VespaSearchAdapter(SearchAdapter):
    """
    Search within a vespa instance

    :param str instance_url: url of the vespa instance
    :param Optional[str] cert_directory: path to the directory containing the cert and key files
        for the given instance
    :param str model: name of the model to use for embedding queries. This should match
        the name of the model used to embed text in the vespa index.
    """

    def __init__(
        self,
        instance_url: str,
        cert_directory: Optional[str] = None,
        model_name: ModelName = "msmarco-distilbert-dot-v5",
    ):
        self.instance_url = instance_url
        if cert_directory is None:
            cert_path, key_path = _find_vespa_cert_paths()
        else:
            cert_path = Path(cert_directory) / "cert.pem"
            key_path = Path(cert_directory) / "key.pem"

        self.client = Vespa(url=instance_url, cert=str(cert_path), key=str(key_path))
        self.embedder = Embedder(model_name)

    def search(self, request: SearchRequestBody) -> SearchResponse:
        """
        Search a vespa instance

        :param SearchRequestBody request: a search request object
        :return SearchResponse: a list of families, with response metadata
        """
        total_time_start = time.time()

        vespa_request_body = {"yql": _build_yql(request)}
        if request.exact_match:
            vespa_request_body["ranking.profile"] = "exact"
        else:
            vespa_request_body["ranking.profile"] = "hybrid"
            embedding = self.embedder.embed(request.query_string, normalize=True)
            vespa_request_body["input.query(query_embedding)"] = embedding

        query_time_start = time.time()
        vespa_response = self.client.query(body=vespa_request_body)
        query_time_end = time.time()

        response = _parse_vespa_response(request=request, vespa_response=vespa_response)

        response.query_time_ms = int((query_time_end - query_time_start) * 1000)
        response.total_time_ms = int((time.time() - total_time_start) * 1000)

        return response

    def get_by_id(self, document_id: str) -> Hit:
        """
        Get a single document by its id

        :param str document_id: IDs should look something like
            "id:doc_search:family_document::CCLW.family.11171.0"
            or
            "id:doc_search:document_passage::UNFCCC.party.1060.0.3743"
        :return Hit: a single document or passage
        """
        namespace, schema, data_id = _split_document_id(document_id)
        try:
            vespa_response = self.client.get_data(
                namespace=namespace, schema=schema, data_id=data_id
            )
        except HTTPError as e:
            if e.response.status_code == 404:
                raise ValueError(f'No document found with id "{document_id}"') from e
            else:
                raise ValueError(
                    f'Something went wrong when fetching document with id "{document_id}"'
                ) from e

        return Hit.from_vespa_response(vespa_response.json)