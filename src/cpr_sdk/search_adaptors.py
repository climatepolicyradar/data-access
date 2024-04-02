"""Adaptors for searching CPR data"""
import time
from abc import ABC
from pathlib import Path
from typing import Optional
import logging

from cpr_sdk.embedding import Embedder
from cpr_sdk.exceptions import DocumentNotFoundError, FetchError, QueryError
from cpr_sdk.models.search import Hit, SearchParameters, SearchResponse
from cpr_sdk.vespa import (
    build_vespa_request_body,
    find_vespa_cert_paths,
    parse_vespa_response,
    split_document_id,
    VespaErrorDetails,
)
from requests.exceptions import HTTPError
from vespa.application import Vespa
from vespa.exceptions import VespaError

LOGGER = logging.getLogger(__name__)


class SearchAdapter(ABC):
    """Base class for all search adapters."""

    def search(self, parameters: SearchParameters) -> SearchResponse:
        """
        Search a dataset

        :param SearchParameters parameters: a search request object
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
    :param Optional[str] cert_directory: path to the directory containing the
        cert and key files for the given instance
    :param Embedder embedder: a configured embedder to use for embedding queries.
        This should match the embedding model used to embed text in the vespa index.
    """

    def __init__(
        self,
        instance_url: str,
        cert_directory: Optional[str] = None,
        embedder: Optional[Embedder] = None,
    ):
        self.instance_url = instance_url
        if cert_directory is None:
            cert_path, key_path = find_vespa_cert_paths()
        else:
            cert_path = Path(cert_directory) / "cert.pem"
            key_path = Path(cert_directory) / "key.pem"

        self.client = Vespa(url=instance_url, cert=str(cert_path), key=str(key_path))
        self.embedder = embedder or Embedder()

    def search(self, parameters: SearchParameters) -> SearchResponse:
        """
        Search a vespa instance

        :param SearchParameters parameters: a search request object
        :return SearchResponse: a list of families, with response metadata
        """
        total_time_start = time.time()
        vespa_request_body = build_vespa_request_body(parameters, self.embedder)
        query_time_start = time.time()
        try:
            vespa_response = self.client.query(body=vespa_request_body)
        except VespaError as e:
            err_details = VespaErrorDetails(e)
            if err_details.is_invalid_query_parameter:
                LOGGER.error(err_details.message)
                raise QueryError(err_details.summary)
            else:
                raise e
        query_time_end = time.time()

        response = parse_vespa_response(vespa_response=vespa_response)

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
        namespace, schema, data_id = split_document_id(document_id)
        try:
            vespa_response = self.client.get_data(
                namespace=namespace, schema=schema, data_id=data_id
            )
        except HTTPError as e:
            if e.response is not None:
                status_code = e.response.status_code
            else:
                status_code = "Unknown"
            if status_code == 404:
                raise DocumentNotFoundError(document_id) from e
            else:
                raise FetchError(
                    f"Received status code {status_code} when fetching "
                    f"document {document_id}",
                    status_code=status_code,
                ) from e

        return Hit.from_vespa_response(vespa_response.json)
