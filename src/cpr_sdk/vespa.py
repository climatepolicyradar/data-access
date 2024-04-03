from pathlib import Path
from typing import Any, List

import yaml
from vespa.io import VespaResponse
from vespa.exceptions import VespaError

from cpr_sdk.models.search import (
    Family,
    Hit,
    SearchParameters,
    SearchResponse,
)
from cpr_sdk.embedding import Embedder
from cpr_sdk.exceptions import FetchError
from cpr_sdk.utils import dig, is_sensitive_query, load_sensitive_query_terms
from cpr_sdk.yql_builder import YQLBuilder


SENSITIVE_QUERY_TERMS = load_sensitive_query_terms()


def split_document_id(document_id: str) -> tuple[str, str, str]:
    """
    Split a document_id into its namespace, schema, and data_id components.

    IDs should be of the form: "id:namespace:schema::data_id"

    :param str document_id: a document id of the form "id:namespace:schema::data_id"
    :raises ValueError: if the document id is not of the expected form
    :return tuple[str, str, str]: the namespace, schema, and data_id components of the
        document_id
    """
    try:
        namespace_and_schema, data_id = document_id.split("::")
        _, namespace, schema = namespace_and_schema.split(":")
    except ValueError as e:
        raise ValueError(
            f'Failed to parse document id: "{document_id}". '
            'Document ids should be of the form: "id:namespace:schema::data_id"'
        ) from e
    return namespace, schema, data_id


def find_vespa_cert_paths() -> tuple[Path, Path]:
    """
    Automatically find the certificate and key files for the vespa instance

    :raises FileNotFoundError: if the .vespa directory is not found in the home
        directory, or if the application name is not found in the config.yaml file
    :return tuple[Path, Path]: The paths to the certificate and key files, respectively
    """
    vespa_directory = Path.home() / ".vespa/"
    if not vespa_directory.exists():
        raise FileNotFoundError(
            "Could not find .vespa directory in home directory. "
            "Please specify a cert_directory."
        )

    # read the config.yaml file to find the application name
    with open(vespa_directory / "config.yaml", "r", encoding="utf-8") as yaml_file:
        data = yaml.safe_load(yaml_file)
        application_name = data["application"]

    cert_directory = vespa_directory / application_name
    cert_path = list(cert_directory.glob("*cert.pem"))[0]
    key_path = list(cert_directory.glob("*key.pem"))[0]
    return cert_path, key_path


def build_vespa_request_body(
    parameters: SearchParameters, embedder: Embedder
) -> dict[str, str]:
    """Constructs the payload for a vespa query"""
    sensitive = is_sensitive_query(parameters.query_string, SENSITIVE_QUERY_TERMS)

    yql = YQLBuilder(params=parameters, sensitive=sensitive).to_str()
    vespa_request_body: dict[str, Any] = {
        "yql": yql,
        "timeout": "20",
        "ranking.softtimeout.factor": "0.7",
        "query_string": parameters.query_string,
    }

    if parameters.all_results:
        pass
    elif parameters.exact_match:
        vespa_request_body["ranking.profile"] = "exact"
    elif sensitive:
        vespa_request_body["ranking.profile"] = "hybrid_no_closeness"
    else:
        vespa_request_body["ranking.profile"] = "hybrid"
        vespa_request_body["input.query(query_embedding)"] = embedder.embed(
            parameters.query_string, normalize=False, show_progress_bar=False
        )
    return vespa_request_body


def parse_vespa_response(vespa_response: VespaResponse) -> SearchResponse:
    """
    Parse a vespa response into a SearchResponse object

    :param SearchParameters request: The user's original search request
    :param VespaResponse vespa_response: The response from the vespa instance
    :raises FetchError: if the vespa response status code is not 200, indicating an
        error in the query, or the vespa instance
    :return SearchResponse: a list of families, with response metadata
    """
    if vespa_response.status_code != 200:
        raise FetchError(
            f"Received status code {vespa_response.status_code}",
            status_code=vespa_response.status_code,
        )
    families: List[Family] = []
    root = vespa_response.json["root"]

    response_families = dig(root, "children", 0, "children", 0, "children", default=[])

    for family in response_families:
        total_passage_hits = dig(family, "fields", "count()")
        family_hits: List[Hit] = []
        passages_continuation = dig(family, "children", 0, "continuation", "next")
        prev_passages_continuation = dig(family, "children", 0, "continuation", "prev")
        for hit in dig(family, "children", 0, "children", default=[]):
            family_hits.append(Hit.from_vespa_response(response_hit=hit))
        families.append(
            Family(
                id=family["value"],
                hits=family_hits,
                total_passage_hits=total_passage_hits,
                continuation_token=passages_continuation,
                prev_continuation_token=prev_passages_continuation,
            )
        )

    next_family_continuation = dig(
        root, "children", 0, "children", 0, "continuation", "next"
    )
    prev_family_continuation = dig(
        root, "children", 0, "children", 0, "continuation", "prev"
    )
    this_family_continuation = dig(root, "children", 0, "continuation", "this")
    total_hits = dig(root, "fields", "totalCount", default=0)
    total_family_hits = dig(root, "children", 0, "fields", "count()", default=0)
    return SearchResponse(
        total_hits=total_hits,
        total_family_hits=total_family_hits,
        families=families,
        continuation_token=next_family_continuation,
        this_continuation_token=this_family_continuation,
        prev_continuation_token=prev_family_continuation,
        query_time_ms=None,
        total_time_ms=None,
    )


class VespaErrorDetails:
    """Wrapper for VespaError that parses the arguments"""

    def __init__(self, e: VespaError) -> None:
        self.e = e
        self.code = None
        self.summary = None
        self.message = None
        self.parse_args(self.e)

    def parse_args(self, e: VespaError) -> None:
        """
        Gets the details of the first error

        Args:
            e (VespaError): An error from the vespa python sdk
        """
        for arg in e.args:
            for error in arg:
                self.code = error.get("code")
                self.summary = error.get("summary")
                self.message = error.get("message")
                break

    @property
    def is_invalid_query_parameter(self) -> bool:
        """
        Checks if an error is coming from vespa on query parameters, see:

        https://github.com/vespa-engine/vespa/blob/0c55dc92a3bf889c67fac1ca855e6e33e1994904/
        container-core/src/main/java/com/yahoo/container/protect/Error.java
        """
        INVALID_QUERY_PARAMETER = 4
        return self.code == INVALID_QUERY_PARAMETER
