from pathlib import Path

import yaml
from vespa.application import Vespa

from cpr_data_access.models.search import SearchRequestBody
from cpr_data_access.parser_models import BaseParserOutput


def get_by_id_from_vespa(client: Vespa, document_id: str) -> BaseParserOutput:
    """
    Get a document by its id from vespa

    :param client: Vespa client
    :param document_id: document id
    :return: parser output
    """

    results = client.get_data(data_id=document_id).json()

    if len(results) == 0:
        raise ValueError(f"No document found with id {document_id}")

    return BaseParserOutput.parse_raw(results[0])


def _find_vespa_cert_directory() -> Path:
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
    return cert_directory


def _build_yql(request: SearchRequestBody, target_num_hits: int = 1000) -> str:
    """
    Build a YQL string for retrieving relevant, filtered, sorted results from vespa

    :param request: SearchRequestBody
    :param target_num_hits: rough number of hits to return
    :return: YQL string
    """
    if request.exact_match:
        rendered_query_string_match = f"""
            where (
                name contains ({{stem: false}}\"{ request.query_string }\"),
                description contains ({{stem: false}}\"{ request.query_string }\")
            )
        """
    else:
        rendered_query_string_match = f"""
            where
            (
                {{"targetHits": {target_num_hits}}}weakAnd(
                    name contains "{ request.query_string }",
                    description contains "{ request.query_string }"
                )
            ) or 
            ((
                [{{"targetNumHits":{target_num_hits}}}]
                nearestNeighbor(description_embedding,query_embedding
            ))
        """

    rendered_filters = ""
    if request.keyword_filters:
        rendered_filters = "and " + " and ".join(
            f'{field} contains "{value}"'
            for field, value in request.keyword_filters.items()
        )

    if request.year_range:
        start, end = request.year_range
        if start:
            rendered_filters += f" and year >= {start}"
        if end:
            rendered_filters += f" and year <= {end}"

    rendered_sort = (
        f"order by {request.sort_field} {request.sort_order}"
        if request.sort_field
        else ""
    )

    rendered_query = f"""
        select *
        from sources family_document, document_passage
        { rendered_query_string_match }
        { rendered_filters }
        limit 0
        |
        all(
            group(family_import_id)
            each(
                all(
                    group(document_import_id)
                    each(
                        max({ request.max_passages_per_doc })
                        each(output(summary()))
                    )
                )
            )
        )
        { rendered_sort }
    """

    return rendered_query
