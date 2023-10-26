from pathlib import Path
from typing import List

import yaml
from vespa.io import VespaResponse

from cpr_data_access.models.search import (
    Family,
    Hit,
    SearchParameters,
    SearchResponse,
    filter_fields,
    sort_fields,
)


def split_document_id(document_id: str) -> tuple[str, str, str]:
    try:
        namespace_and_schema, data_id = document_id.split("::")
        _, namespace, schema = namespace_and_schema.split(":")
    except ValueError as e:
        raise ValueError(
            f'Document id "{document_id}" is not in the expected format'
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


def sanitize(user_input: str) -> str:
    """
    Sanitize user input strings to limit possible YQL injection attacks

    :param str user_input: a potentially hazardous user input string
    :return str: sanitized user input string
    """
    # in the generated YQL string, user inputs are wrapped in double quotes. We should
    # therefore remove any double quotes from the user inputs to avoid early terminations,
    # which could allow for subsequent injections
    user_input = user_input.replace('"', "")

    # remove any extra whitespace from the user input string
    user_input = " ".join(user_input.split())

    return user_input


def build_yql(request: SearchParameters) -> str:
    """
    Build a YQL string for retrieving relevant, filtered, sorted results from vespa

    :param SearchParameters request: a search request object comprised of the user's
        search parameters
    :return str: formatted YQL which incorporates the user's search parameters
    """
    request.query_string = sanitize(request.query_string)

    if request.exact_match:
        rendered_query_string_match = f"""
            where (
                (family_name contains({{stem: false}}"{request.query_string}")) or
                (family_description contains({{stem: false}}"{request.query_string}")) or
                (text_block contains ({{stem: false}}"{request.query_string}"))
            )
        """
    else:
        rendered_query_string_match = f"""
            where ((
                {{"targetHits": 1000}} weakAnd(
                    family_name contains "{ request.query_string }",
                    family_description contains "{ request.query_string }",
                    text_block contains "{ request.query_string }"
                )
            ) or (
                [{{"targetNumHits": 1000}}]
                nearestNeighbor(family_description_embedding,query_embedding)
            ) or (
                [{{"targetNumHits": 1000}}]
                nearestNeighbor(text_embedding,query_embedding)
            ))
        """

    rendered_filters = ""
    if request.keyword_filters:
        filters = []
        for field_key, values in request.keyword_filters.items():
            field_name = filter_fields[field_key]
            for value in values:
                filters.append(f'({field_name} contains "{sanitize(value)}")')
        rendered_filters = " and " + " and ".join(filters)

    if request.year_range:
        start, end = request.year_range
        if start:
            rendered_filters += f" and (family_publication_year >= {start})"
        if end:
            rendered_filters += f" and (family_publication_year <= {end})"

    rendered_continuation = (
        f"{{ 'continuations':['{request.continuation_token}'] }}"
        if request.continuation_token
        else ""
    )

    rendered_query = f"""
        select *
        from sources family_document, document_passage
        { rendered_query_string_match }
        { rendered_filters }
        limit 0
        |
        { rendered_continuation }
        all(
            group(family_import_id)
            max({request.limit})
            each(
                max({request.max_hits_per_family})
                each(output(summary(search_summary))
            )
        )
    )
    """

    return " ".join(rendered_query.split())


def parse_vespa_response(
    request: SearchParameters,
    vespa_response: VespaResponse,
) -> SearchResponse:
    """
    Parse a vespa response into a SearchResponse object

    :param SearchParameters request: The user's original search request
    :param VespaResponse vespa_response: The response from the vespa instance
    :raises ValueError: if the vespa response status code is not 200, indicating an
        error in the query, or the vespa instance
    :return SearchResponse: a list of families, with response metadata
    """
    if vespa_response.status_code != 200:
        raise ValueError(
            f"Vespa response status code was {vespa_response.status_code}, "
            f"expected 200"
        )
    families: List[Family] = []
    root = vespa_response.json["root"]
    response_families = root["children"][0]["children"][0]["children"]

    for family in response_families:
        family_hits: List[Hit] = []
        for hit in family.get("children", [{}])[0].get("children", []):
            family_hits.append(Hit.from_vespa_response(response_hit=hit))
        families.append(Family(id=family["value"], hits=family_hits))

    # For now, we can't sort our results natively in vespa because sort orders are
    # applied _before_ grouping. We're sorting here instead.
    if request.sort_by:
        # pyright: reportGeneralTypeIssues=none
        families.sort(
            key=lambda f: getattr(f.hits[0], sort_fields[request.sort_by]),
            reverse=request.sort_order == "descending",
        )

    next_family_continuation_token = (
        vespa_response.json["root"]["children"][0]
        .get("continuation", {})
        .get("next", None)
    )

    return SearchResponse(
        total_hits=vespa_response.json["root"]["fields"]["totalCount"],
        families=families,
        continuation_token=next_family_continuation_token,
        query_time_ms=None,
        total_time_ms=None,
    )
