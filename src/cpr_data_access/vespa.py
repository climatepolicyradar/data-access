from pathlib import Path
from typing import List, Sequence

import yaml
from vespa.io import VespaResponse

from cpr_data_access.models.search import Family, Hit, SearchRequestBody


def _find_vespa_cert_paths() -> tuple[Path, Path]:
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


def _build_yql(request: SearchRequestBody) -> str:
    """
    Build a YQL string for retrieving relevant, filtered, sorted results from vespa

    :param request SearchRequestBody: request object
    :return: YQL string
    """
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
        rendered_filters += " and "
        for field, values in request.keyword_filters.items():
            if not isinstance(values, list):
                values = [values]

            rendered_filters += " and ".join(
                f'({field.value} contains "{value}")' for value in values
            )

    if request.year_range:
        start, end = request.year_range
        if start:
            rendered_filters += f" and (family_publication_year >= {start})"
        if end:
            rendered_filters += f" and (family_publication_year <= {end})"

    rendered_sort = (
        f"order by {request.sort_field} {request.sort_order}"
        if request.sort_field
        else ""
    )

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
        { rendered_sort }
        limit 0
        |
        { rendered_continuation }
        all(
            group(family_import_id)
            max({request.limit})
            each(
                all(
                    max({request.max_hits_per_family})
                    each(output(summary(search_summary)))
                )
            )
        )
    """
    return " ".join(rendered_query.split())


def _parse_vespa_response(
    vespa_response: VespaResponse,
) -> Sequence[Family]:
    families: List[Family] = []
    root = vespa_response.json["root"]
    response_families = root["children"][0]["children"][0]["children"]
    for family in response_families:
        family_hits: List[Hit] = []
        for hit in family["children"][0]["children"]:
            family_hits.append(Hit.from_vespa_response(response_hit=hit))
        families.append(Family(id=family["value"], hits=family_hits))

    return families
