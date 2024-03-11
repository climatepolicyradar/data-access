from datetime import datetime
import re
from typing import List, Mapping, Optional, Sequence, Union

from pydantic import BaseModel, field_validator

from cpr_data_access.exceptions import QueryError

sort_orders = ["ascending", "descending"]

sort_fields = {
    "date": "family_publication_ts",
    "name": "family_name",
}

filter_fields = {
    "geography": "family_geography",
    "category": "family_category",
    "language": "document_languages",
    "source": "family_source",
}

_ID_ELEMENT = r"[a-zA-Z0-9]+([-_]?[a-zA-Z0-9]+)*"
ID_PATTERN = re.compile(rf"{_ID_ELEMENT}\.{_ID_ELEMENT}\.{_ID_ELEMENT}\.{_ID_ELEMENT}")


class SearchParameters(BaseModel):
    """Parameters for a search request"""

    query_string: str
    exact_match: bool = False
    limit: int = 100
    max_hits_per_family: int = 10

    family_ids: Optional[Sequence[str]] = None
    document_ids: Optional[Sequence[str]] = None

    keyword_filters: Optional[Mapping[str, Union[str, Sequence[str]]]] = None
    year_range: Optional[tuple[Optional[int], Optional[int]]] = None

    sort_by: Optional[str] = None
    sort_order: str = "descending"

    continuation_token: Optional[str] = None

    @field_validator("query_string")
    def query_string_must_not_be_empty(cls, query_string):
        """Validate that the query string is not empty."""
        if query_string == "":
            raise QueryError("query_string must not be empty")
        return query_string

    @field_validator("family_ids", "document_ids")
    def ids_must_fit_pattern(cls, ids):
        """
        Validate that the family and document ids are ids.

        Example ids:
            CCLW.document.i00000004.n0000
            CCLW.family.i00000003.n0000
            CCLW.executive.10014.4470
            CCLW.family.10014.0
        """
        if ids:
            for _id in ids:
                if not re.fullmatch(ID_PATTERN, _id):
                    raise QueryError(f"id seems invalid: {_id}")
        return ids

    @field_validator("year_range")
    def year_range_must_be_valid(cls, year_range):
        """Validate that the year range is valid."""
        if year_range is not None:
            if year_range[0] is not None and year_range[1] is not None:
                if year_range[0] > year_range[1]:
                    raise QueryError(
                        "The first supplied year must be less than or equal to the "
                        f"second supplied year. Received: {year_range}"
                    )
        return year_range

    @field_validator("sort_by")
    def sort_by_must_be_valid(cls, sort_by):
        """Validate that the sort field is valid."""
        if sort_by is not None:
            if sort_by not in sort_fields:
                raise QueryError(
                    f"Invalid sort field: {sort_by}. sort_by must be one of: "
                    f"{list(sort_fields.keys())}"
                )
        return sort_by

    @field_validator("sort_order")
    def sort_order_must_be_valid(cls, sort_order):
        """Validate that the sort order is valid."""
        if sort_order not in ["ascending", "descending"]:
            raise QueryError(
                f"Invalid sort order: {sort_order}. sort_order must be one of: "
                f"{sort_orders}"
            )
        return sort_order

    @field_validator("keyword_filters")
    def keyword_filters_must_be_valid(cls, keyword_filters):
        """Validate that the keyword filters are valid."""
        if keyword_filters is not None:
            for field_key, values in keyword_filters.items():
                if field_key not in filter_fields.values():
                    raise QueryError(
                        f"Invalid keyword filter: {field_key}. keyword_filters must be "
                        f"a subset of: {list(filter_fields.values())}"
                    )

                # convert single values to lists to make things easier later on
                if not isinstance(values, list):
                    keyword_filters[field_key] = [values]

                for value in keyword_filters[field_key]:
                    if not isinstance(value, str):
                        raise QueryError(
                            "Invalid keyword filter value: "
                            f"{{{field_key}: {value}}}. "
                            "Keyword filter values must be strings."
                        )

        return keyword_filters


class Hit(BaseModel):
    """Common model for all search result hits."""

    family_name: Optional[str] = None
    family_description: Optional[str] = None
    family_source: Optional[str] = None
    family_import_id: Optional[str] = None
    family_slug: Optional[str] = None
    family_category: Optional[str] = None
    family_publication_ts: Optional[datetime] = None
    family_geography: Optional[str] = None
    document_import_id: Optional[str] = None
    document_slug: Optional[str] = None
    document_languages: Optional[List[str]] = None
    document_content_type: Optional[str] = None
    document_cdn_object: Optional[str] = None
    document_source_url: Optional[str] = None

    @classmethod
    def from_vespa_response(cls, response_hit: dict) -> "Hit":
        """
        Create a Hit from a Vespa response hit.

        :param dict response_hit: part of a json response from Vespa
        :raises ValueError: if the response type is unknown
        :return Hit: an individual document or passage hit
        """
        # vespa structures its response differently depending on the api endpoint
        # for searches, the response should contain a sddocname field
        response_type = response_hit.get("fields", {}).get("sddocname")
        if response_type is None:
            # for get_by_id, the response should contain an id field
            response_type = response_hit["id"].split(":")[2]

        if response_type == "family_document":
            hit = Document.from_vespa_response(response_hit=response_hit)
        elif response_type == "document_passage":
            hit = Passage.from_vespa_response(response_hit=response_hit)
        else:
            raise ValueError(f"Unknown response type: {response_type}")
        return hit


class Document(Hit):
    """A document search result hit."""

    @classmethod
    def from_vespa_response(cls, response_hit: dict) -> "Document":
        """
        Create a Document from a Vespa response hit.

        :param dict response_hit: part of a json response from Vespa
        :return Document: a populated document
        """
        fields = response_hit["fields"]
        family_publication_ts = fields.get("family_publication_ts", None)
        family_publication_ts = (
            datetime.fromisoformat(family_publication_ts)
            if family_publication_ts
            else None
        )
        return cls(
            family_name=fields.get("family_name"),
            family_description=fields.get("family_description"),
            family_source=fields.get("family_source"),
            family_import_id=fields.get("family_import_id"),
            family_slug=fields.get("family_slug"),
            family_category=fields.get("family_category"),
            family_publication_ts=family_publication_ts,
            family_geography=fields.get("family_geography"),
            document_import_id=fields.get("document_import_id"),
            document_slug=fields.get("document_slug"),
            document_languages=fields.get("document_languages", []),
            document_content_type=fields.get("document_content_type"),
            document_cdn_object=fields.get("document_cdn_object"),
            document_source_url=fields.get("document_source_url"),
        )


class Passage(Hit):
    """A passage search result hit."""

    text_block: str
    text_block_id: str
    text_block_type: str
    text_block_page: Optional[int] = None
    text_block_coords: Optional[Sequence[tuple[float, float]]] = None

    @classmethod
    def from_vespa_response(cls, response_hit: dict) -> "Passage":
        """
        Create a Passage from a Vespa response hit.

        :param dict response_hit: part of a json response from Vespa
        :return Passage: a populated passage
        """
        fields = response_hit["fields"]
        family_publication_ts = fields.get("family_publication_ts")
        family_publication_ts = (
            datetime.fromisoformat(family_publication_ts)
            if family_publication_ts
            else None
        )
        return cls(
            family_name=fields.get("family_name"),
            family_description=fields.get("family_description"),
            family_source=fields.get("family_source"),
            family_import_id=fields.get("family_import_id"),
            family_slug=fields.get("family_slug"),
            family_category=fields.get("family_category"),
            family_publication_ts=family_publication_ts,
            family_geography=fields.get("family_geography"),
            document_import_id=fields.get("document_import_id"),
            document_slug=fields.get("document_slug"),
            document_languages=fields.get("document_languages", []),
            document_content_type=fields.get("document_content_type"),
            document_cdn_object=fields.get("document_cdn_object"),
            document_source_url=fields.get("document_source_url"),
            text_block=fields["text_block"],
            text_block_id=fields["text_block_id"],
            text_block_type=fields["text_block_type"],
            text_block_page=fields.get("text_block_page"),
            text_block_coords=fields.get("text_block_coords"),
        )


class Family(BaseModel):
    """A family containing relevant documents and passages."""

    id: str
    hits: Sequence[Hit]
    total_passage_hits: int = 0


class SearchResponse(BaseModel):
    """Relevant results, and search response metadata"""

    total_hits: int
    total_family_hits: int = 0
    query_time_ms: Optional[int] = None
    total_time_ms: Optional[int] = None
    families: Sequence[Family]
    continuation_token: Optional[str] = None
