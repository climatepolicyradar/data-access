from datetime import datetime
from typing import List, Literal, Mapping, Optional, Sequence, Union

from pydantic import BaseModel, validator

SortOrder = Literal["ascending", "descending"]

sort_fields = {
    "date": "family_publication_ts",
    "name": "family_name",
}

SortField = Literal["date", "name"]


filter_fields = {
    "geography": "family_geography",
    "category": "family_category",
    "language": "document_languages",
    "source": "family_source",
}

FilterField = Literal["geography", "category", "language", "source"]


class SearchRequestBody(BaseModel):
    """Parameters for a search request"""

    query_string: str
    exact_match: bool = False
    limit: int = 100
    max_hits_per_family: int = 10

    keyword_filters: Optional[Mapping[FilterField, Union[str, Sequence[str]]]] = None
    year_range: Optional[tuple[Optional[int], Optional[int]]] = None

    sort_by: Optional[SortField] = None
    sort_order: SortOrder = "descending"

    continuation_token: Optional[str] = None

    @validator("query_string")
    def query_string_must_not_be_empty(cls, v):
        """Validate that the query string is not empty."""
        if v == "":
            raise ValueError("Query string must not be empty")
        return v

    @validator("year_range")
    def year_range_must_be_valid(cls, v):
        """Validate that the year range is valid."""
        if v is not None:
            if v[0] is not None and v[1] is not None:
                if v[0] > v[1]:
                    raise ValueError("Invalid year range")
        return v


class Hit(BaseModel):
    """Common model for all search result hits."""

    family_name: Optional[str]
    family_description: Optional[str]
    family_import_id: Optional[str]
    family_slug: Optional[str]
    family_category: Optional[str]
    family_publication_ts: Optional[datetime]
    family_geography: Optional[str]
    document_import_id: Optional[str]
    document_slug: Optional[str]
    document_languages: Optional[List[str]]
    document_content_type: Optional[str]
    document_cdn_object: Optional[str]
    document_source_url: Optional[str]

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
    text_block_page: Optional[int]
    text_block_coords: Optional[Sequence[tuple[float, float]]]

    @classmethod
    def from_vespa_response(cls, response_hit: dict) -> "Passage":
        """
        Create a Passage from a Vespa response hit.

        :param dict response_hit: part of a json response from Vespa
        :return Passage: a populated passage
        """
        fields = response_hit["fields"]
        family_publication_ts = fields.get("family_publication_ts", None)
        family_publication_ts = (
            datetime.fromisoformat(family_publication_ts)
            if family_publication_ts
            else None
        )
        return cls(
            family_name=fields.get("family_name", None),
            family_description=fields.get("family_description", None),
            family_import_id=fields.get("family_import_id", None),
            family_slug=fields.get("family_slug", None),
            family_category=fields.get("family_category", None),
            family_publication_ts=family_publication_ts,
            family_geography=fields.get("family_geography", None),
            document_import_id=fields.get("document_import_id", None),
            document_slug=fields.get("document_slug", None),
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


class SearchResponse(BaseModel):
    """Relevant results, and search response metadata"""

    total_hits: int
    query_time_ms: Optional[int]
    total_time_ms: Optional[int]
    families: Sequence[Family]
    continuation_token: Optional[str]
