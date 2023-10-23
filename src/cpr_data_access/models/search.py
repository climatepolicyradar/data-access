from datetime import datetime
from typing import List, Literal, Mapping, Optional, Sequence, Union

from pydantic import BaseModel

sort_orders = {
    "ascending": "asc",
    "descending": "desc",
}

SortOrder = Literal[tuple(sort_orders.keys())]

sort_fields = {
    "date": "family_publication_ts",
    "name": "family_name",
}

SortField = Literal[tuple(sort_fields.keys())]


filter_fields = {
    "geography": "family_geography",
    "category": "family_category",
    "language": "document_languages",
    "source": "family_source",
}

FilterField = Literal[tuple(filter_fields.keys())]


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


class Hit(BaseModel):
    """Common model for all search result hits."""

    family_name: str
    family_description: str
    family_import_id: str
    family_slug: str
    family_category: str
    family_publication_ts: datetime
    family_geography: str
    document_import_id: str
    document_slug: str
    document_languages: List[str]
    document_content_type: Optional[str]
    document_cdn_object: Optional[str]
    document_source_url: Optional[str]

    @classmethod
    def from_vespa_response(cls, response_hit) -> "Hit":
        response_type = response_hit["fields"]["sddocname"]
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
    def from_vespa_response(cls, response_hit) -> "Document":
        fields = response_hit["fields"]
        return cls(
            family_name=fields["family_name"],
            family_description=fields["family_description"],
            family_import_id=fields["family_import_id"],
            family_slug=fields["family_slug"],
            family_category=fields["family_category"],
            family_publication_ts=datetime.fromisoformat(
                fields["family_publication_ts"]
            ),
            family_geography=fields["family_geography"],
            document_import_id=fields["document_import_id"],
            document_slug=fields["document_slug"],
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
    def from_vespa_response(cls, response_hit) -> "Passage":
        fields = response_hit["fields"]
        return cls(
            family_name=fields["family_name"],
            family_description=fields["family_description"],
            family_import_id=fields["family_import_id"],
            family_slug=fields["family_slug"],
            family_category=fields["family_category"],
            family_publication_ts=datetime.fromisoformat(
                fields["family_publication_ts"]
            ),
            family_geography=fields["family_geography"],
            document_import_id=fields["document_import_id"],
            document_slug=fields["document_slug"],
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
    id: str
    hits: Sequence[Hit]


class SearchResponse(BaseModel):
    total_hits: int
    query_time_ms: int
    total_time_ms: int
    families: Sequence[Family]
    continuation_token: Optional[str]
