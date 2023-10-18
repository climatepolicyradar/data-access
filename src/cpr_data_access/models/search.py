from datetime import datetime
from enum import Enum
from typing import List, Mapping, Optional, Sequence

from pydantic import BaseModel


class SortOrder(str, Enum):
    """Valid sort orders for search results"""

    ASCENDING = "asc"
    DESCENDING = "desc"


class SortField(str, Enum):
    """Valid fields to sort search results by"""

    DATE = "date"
    TITLE = "title"


class FilterField(str, Enum):
    """Valid fields for filtering search results"""

    SOURCE = "sources"
    COUNTRY = "countries"
    REGION = "regions"
    INSTRUMENT = "instruments"
    SECTOR = "sectors"
    TYPE = "types"
    CATEGORY = "categories"
    TOPIC = "topics"
    KEYWORD = "keywords"
    HAZARD = "hazards"
    LANGUAGE = "languages"
    FRAMEWORK = "frameworks"


class SearchRequestBody(BaseModel):
    """Parameters for a search request"""

    query_string: str
    exact_match: bool = False
    max_hits_per_family: int = 10

    keyword_filters: Optional[Mapping[FilterField, Sequence[str]]] = None
    year_range: Optional[tuple[Optional[int], Optional[int]]] = None

    sort_field: Optional[SortField] = None
    sort_order: SortOrder = SortOrder.DESCENDING

    limit: int = 10
    offset: int = 0


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
            family_name=fields["name"],
            family_description=fields["description"],
            family_import_id=fields["family_import_id"],
            family_slug=fields["family_slug"],
            family_category=fields["category"],
            family_publication_ts=datetime.fromisoformat(fields["publication_ts"]),
            family_geography=fields["geography"],
            document_import_id=fields["documentid"],
            document_slug=fields["document_slug"],
            document_languages=fields["family_metadata"].get("language", []),
            document_content_type=fields.get("content_type", None),
            document_cdn_object=fields.get("cdn_object", None),
            document_source_url=fields.get("source_url", None),
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
            family_name=fields["name"],
            family_description=fields["description"],
            family_import_id=fields["family_import_id"],
            family_slug=fields["family_slug"],
            family_category=fields["category"],
            family_publication_ts=datetime.fromisoformat(fields["publication_ts"]),
            family_geography=fields["geography"],
            document_import_id=fields["documentid"],
            document_slug=fields["document_slug"],
            document_languages=fields["family_metadata"].get("language", []),
            document_content_type=fields.get("content_type", None),
            document_cdn_object=fields.get("cdn_object", None),
            document_source_url=fields.get("source_url", None),
            text_block=fields["text_block"],
            text_block_id=fields["text_block_id"],
            text_block_type=fields["text_block_type"],
            text_block_page=fields.get("text_block_page", None),
            text_block_coords=fields.get("text_block_coords", None),
        )


class Family(BaseModel):
    id: str
    hits: Sequence[Hit]


class SearchResponse(BaseModel):
    total_hits: int
    query_time_ms: int
    total_time_ms: int

    families: Sequence[Family]
