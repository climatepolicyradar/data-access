from enum import Enum
from typing import Mapping, Optional, Sequence

from pydantic import BaseModel

Coord = tuple[float, float]


class SortOrder(str, Enum):
    """Sort ordering for use building OpenSearch query body."""

    ASCENDING = "asc"
    DESCENDING = "desc"


class SortField(str, Enum):
    """Sort field for use building OpenSearch query body."""

    DATE = "date"
    TITLE = "title"


class FilterField(str, Enum):
    """Filter field for use building OpenSearch query body."""

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
    """The request body expected by the search API endpoint."""

    query_string: str
    exact_match: bool = False
    max_passages_per_doc: int = 10

    keyword_filters: Optional[Mapping[FilterField, Sequence[str]]] = None
    year_range: Optional[tuple[Optional[int], Optional[int]]] = None

    sort_field: Optional[SortField] = None
    sort_order: SortOrder = SortOrder.DESCENDING

    limit: int = 10
    offset: int = 0


class ResponseDocumentPassage(BaseModel):
    """A Document passage match returned by the search API endpoint."""

    text: str
    text_block_id: str
    text_block_page: Optional[int]
    text_block_coords: Optional[Sequence[Coord]]


class ResponseMatchBase(BaseModel):
    """Describes matches returned by a query"""

    name: str
    geography: str
    description: str
    sectors: Sequence[str]
    source: str
    id: str  # Changed semantics to be import_id, not database id
    date: str
    type: str
    source_url: Optional[str]
    cdn_object: Optional[str]
    category: str
    content_type: Optional[str]
    slug: str


class SearchResponseFamily(BaseModel):
    """
    The object that is returned in the response.

    Used to extend with postfix
    """

    slug: str
    name: str
    description: str
    category: str
    date: str
    last_updated_date: str
    source: str
    geography: str
    metadata: dict
    title_match: Optional[bool]
    description_match: Optional[bool]
    documents: list[ResponseMatchBase]


class SearchResponse(BaseModel):
    """The response body produced by the search API endpoint."""

    hits: int
    query_time_ms: int
    total_time_ms: int

    families: Sequence[SearchResponseFamily]
