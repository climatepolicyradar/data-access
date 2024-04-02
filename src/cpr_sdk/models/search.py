from datetime import datetime
import re
from typing import List, Optional, Sequence

from pydantic import (
    AliasChoices,
    BaseModel,
    computed_field,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)


from cpr_sdk.exceptions import QueryError

# Value Lookup Tables
sort_orders = {
    "asc": "+",
    "desc": "-",
    "ascending": "+",
    "descending": "-",
}

sort_fields = {
    "date": "family_publication_ts",
    "title": "family_name",
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


class Filters(BaseModel):
    """Filterable fields in a search request"""

    family_geography: Sequence[str] = []
    family_category: Sequence[str] = []
    document_languages: Sequence[str] = []
    family_source: Sequence[str] = []

    model_config: ConfigDict = {
        "extra": "forbid",
    }

    @field_validator(
        "family_geography", "family_category", "document_languages", "family_source"
    )
    def sanitise_filter_inputs(cls, field):
        """Remove problematic characters from filter values"""
        clean_values = []
        for keyword in field:
            keyword = keyword.replace('"', "")
            keyword = keyword.replace("\\", " ")
            keyword = " ".join(keyword.split())
            clean_values.append(keyword)
        return clean_values


class SearchParameters(BaseModel):
    """Parameters for a search request"""

    query_string: Optional[str] = ""
    exact_match: bool = False
    all_results: bool = False
    limit: int = Field(ge=0, default=100)
    max_hits_per_family: int = Field(
        validation_alias=AliasChoices("max_passages_per_doc", "max_hits_per_family"),
        default=10,
        ge=0,
    )

    family_ids: Optional[Sequence[str]] = None
    document_ids: Optional[Sequence[str]] = None

    filters: Optional[Filters] = None
    year_range: Optional[tuple[Optional[int], Optional[int]]] = None

    sort_by: Optional[str] = Field(
        validation_alias=AliasChoices("sort_field", "sort_by"), default=None
    )
    sort_order: str = "descending"

    continuation_tokens: Optional[Sequence[str]] = None

    @model_validator(mode="after")
    def validate(self):
        """Validate against mutually exclusive fields"""
        if self.exact_match and self.all_results:
            raise QueryError("`exact_match` and `all_results` are mutually exclusive")
        return self

    @field_validator("continuation_tokens")
    def continuation_tokens_must_be_upper_strings(cls, continuation_tokens):
        """Validate continuation_tokens match the expected format"""
        if not continuation_tokens:
            return continuation_tokens

        for token in continuation_tokens:
            if token == "":
                continue
            if not token.isalpha():
                raise QueryError(f"Expected continuation tokens to be letters: {token}")
            if not token.isupper():
                raise QueryError(
                    f"Expected continuation tokens to be uppercase: {token}"
                )
        return continuation_tokens

    @model_validator(mode="after")
    def query_string_must_not_be_empty(self):
        """Validate that the query string is not empty."""
        if not self.query_string:
            self.all_results = True
        return self

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
        if sort_order not in sort_orders:
            raise QueryError(
                f"Invalid sort order: {sort_order}. sort_order must be one of: "
                f"{sort_orders}"
            )
        return sort_order

    @computed_field
    def vespa_sort_by(self) -> Optional[str]:
        """Translates sort by into the format acceptable by vespa"""
        if self.sort_by:
            return sort_fields.get(self.sort_by)
        else:
            return None

    @computed_field
    def vespa_sort_order(self) -> Optional[str]:
        """Translates sort order into the format acceptable by vespa"""
        return sort_orders.get(self.sort_order)


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
    continuation_token: Optional[str] = None
    prev_continuation_token: Optional[str] = None


class SearchResponse(BaseModel):
    """Relevant results, and search response metadata"""

    total_hits: int
    total_family_hits: int = 0
    query_time_ms: Optional[int] = None
    total_time_ms: Optional[int] = None
    families: Sequence[Family]
    continuation_token: Optional[str] = None
    this_continuation_token: Optional[str] = None
    prev_continuation_token: Optional[str] = None
