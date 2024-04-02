"""Data models for data access."""

import itertools
from typing import (
    Iterable,
    Sequence,
    Optional,
    List,
    Dict,
    Tuple,
    Any,
    Union,
    TypeVar,
    Literal,
    Annotated,
)
from pathlib import Path
import datetime
import hashlib
import logging
from functools import cached_property
import os

import pandas as pd
from pydantic import (
    AnyHttpUrl,
    BaseModel,
    Field,
    StringConstraints,
    NonNegativeInt,
    PrivateAttr,
    model_validator,
    ConfigDict,
)
from tqdm.auto import tqdm
import numpy as np
import random

from datasets import Dataset as HFDataset, DatasetInfo, load_dataset
import cpr_sdk.data_adaptors as adaptors
from cpr_sdk.parser_models import BlockType, BaseParserOutput
from cpr_sdk.pipeline_general_models import (
    CONTENT_TYPE_HTML,
    CONTENT_TYPE_PDF,
    Json,
)

LOGGER = logging.getLogger(__name__)

AnyDocument = TypeVar("AnyDocument", bound="BaseDocument")


def _load_and_validate_metadata_csv(
    metadata_csv_path: Path, target_model: type[AnyDocument]
) -> pd.DataFrame:
    """
    Load a metadata CSV

    Raise a ValueError if it does not exist or doesn't have the expected columns.
    """
    if not metadata_csv_path.exists():
        raise ValueError(f"metadata_csv_path {metadata_csv_path} does not exist")

    if not metadata_csv_path.is_file() or not metadata_csv_path.suffix == ".csv":
        raise ValueError(f"metadata_csv_path {metadata_csv_path} must be a csv file")

    metadata_df = pd.read_csv(metadata_csv_path)

    expected_cols = {
        "Geography",
        "Geography ISO",
        "CPR Document Slug",
        "Category",
        "CPR Collection ID",
        "CPR Family ID",
        "CPR Family Slug",
        "CPR Document Status",
    }

    cclw_expected_cols = {
        "Sectors",
        "Collection name",
        "Document Type",
        "Family name",
        "Document role",
        "Document variant",
    }

    gst_expected_cols = {
        "Author",
        "Author Type",
        "Date",
        "Documents",  # URL
        "Submission Type",  # Document Type
        "Family Name",
        "Document Role",
        "Document Variant",
    }

    if target_model == CPRDocument:
        metadata_df["Sectors"] = metadata_df["Sectors"].fillna("")

        cpr_expected_cols = expected_cols | cclw_expected_cols
        if missing_cols := cpr_expected_cols - set(metadata_df.columns):
            raise ValueError(f"Metadata CSV is missing columns {missing_cols}")

    if target_model == GSTDocument:
        gst_expected_cols = expected_cols | gst_expected_cols
        if missing_cols := gst_expected_cols - set(metadata_df.columns):
            raise ValueError(f"Metadata CSV is missing columns {missing_cols}")

    return metadata_df


class KnowledgeBaseIDs(BaseModel):
    """Store for knowledge base IDs."""

    wikipedia_title: Optional[str]
    wikidata_id: Optional[Annotated[str, StringConstraints(pattern=r"^Q\d+$")]]  # type: ignore
    model_config: ConfigDict = {
        "frozen": True,
    }


class Span(BaseModel):
    """
    Annotation with a type and ID made to a span of text in a document.

    The following validation is performed on creation of a `Span` instance:
    - checking that `start_idx` and `end_idx` are consistent with the length of `text`

    Properties:
    - document_id: document ID containing text block that span is in
    - text_block_text_hash: to check that the annotation is still valid when added to a text block
    - type: less fine-grained identifier for concept, e.g. "LOCATION". Converted to uppercase and spaces replaced with underscores.
    - id: fine-grained identifier for concept, e.g. 'Paris_France'. Converted to uppercase and spaces replaced with underscores.
    - text: text of span
    - start_idx: start index in text block text
    - end_idx: the index of the first character after the span in text block text
    - sentence: containing sentence (or otherwise useful surrounding text window) of span
    - annotator: name of annotator
    """

    document_id: str
    text_block_text_hash: str
    type: str
    id: str
    text: str
    start_idx: int
    end_idx: int
    sentence: str
    pred_probability: Annotated[float, Field(ge=0, le=1)]
    annotator: str
    kb_ids: Optional[KnowledgeBaseIDs] = None

    def __hash__(self):
        """Make hashable."""
        return hash((type(self),) + tuple(self.__dict__.values()))

    @model_validator(mode="after")
    def _is_valid(self):
        """Check that the span is valid, and convert label and id to a consistent format."""

        if self.start_idx + len(self.text) != self.end_idx:
            raise ValueError(
                "Values of 'start_idx', 'end_idx' and 'text' are not consistent. 'end_idx' should be 'start_idx' + len('text')."
            )

        self.type = self.type.upper().replace(" ", "_")
        self.id = self.id.upper().replace(" ", "_")

        return self


class TextBlock(BaseModel):
    """Text block data model. Generic across content types"""

    model_config: ConfigDict = {"ignored_types": (cached_property,)}

    text: Sequence[str]
    text_block_id: str
    language: Optional[str] = None
    type: BlockType
    type_confidence: Annotated[float, Field(ge=0, le=1)]
    page_number: Annotated[int, Field(ge=-1)]
    coords: Optional[List[Tuple[float, float]]] = None
    _spans: list[Span] = PrivateAttr(default_factory=list)

    def to_string(self) -> str:
        """Return text in a clean format"""
        return " ".join([line.strip() for line in self.text])

    def __hash__(self) -> int:
        """Get hash of the text-block. Based on the text and the text_block_id"""
        text_utf8 = self.to_string().encode("utf-8")

        return hash(f"{text_utf8}-{self.text_block_id.encode()}")

    @cached_property
    def text_hash(self) -> str:
        """
        Get hash of text block text. If the text block has no text (although this shouldn't be the case), return an empty string.

        :return str: md5sum + "__" + sha256, or empty string if the text block has no text
        """
        if self.text == "":
            return ""

        text_utf8 = self.to_string().encode("utf-8")

        return (
            hashlib.md5(text_utf8).hexdigest()
            + "__"
            + hashlib.sha256(text_utf8).hexdigest()
        )

    @property
    def spans(self) -> Sequence[Span]:
        """Return all spans in the text block."""
        return self._spans

    def _add_spans(
        self,
        spans: Sequence[Span],
        raise_on_error: bool = False,
        skip_check: bool = False,
    ) -> "TextBlock":
        """
        Add spans to the text block.

        If adding spans to a document, `Document.add_spans` should be used instead, as it checks that the document ID of the span matches the text block.

        :param spans: spans to add
        :param raise_on_error: if True, raise an error if any of the spans do not have `text_block_text_hash` equal to the text block's text hash. If False, print a warning message instead.
        :param skip_check: if True, skip the check that the text block's text hash matches the text hash of the spans. This can be used if calling from a method that already performs this check.
        :raises ValueError: if any of the spans do not have `text_block_text_hash` equal to the text block's text hash
        :raises ValueError: if the text block has no text
        :return: text block with spans added
        """

        block_text_hash = self.text_hash

        if block_text_hash == "":
            raise ValueError("Text block has no text")

        spans_unique = set(spans)

        if skip_check:
            valid_spans_text_hash = spans_unique
        else:
            valid_spans_text_hash = set(
                [
                    span
                    for span in spans_unique
                    if span.text_block_text_hash == block_text_hash
                ]
            )

            if len(valid_spans_text_hash) < len(spans_unique):
                error_msg = "Some spans are invalid as their text does not match the text block's."

                if raise_on_error:
                    raise ValueError(
                        error_msg
                        + " No spans have been added. Use ignore_errors=True to ignore this error and add valid spans."
                    )
                else:
                    LOGGER.warning(error_msg + " Valid spans have been added.")

        self._spans.extend(list(valid_spans_text_hash))

        return self

    @staticmethod
    def character_idx_to_token_idx(doc, char_idx: int) -> int:
        """
        Convert a character index to a token index in a spacy doc.

        The token index returned is the index of the token that contains the character index.

        :param doc: spacy doc object
        :param char_idx: character index
        :return: token index
        """

        if char_idx < 0:
            raise ValueError("Character index must be positive.")

        if char_idx > len(doc.text):
            raise ValueError(
                "Character index must be less than the length of the document."
            )

        for token in doc:
            if char_idx > token.idx:
                continue
            if char_idx == token.idx:
                return token.i
            if char_idx < token.idx:
                return token.i - 1

        # Return last token index if character index is at the end of the document
        return len(doc) - 1

    def display(self, style: Literal["ent", "span"] = "span", nlp=None) -> str:
        """
        Use spacy to display any annotations on the text block.

        :return str: HTML string of text block with annotations
        """
        try:
            from spacy import displacy
        except ImportError as e:
            raise ImportError(
                "spacy is required to use the display method. Please install it with `pip install spacy`."
            ) from e

        if style == "ent":
            ents = [
                {"start": span.start_idx, "end": span.end_idx, "label": span.type}
                for span in self._spans
            ]

            block_object = [{"text": self.to_string(), "ents": ents, "title": None}]

            return displacy.render(block_object, style="ent", manual=True)

        elif style == "span":
            if nlp is None:
                raise ValueError(
                    "Spacy pipeline object is required to use the display method with style='span'."
                )

            # TODO: we should store tokens in the text block object rather than creating them here
            spacy_doc = nlp(self.to_string())
            block_tokens = [tok.text for tok in spacy_doc]

            ents = [
                {
                    "start_token": self.character_idx_to_token_idx(
                        spacy_doc, span.start_idx
                    ),
                    "end_token": self.character_idx_to_token_idx(
                        spacy_doc, span.end_idx
                    )
                    + 1,
                    "label": span.type,
                }
                for span in self._spans
            ]

            block_object = [
                {
                    "text": self.to_string(),
                    "spans": ents,
                    "tokens": block_tokens,
                    "title": None,
                }
            ]

            return displacy.render(block_object, style="span", manual=True)


class PageMetadata(BaseModel):
    """
    Set of metadata for a single page of a paged document.

    :attribute page_number: The page number of the page in the document. 0-indexed.
    :attribute dimensions: (width, height) of the page in pixels
    """

    page_number: NonNegativeInt
    dimensions: Tuple[float, float]


class BaseMetadata(BaseModel):
    """Metadata that we expect to appear in every document. Should be kept minimal."""

    geography: Optional[str] = None
    publication_ts: Optional[datetime.datetime]


class BaseDocument(BaseModel):
    """Base model for a document."""

    document_id: str
    document_name: str
    document_source_url: Optional[AnyHttpUrl] = None
    document_content_type: Optional[str] = None
    document_md5_sum: Optional[str] = None
    languages: Optional[Sequence[str]] = None
    translated: bool
    has_valid_text: bool
    text_blocks: Optional[
        Sequence[TextBlock]
    ] = None  # None if there is no content type
    page_metadata: Optional[
        Sequence[PageMetadata]
    ] = None  # Properties such as page numbers and dimensions for paged documents
    document_metadata: BaseMetadata
    # The current fields are set in the document parser:
    # https://github.com/climatepolicyradar/navigator-document-parser/blob/5a2872389a85e9f81cdde148b388383d7490807e/cli/parse_pdfs.py#L435
    # These are azure_api_version, azure_model_id and parsing_date
    pipeline_metadata: Json = {}

    @classmethod
    def from_parser_output(
        cls: type[AnyDocument], parser_document: BaseParserOutput
    ) -> AnyDocument:
        """Load from document parser output"""

        if parser_document.document_content_type is None:
            has_valid_text = False
            text_blocks = None
            page_metadata = None

        elif parser_document.document_content_type == CONTENT_TYPE_HTML:
            has_valid_text = parser_document.html_data.has_valid_text  # type: ignore
            text_blocks = [
                TextBlock(
                    text=html_block.text,
                    text_block_id=html_block.text_block_id,
                    language=html_block.language,
                    type=BlockType.TEXT,
                    type_confidence=1,
                    page_number=-1,
                    coords=None,
                )
                for html_block in parser_document.html_data.text_blocks  # type: ignore
            ]
            page_metadata = None

        elif parser_document.document_content_type == CONTENT_TYPE_PDF:
            has_valid_text = True
            text_blocks = [
                TextBlock.model_validate(block.model_dump())
                for block in (parser_document.pdf_data.text_blocks)  # type: ignore
            ]
            page_metadata = [
                PageMetadata.model_validate(meta.model_dump())
                for meta in parser_document.pdf_data.page_metadata  # type: ignore
            ]

        else:
            raise ValueError(
                f"Unsupported content type: {parser_document.document_content_type}"
            )

        parser_document_data = parser_document.model_dump(
            exclude={"html_data", "pdf_data"}
        )
        metadata = {
            "document_metadata": parser_document.document_metadata,
            "pipeline_metadata": parser_document.pipeline_metadata,
        }
        text_and_page_data = {
            "text_blocks": text_blocks,  # type: ignore
            "page_metadata": page_metadata,  # type: ignore
            "has_valid_text": has_valid_text,
        }

        return cls.model_validate(parser_document_data | metadata | text_and_page_data)

    @classmethod
    def load_from_remote(
        cls: type[AnyDocument], bucket_name: str, document_id: str
    ) -> AnyDocument:
        """
        Load document from s3

        :param str bucket_name: bucket name
        :param str document_id: document id
        :raises ValueError: if document not found
        :return Document: document object
        """

        parser_output = adaptors.S3DataAdaptor().get_by_id(bucket_name, document_id)

        if parser_output is None:
            raise ValueError(f"Document with id {document_id} not found")

        return cls.from_parser_output(parser_output)

    @classmethod
    def load_from_local(
        cls: type[AnyDocument], path: str, document_id: str
    ) -> AnyDocument:
        """
        Load document from local directory

        :param str path: local path to document
        :param str document_id: document id
        :raises ValueError: if document not found
        :return Document: document object
        """

        parser_output = adaptors.LocalDataAdaptor().get_by_id(path, document_id)

        if parser_output is None:
            raise ValueError(f"Document with id {document_id} not found")

        return cls.from_parser_output(parser_output)

    @property
    def text(self) -> str:
        """Text blocks concatenated with joining spaces."""

        if self.text_blocks is None:
            return ""

        return " ".join([block.to_string().strip() for block in self.text_blocks])

    @cached_property
    def _text_block_idx_hash_map(self) -> dict[str, set[int]]:
        """Return a map of text block hash to text block indices."""

        if self.text_blocks is None:
            return {}

        hash_map: dict[str, set[int]] = dict()

        for idx, block in enumerate(self.text_blocks):
            if block.text_hash in hash_map:
                hash_map[block.text_hash].add(idx)
            else:
                hash_map[block.text_hash] = {idx}

        return hash_map

    def add_spans(
        self: AnyDocument, spans: Sequence[Span], raise_on_error: bool = False
    ) -> AnyDocument:
        """
        Add spans to text blocks in the document.

        :param Sequence[Span] spans: spans to add
        :param bool raise_on_error: whether to raise if a span in the input is invalid, defaults to False
        :raises ValueError: if any of the spans do not have `text_block_text_hash` equal to the text block's text hash
        :return Document: document with spans added to text blocks
        """

        if self.text_blocks is None:
            raise ValueError("Document has no text blocks")

        spans_unique = set(spans)

        if invalid_spans_document_id := {
            span for span in spans_unique if span.document_id != self.document_id
        }:
            error_msg = f"Span document id does not match document id for {len(invalid_spans_document_id)} spans provided."

            if raise_on_error:
                raise ValueError(error_msg)
            else:
                LOGGER.warning(error_msg + " Skipping these spans.")

            spans_unique = spans_unique - invalid_spans_document_id

        if invalid_spans_block_text := {
            span
            for span in spans_unique
            if span.text_block_text_hash not in self._text_block_idx_hash_map
        }:
            error_msg = f"Span text hash is not in document for {len(invalid_spans_block_text)}/{len(spans_unique)} spans provided."

            if raise_on_error:
                raise ValueError(error_msg)
            else:
                LOGGER.warning(error_msg + " Skipping these spans.")

            spans_unique = spans_unique - invalid_spans_block_text

        spans_unique = sorted(spans_unique, key=lambda span: span.text_block_text_hash)

        for block_text_hash, spans in itertools.groupby(spans_unique, key=lambda span: span.text_block_text_hash):  # type: ignore
            idxs = self._text_block_idx_hash_map[block_text_hash]
            for idx in idxs:
                try:
                    self.text_blocks[idx]._add_spans(
                        spans, raise_on_error=raise_on_error, skip_check=True
                    )
                except Exception as e:
                    if raise_on_error:
                        raise e
                    else:
                        LOGGER.warning(
                            f"Error adding span {spans} to text block {self.text_blocks[idx]}: {e}"
                        )

        return self

    def get_text_block_window(
        self, text_block: TextBlock, window_range: tuple[int, int]
    ) -> Sequence[TextBlock]:
        """
        Get a window of text blocks around a given text block.

        :param str text_block: text block
        :param tuple[int, int] window_range: start and end index of text blocks to get relative to the given text block (inclusive).
         The first value should be negative. Fewer text blocks may be returned if the window reaches beyond start or end of the document.
        :return list[TextBlock]: list of text blocks
        """

        if self.text_blocks is None:
            raise ValueError("Document has no text blocks")

        if text_block not in self.text_blocks:
            raise ValueError("Text block not in document")

        if window_range[0] > 0:
            raise ValueError("Window range start index should be negative")

        if window_range[1] < 0:
            raise ValueError("Window range end index should be positive")

        text_block_idx = self.text_blocks.index(text_block)

        start_idx = max(0, text_block_idx + window_range[0])
        end_idx = min(len(self.text_blocks), text_block_idx + window_range[1] + 1)

        return self.text_blocks[start_idx:end_idx]

    def get_text_window(
        self, text_block: TextBlock, window_range: tuple[int, int]
    ) -> str:
        """
        Get text of the text block, and a window of text blocks around it. Useful to add context around a given text block.

        :param str text_block: text block
        :param tuple[int, int] window_range: start and end index of text blocks to get relative to the given text block (inclusive).
         The first value should be negative. Fewer text blocks may be returned if the window reaches beyond start or end of the document.
        :return str: text
        """

        return " ".join(
            [
                tb.to_string()
                for tb in self.get_text_block_window(text_block, window_range)
            ]
        )

    def text_block_before(self, text_block: TextBlock) -> Optional[TextBlock]:
        """Get the text block before the given text block. Returns None if there is no text block before."""
        if blocks_before := self.get_text_block_window(text_block, (-1, 0)):
            return blocks_before[0]

        return None

    def text_block_after(self, text_block: TextBlock) -> Optional[TextBlock]:
        """Get the text block after the given text block. Returns None if there is no text block after."""

        if blocks_after := self.get_text_block_window(text_block, (0, 1)):
            return blocks_after[0]

        return None

    def to_markdown(
        self,
        show_debug_elements: bool = False,
        debug_only_types: Iterable[BlockType] = [
            BlockType.TABLE_CELL,
            BlockType.TABLE,
            BlockType.PAGE_NUMBER,
        ],
    ) -> str:
        """
        Display a document in markdown format.

        :param bool show_debug_elements: whether to show elements that we can't nicely
            display as markdown at the moment. Defaults to False
        :param Iterable[BlockType] debug_only_types: block types to only display if
            `show_debug_elements` is set to True. Defaults to {BlockType.TABLE_CELL,
            BlockType.TABLE, BlockType.PAGE_NUMBER}
        :return str: Markdown string representing the document
        """

        markdown_str = ""

        if self.text_blocks is None:
            return markdown_str

        for block in self.text_blocks:
            if (not show_debug_elements) and (block.type in debug_only_types):
                continue

            block_string = block.to_string()
            if block.type == BlockType.TEXT:
                markdown_str += block_string + "\n\n"

            elif block.type == BlockType.LIST:
                markdown_str += "\n".join(f"- {item}" for item in block) + "\n\n"

            elif block.type == BlockType.TABLE:
                markdown_str += f"**TABLE: {block_string}**" + "\n\n"

            elif block.type == BlockType.SECTION_HEADING:
                markdown_str += f"## {block_string}" + "\n\n"

            elif block.type in {BlockType.TITLE, BlockType.TITLE_LOWER_CASE}:
                markdown_str += f"# {block_string}" + "\n\n"

            elif block.type == BlockType.TABLE_CELL:
                if not markdown_str.endswith("**table**\n\n"):
                    markdown_str += "**table**" + "\n\n"

            else:
                markdown_str += f"**{block.type}:** {block_string}" + "\n\n"

        return markdown_str


class CPRDocumentMetadata(BaseModel):
    """Metadata about a document in the CPR tool."""

    # NOTE: this is duplicated in the GST document metadata model intentionally,
    # as the BaseMetadata model should be kept in sync with the parser output model.
    geography: str
    geography_iso: str
    slug: str
    category: str
    source: str
    type: str
    sectors: Sequence[str]
    collection_id: Optional[str] = None
    collection_name: Optional[str] = None
    family_id: str
    family_name: str
    family_slug: str
    role: Optional[str] = None
    variant: Optional[str] = None
    status: str
    publication_ts: Optional[datetime.datetime] = None


class CPRDocument(BaseDocument):
    """
    Data for a document in the CPR tool (app.climatepolicyradar.org). Note this is very similar to the ParserOutput model.

    Special cases for content types:
    - HTML: all text blocks have page_number == -1, block type == BlockType.TEXT, type_confidence == 1.0 and coords == None
    - PDF: all documents have has_valid_text == True
    - no content type: all documents have has_valid_text == False
    """

    document_description: str
    document_slug: str
    document_cdn_object: Optional[str] = None
    document_metadata: CPRDocumentMetadata


class GSTDocumentMetadata(BaseModel):
    """Metadata for a document in the Global Stocktake dataset."""

    source: str
    author: Sequence[str]
    geography_iso: str
    types: Optional[Sequence[str]] = None
    date: datetime.date
    link: Optional[str] = None
    author_is_party: bool
    collection_id: Optional[str] = None
    family_id: str
    family_name: str
    family_slug: str
    role: Optional[str] = None
    variant: Optional[str] = None
    status: str


class GSTDocument(BaseDocument):
    """Data model for a document in the Global Stocktake dataset."""

    document_metadata: GSTDocumentMetadata


class CPRDocumentWithURL(CPRDocument):
    """CPR Document with a document_url field"""

    document_url: Optional[AnyHttpUrl]


class Dataset:
    """
    Helper class for accessing the entire corpus.

    :param document_model: pydantic model to use for documents
    :param documents: list of documents to add. Recommended to use `Dataset().load_from_remote` or `Dataset().load_from_local` instead. Defaults to []
    """

    model_config: ConfigDict = {"ignored_types": (cached_property,)}

    def __init__(
        self,
        document_model: type[AnyDocument] = BaseDocument,
        documents: Sequence[AnyDocument] = [],
        **kwargs,
    ):
        self.document_model = document_model
        self.documents = documents

        self.hf_hub_repo_map = {
            CPRDocument: "ClimatePolicyRadar/climate-law-and-policy-documents",
            GSTDocument: "ClimatePolicyRadar/global-stocktake-documents",
        }
        self.hf_hub_repo = self.hf_hub_repo_map.get(self.document_model)  # type: ignore

        if self.document_model == CPRDocument:
            if not kwargs.get("cdn_domain"):
                LOGGER.warning(
                    "cdn_domain has not been set. Defaulting to `cdn.climatepolicyradar.org`."
                )

            self.cdn_domain = kwargs.get("cdn_domain", "cdn.climatepolicyradar.org")

    def _load(
        self,
        adaptor: adaptors.DataAdaptor,
        name_or_path: str,
        limit: Optional[int] = None,
    ):
        """Load data from any adaptor."""

        parser_outputs = adaptor.load_dataset(name_or_path, limit)
        self.documents = [
            self.document_model.from_parser_output(doc)
            for doc in tqdm(parser_outputs, desc="Loading documents")
        ]

        if self.document_model == CPRDocument:
            self.documents = [
                doc.with_document_url(cdn_domain=self.cdn_domain)  # type: ignore
                for doc in self.documents
            ]

        return self

    @cached_property
    def _document_id_idx_hash_map(self) -> dict[str, set[int]]:
        """Return a map of document IDs to indices."""

        hash_map: dict[str, set[int]] = dict()

        for idx, document in enumerate(self.documents):
            if document.document_id in hash_map:
                hash_map[document.document_id].add(idx)
            else:
                hash_map[document.document_id] = {idx}

        return hash_map

    @property
    def metadata_df(self) -> pd.DataFrame:
        """Return a dataframe of document metadata"""
        metadata = [
            doc.model_dump(exclude={"text_blocks", "document_metadata"})
            | doc.document_metadata.model_dump()
            | {"num_text_blocks": len(doc.text_blocks) if doc.text_blocks else 0}
            | {"num_pages": len(doc.page_metadata) if doc.page_metadata else 0}
            for doc in self.documents
        ]

        metadata_df = pd.DataFrame(metadata)

        if "publication_ts" in metadata_df.columns:
            metadata_df["publication_year"] = metadata_df["publication_ts"].dt.year

        return metadata_df

    def load_from_remote(
        self,
        dataset_key: str,
        limit: Optional[int] = None,
    ) -> "Dataset":
        """Load data from s3. `dataset_key` is the path to the folder in s3, and should include the s3:// prefix."""

        return self._load(adaptors.S3DataAdaptor(), dataset_key, limit)

    def load_from_local(
        self,
        folder_path: str,
        limit: Optional[int] = None,
    ) -> "Dataset":
        """Load data from local copy of an s3 directory"""

        return self._load(adaptors.LocalDataAdaptor(), folder_path, limit)

    def add_spans(
        self,
        spans: Sequence[Span],
        raise_on_error: bool = False,
        warn_on_error: bool = True,
    ) -> "Dataset":
        """
        Add spans to documents in the dataset overlap with.

        :param Sequence[Span] spans: sequence of span objects
        :param bool raise_on_error: whether to raise if there is an error with matching spans to any documents. Defaults to False
        :param bool warn_on_error: whether to warn if there is an error with matching spans to any documents. Defaults to True
        :return Dataset: dataset with spans added
        """

        spans_sorted = sorted(spans, key=lambda x: x.document_id)

        for document_id, document_spans in tqdm(
            itertools.groupby(spans_sorted, key=lambda x: x.document_id), unit="docs"
        ):
            # find document index in dataset with matching document_id
            idxs = self._document_id_idx_hash_map.get(document_id, set())

            if len(idxs) == 0:
                if warn_on_error:
                    LOGGER.warning(f"Could not find document with id {document_id}")
                continue

            for idx in idxs:
                self.documents[idx].add_spans(
                    list(document_spans), raise_on_error=raise_on_error
                )

        return self

    def add_metadata(
        self,
        target_model: type[AnyDocument],
        metadata_csv_path: Path,
        force_all_documents_have_metadata: bool = True,
    ) -> "Dataset":
        """
        Convert all documents in the dataset to the target model, by adding metadata from the metadata CSV.

        :param target_model: model to convert documents in dataset to
        :param metadata_csv_path: path to metadata CSV
        :param force_all_documents_have_metadata: whether to raise an error if any documents in the dataset do not have metadata in the CSV. **Warning: if set to false, the output dataset may have fewer documents than before running this function.** Defaults to True
        :return self:
        """

        if target_model not in {CPRDocument, GSTDocument}:
            raise ValueError("target_model must be one of {CPRDocument, GSTDocument}")

        # Raises ValueError if metadata CSV doesn't contain the required columns
        metadata_df = _load_and_validate_metadata_csv(metadata_csv_path, target_model)

        new_documents = []

        for document in self.documents:
            if document.document_id not in metadata_df["CPR Document ID"].tolist():
                if force_all_documents_have_metadata:
                    raise Exception(
                        f"No document exists in the scraper data with ID equal to the document's: {document.document_id}"
                    )
                else:
                    continue

            doc_dict = document.model_dump(
                exclude={"document_metadata", "_text_block_idx_hash_map"}
            )
            new_metadata_dict = metadata_df.loc[
                metadata_df["CPR Document ID"] == document.document_id
            ].to_dict(orient="records")[0]

            if target_model == CPRDocument:
                doc_metadata = CPRDocumentMetadata(
                    source="CPR",
                    geography=new_metadata_dict.pop("Geography"),
                    geography_iso=new_metadata_dict.pop("Geography ISO"),
                    slug=new_metadata_dict["CPR Document Slug"],
                    category=new_metadata_dict.pop("Category"),
                    type=new_metadata_dict.pop("Document Type"),
                    sectors=[
                        s.strip()
                        for s in new_metadata_dict.get("Sectors", "").split(";")
                    ],
                    status=new_metadata_dict.pop("CPR Document Status"),
                    collection_id=(
                        new_metadata_dict.pop("CPR Collection ID")
                        if isinstance(new_metadata_dict.get("CPR Collection ID"), str)
                        else None
                    ),
                    collection_name=(
                        new_metadata_dict.pop("Collection name")
                        if isinstance(new_metadata_dict.get("Collection name"), str)
                        else None
                    ),
                    family_id=new_metadata_dict.pop("CPR Family ID"),
                    family_name=new_metadata_dict.pop("Family name"),
                    family_slug=new_metadata_dict.pop("CPR Family Slug"),
                    role=new_metadata_dict.pop("Document role"),
                    variant=(
                        new_metadata_dict.pop("Document variant")
                        if isinstance(new_metadata_dict.get("Document variant"), str)
                        else None
                    ),
                    # NOTE: we incorrectly use the "publication_ts" value from the parser output rather than the correct
                    # document date (calculated from events in product). When we upgrade to Vespa we should use the correct
                    # date.
                    publication_ts=document.document_metadata.publication_ts,
                )

                metadata_at_cpr_document_root = {
                    "document_description": new_metadata_dict.pop("Family summary"),
                    "document_slug": new_metadata_dict["CPR Document Slug"],
                }

                new_documents.append(
                    CPRDocument(
                        **(doc_dict | metadata_at_cpr_document_root),
                        document_metadata=doc_metadata,
                    )
                )

            elif target_model == GSTDocument:
                doc_metadata = GSTDocumentMetadata(
                    source="GST-related documents",
                    geography_iso=new_metadata_dict.pop("Geography ISO"),
                    types=[
                        s.strip()
                        for s in new_metadata_dict.pop("Submission Type").split(",")
                    ],
                    date=new_metadata_dict.pop("Date"),
                    link=new_metadata_dict.pop("Documents"),
                    author_is_party=new_metadata_dict.pop("Author Type") == "Party",
                    collection_id=(
                        new_metadata_dict.pop("CPR Collection ID")
                        if isinstance(new_metadata_dict.get("CPR Collection ID"), str)
                        else None
                    ),
                    family_id=new_metadata_dict.pop("CPR Family ID"),
                    family_name=new_metadata_dict.pop("Family Name"),
                    family_slug=new_metadata_dict.pop("CPR Family Slug"),
                    role=new_metadata_dict.pop("Document Role"),
                    variant=(
                        new_metadata_dict.pop("Document Variant")
                        if isinstance(new_metadata_dict.get("Document Variant"), str)
                        else None
                    ),
                    status=new_metadata_dict.pop("CPR Document Status"),
                    author=[
                        s.strip() for s in new_metadata_dict.pop("Author").split(",")
                    ],
                )

                # TODO: changing the document title manually should only need to be done because we're using old parser outputs.
                # Eventually the clean title should come from the new parser outputs.
                doc_dict["document_name"] = new_metadata_dict["Document Title"]

                new_documents.append(
                    GSTDocument(**doc_dict, document_metadata=doc_metadata)
                )

        self.documents = new_documents

        return self

    @classmethod
    def save(cls, path: Path):
        """Serialise to disk"""
        raise NotImplementedError

    def __len__(self):
        """Number of documents in the dataset"""
        return len(self.documents)

    def __getitem__(self, index: int):
        """Get document in the dataset by index"""
        return self.documents[index]

    def __iter__(self):
        """Iterate over the documents in the dataset"""
        return iter(self.documents)

    def dict(self, exclude: Union[None, str, list[str]] = None) -> Dict[str, Any]:  # type: ignore
        """Returns the dataset object in a dict format"""
        if isinstance(exclude, str):
            attributes_to_exclude = [exclude]
        elif isinstance(exclude, list):
            attributes_to_exclude = exclude
        else:
            attributes_to_exclude = []

        return {
            k: v for k, v in self.__dict__.items() if k not in attributes_to_exclude
        }

    def filter(self, attribute: str, value: Any) -> "Dataset":
        """
        Filter documents by attribute. Value can be a single value or a function returning a boolean.

        :param attribute: attribute (field) to filter on
        :param value: value to filter on, or function returning a boolean which specifies whether to keep a value
        :return Dataset: filtered dataset
        """

        if callable(value):
            documents = [
                doc for doc in self.documents if value(getattr(doc, attribute))
            ]

        else:
            documents = [
                doc for doc in self.documents if getattr(doc, attribute) == value
            ]

        instance_attributes = self.dict(exclude="documents")

        return Dataset(**instance_attributes, documents=documents)

    def filter_by_corpus(self, corpus_name: str) -> "Dataset":
        """Returns documents that are source from the corpus provided as per their document-id"""
        return self.filter(
            "document_id", lambda x: x.lower().startswith(corpus_name.lower())
        )

    def filter_by_language(self, language: str, strict_match: bool = True) -> "Dataset":
        """
        Return documents filtered by the language provided

        :param language: the language to filter by
        :param strict_match: controls whether to only return documents that have 1 language, and that matches with the
            provided language, or (in case of False) return all documents that have one or more languages, one of which
            is the provided one.
        """
        if strict_match:
            return self.filter("languages", [language])
        else:
            return self.filter(
                "languages",
                lambda x: language in x if isinstance(x, Iterable) else False,
            )

    def sample(self, n: Union[float, int], random_state: int = 42) -> "Dataset":
        """Samples n number of proportion of documents from the dataset and returns a Dataset object with only those."""
        random.seed(random_state)

        if isinstance(n, float) and n < 1:
            documents = random.sample(self.documents, round(n * len(self.documents)))
        elif isinstance(n, int) and n > 0:
            documents = random.sample(self.documents, min(n, len(self.documents)))
        else:
            raise ValueError(
                f"n should be a float in (0.0, 1.0) or a positive integer. Provided value: {n}"
            )

        instance_attributes = self.dict(exclude="documents")

        return Dataset(**instance_attributes, documents=documents)

    def sample_text_blocks(
        self, n: int, with_document_context: bool = False
    ) -> Union[List[TextBlock], Tuple[List[TextBlock], dict]]:  #  type: ignore
        """
        Randomly sample a number of text blocks. Used for e.g. negative sampling for text classification.

        For reproducibility you may want to set `random.seed` before calling this function.

        :param n: number of text blocks to sample
        :param with_document_context: If True, include document context in the output. Defaults to False
        :return: list of text blocks or (text block, document context) tuples.
        """

        all_blocks = self.get_all_text_blocks(
            with_document_context=with_document_context
        )

        if n >= len(all_blocks):
            LOGGER.warning(
                "Requested number of text blocks is >= the number of text blocks in the dataset. Returning all text blocks."
            )
            return all_blocks

        else:
            return random.sample(all_blocks, n)  # type: ignore

    def get_all_text_blocks(
        self, with_document_context: bool = False
    ) -> Union[List[TextBlock], Tuple[List[TextBlock], dict]]:  #  type: ignore
        """
        Return all text blocks in the dataset.

        :param with_document_context: If True, include document context in the output. Defaults to False
        :return: list of text blocks or (text block, document context) tuples.
        """

        output_values = []

        for doc in self.documents:
            if doc.text_blocks is not None:
                if with_document_context:
                    doc_dict = doc.model_dump(exclude={"text_blocks"})
                    for block in doc.text_blocks:
                        output_values.append((block, doc_dict))
                else:
                    for block in doc.text_blocks:
                        output_values.append(block)

        return output_values

    def _doc_to_text_block_dicts(self, document: AnyDocument) -> List[Dict[str, Any]]:  # type: ignore
        """
        Create a list of dictionaries with document metadata and text block metadata for each text block in a document.

        :return List[dict[str, Any]]: list of dictionaries with document metadata and text block metadata
        """

        if document.text_blocks is None:
            return []

        doc_metadata_dict = (
            document.model_dump(
                exclude={"text_blocks", "page_metadata", "document_metadata"}
            )
            | document.document_metadata.model_dump()
        )

        return [
            doc_metadata_dict
            | block.model_dump(exclude={"text"})
            | {"text": block.to_string(), "block_index": idx}
            for idx, block in enumerate(document.text_blocks)
        ]

    def to_huggingface(
        self,
        description: Optional[str] = None,
        homepage: Optional[str] = None,
        citation: Optional[str] = None,
    ) -> HFDataset:
        """
        Convert to a huggingface dataset to get access to the huggingface datasets API.

        :param description: description of the dataset for the huggingface dataset metadata
        :param homepage: homepage URL for the huggingface dataset metadata
        :param citation: Bibtex citation for the huggingface dataset metadata

        :return: Huggingface dataset
        """

        text_block_dicts = []

        for doc in self.documents:
            text_block_dicts.extend(self._doc_to_text_block_dicts(doc))

        dict_keys = set().union(*(d.keys() for d in text_block_dicts))

        if description is None and homepage is None and citation is None:
            dataset_info = None
        else:
            dataset_info = DatasetInfo(
                description=description or "",
                homepage=homepage or "",
                citation=citation or "",
            )

        mapping = {
            key: [d.get(key, None) for d in text_block_dicts] for key in dict_keys
        }

        plain_urls = [
            source_url.path if source_url else None
            for source_url in mapping["document_source_url"]
        ]

        mapping["document_source_url"] = plain_urls

        huggingface_dataset = HFDataset.from_dict(
            mapping=mapping,
            info=dataset_info,
        )

        # Rename column to avoid confusion with the 'language' field, which is text block language
        rename_map = {
            "languages": "document_languages",
        }

        huggingface_dataset = huggingface_dataset.rename_columns(rename_map)

        return huggingface_dataset

    def _from_huggingface_parquet(
        self,
        huggingface_dataset: HFDataset,
        limit: Optional[int] = None,
    ) -> "Dataset":
        """
        Create a dataset from a huggingface dataset.

        :param huggingface_dataset: created using `Dataset.to_huggingface()`
        :param limit: optionally limit the number of documents to load
        :return self: with documents loaded from huggingface dataset
        """

        # TODO: validate that we really do have a DataFrame & not an iterator
        hf_dataframe: pd.DataFrame = huggingface_dataset.to_pandas()  # type: ignore

        # This undoes the renaming of columns done in to_huggingface()
        hf_dataframe = hf_dataframe.rename(columns={"document_languages": "languages"})

        # Create a dummy variable to group on combining document_id and translated.
        # This way we get an accurate count in the progress bar.
        hf_dataframe["_document_id_translated"] = hf_dataframe[
            "document_id"
        ] + hf_dataframe["translated"].astype(str)

        if limit is not None:
            doc_ids = hf_dataframe["_document_id_translated"].unique()[:limit]
            hf_dataframe = hf_dataframe[
                hf_dataframe["_document_id_translated"].isin(doc_ids)
            ]

        documents = []

        for _, doc_df in tqdm(
            hf_dataframe.groupby("_document_id_translated"),
            total=hf_dataframe["_document_id_translated"].nunique(),
            unit="docs",
        ):
            document_text_blocks = [
                TextBlock(
                    # TODO: we aren't able to access the original text split over lines as it's not stored in the huggingface dataset
                    text=[row["text"]],
                    text_block_id=row["text_block_id"],
                    language=row["language"],
                    type=row["type"],
                    type_confidence=row["type_confidence"],
                    page_number=row["page_number"],
                    coords=[tuple(c) for c in row["coords"].tolist()] if row["coords"] is not None else None,  # type: ignore
                )
                for _, row in doc_df.iterrows()
            ]

            # pandas to_dict() stores sequences as numpy arrays, so we need to convert them back to lists
            doc_fields = {
                k: list(v) if isinstance(v, np.ndarray) else v
                for k, v in doc_df.iloc[0].to_dict().items()
            }

            doc_metadata_dict = doc_fields | {
                "source": "GST" if self.document_model == GSTDocument else "CPR"
            }

            doc = self.document_model.model_validate(
                doc_fields
                | {
                    "document_metadata": doc_metadata_dict,
                    "text_blocks": document_text_blocks,
                }
            )

            documents.append(doc)

        self.documents = documents

        return self

    def from_huggingface(
        self,
        dataset_name: Optional[str] = None,
        dataset_version: Optional[str] = None,
        limit: Optional[int] = None,
        **kwargs,
    ) -> "Dataset":
        """
        Load documents from a huggingface hub dataset.

        For private repos a token should be provided either as a `token` kwarg or as
        environment variable HUGGINGFACE_TOKEN. Any additional keyword arguments are
        passed to the huggingface datasets load_dataset function.

        :param dataset_name: name of the dataset on huggingface hub
        :param dataset_version: version of the dataset on huggingface hub
        :param limit: optionally limit the number of documents to load
        :return self: with documents loaded from huggingface dataset
        """

        token = kwargs.pop("token", os.getenv("HUGGINGFACE_TOKEN"))

        if dataset_name is None:
            if self.hf_hub_repo is None:
                raise ValueError(
                    f"Dataset name not provided and no default dataset name found for document model {self.document_model}. Provide a dataset name directly to this function."
                )

            dataset_name = self.hf_hub_repo
            LOGGER.info(
                f"Dataset name not provided. Using default dataset name {dataset_name} for document model {self.document_model}."
            )

        huggingface_dataset = load_dataset(
            dataset_name, dataset_version, token=token, split="train", **kwargs
        )

        # TODO: validate the result coming from the below method
        return self._from_huggingface_parquet(huggingface_dataset, limit)  # type: ignore
