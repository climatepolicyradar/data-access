"""Data models for data access."""

from typing import Sequence, Optional, List, Tuple, Any, Union, TypeVar, Literal
from pathlib import Path
from enum import Enum
import datetime
import hashlib
import logging
import itertools

from pydantic import (
    BaseModel,
    AnyHttpUrl,
    NonNegativeInt,
    confloat,
    conint,
    root_validator,
)
from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import String, Column, JSON
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
import pandas as pd
from tqdm.auto import tqdm

import cpr_data_access.data_adaptors as adaptors
from cpr_data_access.parser_models import (
    ParserOutput,
    CONTENT_TYPE_HTML,
    CONTENT_TYPE_PDF,
)

LOGGER = logging.getLogger(__name__)

AnyDocument = TypeVar("AnyDocument", bound="BaseDocument")


class Span(SQLModel, table=True):
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

    db_id: Optional[int] = Field(
        default=None, primary_key=True
    )  # FIXME: better primary key?
    document_id: str
    text_block_text_hash: str
    type: str
    id: str
    text: str
    start_idx: int
    end_idx: int
    sentence: str
    pred_probability: confloat(ge=0, le=1)  # type: ignore
    annotator: str
    text_block_db_id: Optional[int] = Field(default="", foreign_key="textblock.id")
    text_block: "TextBlock" = Relationship(back_populates="_spans")

    def __hash__(self):
        """Make hashable."""
        return hash((type(self),) + tuple(self.__dict__.values()))

    @root_validator
    def _is_valid(cls, values):
        """Check that the span is valid, and convert label and id to a consistent format."""

        if values["start_idx"] + len(values["text"]) != values["end_idx"]:
            raise ValueError(
                "Values of 'start_idx', 'end_idx' and 'text' are not consistent. 'end_idx' should be 'start_idx' + len('text')."
            )

        values["type"] = values["type"].upper().replace(" ", "_")
        values["id"] = values["id"].upper().replace(" ", "_")

        return values


class BlockType(str, Enum):
    """
    List of possible block types from the PubLayNet model.

    https://layout-parser.readthedocs.io/en/latest/notes/modelzoo.html#model-label-map
    """

    TEXT = "Text"
    TITLE = "Title"
    LIST = "List"
    TABLE = "Table"
    FIGURE = "Figure"
    INFERRED = "Inferred from gaps"
    AMBIGUOUS = "Ambiguous"


class TextBlock(SQLModel, table=True):
    """Text block data model. Generic across content types"""

    id: Optional[int] = Field(
        default=None, primary_key=True
    )  # FIXME: better primary key?
    text: Sequence[str] = Field(default_factory=list, sa_column=Column(ARRAY(String)))
    text_block_id: str
    language: Optional[str]
    type: BlockType
    type_confidence: confloat(ge=0, le=1)  # type: ignore
    page_number: conint(ge=-1)  # type: ignore
    coords: Optional[List[Tuple[float, float]]] = Field(
        default_factory=list, sa_column=Column(JSONB)
    )
    _spans: list[Span] = Relationship(back_populates="text_block")
    document_id: str = Field(default="", foreign_key="document.document_id")
    document: "BaseDocument" = Relationship(back_populates="text_blocks")

    def to_string(self) -> str:
        """Return text in a clean format"""
        return " ".join([line.strip() for line in self.text])

    @property
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
        self, spans: Sequence[Span], raise_on_error: bool = False
    ) -> "TextBlock":
        """
        Add spans to the text block.

        If adding spans to a document, `Document.add_spans` should be used instead, as it checks that the document ID of the span matches the text block.

        :param spans: spans to add
        :param raise_on_error: if True, raise an error if any of the spans do not have `text_block_text_hash` equal to the text block's text hash. If False, print a warning message instead.
        :raises ValueError: if any of the spans do not have `text_block_text_hash` equal to the text block's text hash
        :raises ValueError: if the text block has no text
        :return: text block with spans added
        """

        block_text_hash = self.text_hash

        if block_text_hash == "":
            raise ValueError("Text block has no text")

        spans_unique = set(spans)
        valid_spans_text_hash = set(
            [
                span
                for span in spans_unique
                if span.text_block_text_hash == block_text_hash
            ]
        )

        if len(valid_spans_text_hash) < len(spans_unique):
            error_msg = (
                "Some spans are invalid as their text does not match the text block's."
            )

            if raise_on_error:
                raise ValueError(
                    error_msg
                    + " No spans have been added. Use ignore_errors=True to ignore this error and add valid spans."
                )
            else:
                LOGGER.warning(error_msg + " Valid spans have been added.")

        self._spans.extend(list(valid_spans_text_hash))

        return self


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

    geography: Optional[str]


class BaseDocument(SQLModel, table=True):
    """Base model for a document."""

    __tablename__: str = "document"

    document_id: str = Field(primary_key=True)
    document_name: str
    document_source_url: Optional[AnyHttpUrl]
    document_content_type: Optional[str]
    document_md5_sum: Optional[str]
    languages: Optional[Sequence[str]]
    translated: bool
    has_valid_text: bool
    text_blocks: Optional[Sequence[TextBlock]] = Relationship(back_populates="document")
    page_metadata: Optional[Sequence[PageMetadata]] = Field(
        default_factory=list, sa_column=Column(ARRAY(JSON))
    )

    @classmethod
    def from_parser_output(
        cls: type[AnyDocument], parser_document: ParserOutput
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
            text_blocks = [TextBlock.parse_obj(block) for block in (parser_document.pdf_data.text_blocks)]  # type: ignore
            page_metadata = [PageMetadata.parse_obj(meta) for meta in parser_document.pdf_data.page_metadata]  # type: ignore

        else:
            raise ValueError(
                f"Unsupported content type: {parser_document.document_content_type}"
            )

        parser_document_data = parser_document.dict()
        metadata = {"document_metadata": parser_document.document_metadata}
        text_and_page_data = {
            "text_blocks": text_blocks,
            "page_metadata": page_metadata,
            "has_valid_text": has_valid_text,
        }

        return cls.parse_obj(parser_document_data | metadata | text_and_page_data)

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

    @property
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
            error_msg = f"Span text hash does not match text block text hash for {len(invalid_spans_block_text)} spans provided."

            if raise_on_error:
                raise ValueError(error_msg)
            else:
                LOGGER.warning(error_msg + " Skipping these spans.")

            spans_unique = spans_unique - invalid_spans_block_text

        for span in spans_unique:
            for idx in self._text_block_idx_hash_map[span.text_block_text_hash]:
                try:
                    self.text_blocks[idx]._add_spans(
                        [span], raise_on_error=raise_on_error
                    )
                except Exception as e:
                    if raise_on_error:
                        raise e
                    else:
                        LOGGER.warning(
                            f"Error adding span {span} to text block {self.text_blocks[idx]}: {e}"
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


class Document(BaseDocument):
    """Generic document with as few as possible assumptions about metadata."""

    document_metadata: BaseMetadata = Field(
        default_factory=dict, sa_column=Column(JSON)
    )


class CPRDocumentMetadata(SQLModel):
    """Metadata about a document in the CPR tool."""

    publication_ts: Optional[datetime.datetime]
    geography: str
    category: str
    source: str
    type: str
    sectors: Sequence[str] = Field(
        default_factory=list, sa_column=Column(ARRAY(String))
    )


class CPRDocument(BaseDocument):
    """
    Data for a document in the CPR tool (app.climatepolicyradar.org). Note this is very similar to the ParserOutput model.

    Special cases for content types:
    - HTML: all text blocks have page_number == -1, block type == BlockType.TEXT, type_confidence == 1.0 and coords == None
    - PDF: all documents have has_valid_text == True
    - no content type: all documents have has_valid_text == False
    """

    document_description: str
    document_cdn_object: Optional[str]
    document_metadata: CPRDocumentMetadata = Field(
        default_factory=dict, sa_column=Column(JSON)
    )
    document_slug: str

    def with_document_url(self, cdn_domain: str) -> "CPRDocumentWithURL":
        """
        Return a document with a URL set. This is the CDN URL if there is a CDN object, otherwise the source URL.

        :param cdn_domain: domain of CPR CDN
        """

        document_url = self.document_source_url if self.document_cdn_object is None else f"https://{cdn_domain}/{self.document_cdn_object}"  # type: ignore

        return CPRDocumentWithURL(**self.dict(), document_url=document_url)  # type: ignore


class GSTDocumentMetadata(BaseModel):
    """Metadata for a document in the Global Stocktake dataset."""

    source: str
    author: str
    validation_status: Literal["validated", "not validated", "error"]
    theme: Optional[str]
    type: Optional[str]
    version: Optional[str]
    author_type: Optional[str]
    date: datetime.date
    link: Optional[str]
    data_error_type: Optional[
        Literal[
            "source_incorrect",
            "outdated",
            "missing",
            "duplicate",
            "synthesis_error",
            "metadata_error",
            "incorrect_document",
        ]
    ]
    party: Optional[str]
    translation: Optional[str]
    topics: Optional[Sequence[str]]


class GSTDocument(BaseDocument):
    """Data model for a document in the Global Stocktake dataset."""

    document_metadata: GSTDocumentMetadata = Field(
        default_factory=dict, sa_column=Column(JSONB)
    )


class CPRDocumentWithURL(CPRDocument):
    """CPR Document with a document_url field"""

    document_url: Optional[AnyHttpUrl]


class Dataset:
    """
    Helper class for accessing the entire corpus.

    :param document_model: pydantic model to use for documents
    :param documents: list of documents to add. Recommended to use `Dataset.load_from_remote` or `Dataset.load_from_local` instead. Defaults to []
    """

    def __init__(
        self,
        document_model: type[AnyDocument],
        documents: Sequence[AnyDocument] = [],
        **kwargs,
    ):
        self.document_model = document_model
        self.documents = documents

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
            self.document_model.from_parser_output(doc) for doc in parser_outputs
        ]

        if self.document_model == CPRDocument:
            self.documents = [
                doc.with_document_url(cdn_domain=self.cdn_domain)  # type: ignore
                for doc in self.documents
            ]

        return self

    @property
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
            doc.dict(exclude={"text_blocks", "document_metadata"})
            | doc.document_metadata.dict() # FIXME: cannot access member "document_metadata" for type "BaseDocument*"
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

    @classmethod
    def save(cls, path: Path):
        """Serialise to disk"""
        raise NotImplementedError

    def __len__(self):
        """Number of documents in the dataset"""
        return len(self.documents)

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

        instance_attributes = {
            k: v for k, v in self.__dict__.items() if k != "documents"
        }

        return Dataset(**instance_attributes, documents=documents)

    def filter_by_language(self, language: str) -> "Dataset":
        """Return documents whose only language is the given language."""
        return self.filter("languages", [language])

    def sample_text(
        self, n: int, document_ids: Optional[Sequence[str]], replace: bool = False
    ):
        """Randomly sample a number of text blocks. Used for e.g. negative sampling for text classification."""
        raise NotImplementedError

    def get_all_text_blocks(
        self, with_document_context: bool = False
    ) -> Union[List[TextBlock], Tuple[List[TextBlock], dict]]:
        """
        Return all text blocks in the dataset.

        :param with_document_context: If True, include document context in the output. Defaults to False
        :return: list of text blocks or (text block, document context) tuples.
        """

        output_values = []

        for doc in self.documents:
            if doc.text_blocks is not None:
                if with_document_context:
                    doc_dict = doc.dict(exclude={"text_blocks"})
                    for block in doc.text_blocks:
                        output_values.append((block, doc_dict))
                else:
                    for block in doc.text_blocks:
                        output_values.append(block)

        return output_values

    def add_spans(
        self, spans: Sequence[Span], raise_on_error: bool = False
    ) -> "Dataset":
        """
        Add spans to documents in the dataset overlap with.

        :param Sequence[Span] spans: sequence of span objects
        :param bool raise_on_error: whether to raise if there is an error with matching spans to any documents. Defaults to False
        :return Dataset: dataset with spans added
        """

        spans_sorted = sorted(spans, key=lambda x: x.document_id)

        for document_id, document_spans in tqdm(
            itertools.groupby(spans_sorted, key=lambda x: x.document_id), unit="docs"
        ):
            # find document index in dataset with matching document_id
            idxs = self._document_id_idx_hash_map.get(document_id, set())

            if len(idxs) == 0:
                LOGGER.warning(f"Could not find document with id {document_id}")
                continue

            for idx in idxs:
                self.documents[idx].add_spans(
                    list(document_spans), raise_on_error=raise_on_error
                )

        return self
