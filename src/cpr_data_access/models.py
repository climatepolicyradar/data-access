"""Data models for data access."""

from typing import Sequence, Optional, List, Tuple, Any, Union
from pathlib import Path
from enum import Enum
import datetime
import hashlib
import logging

from pydantic import (
    BaseModel,
    AnyHttpUrl,
    NonNegativeInt,
    confloat,
    conint,
    root_validator,
    PrivateAttr,
)

import cpr_data_access.data_adaptors as adaptors
from cpr_data_access.parser_models import (
    ParserOutput,
    CONTENT_TYPE_HTML,
    CONTENT_TYPE_PDF,
)

LOGGER = logging.getLogger(__name__)


class Span(BaseModel):
    """
    Annotation with a type and ID made to a span of text in a document.

    The following validation is performed on creation of a `Span` instance:
    - checking that `start_idx` and `end_idx` are consistent with the length of `text`
    - checking that the span's `text` value appears from indices `start_idx` to `end_idx` in `sentence`

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
    pred_probability: confloat(ge=0, le=1)  # type: ignore
    annotator: str

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

        if (
            values["sentence"][values["start_idx"] : values["end_idx"]]
            != values["text"]
        ):
            raise ValueError(
                f"The span's 'text' value does not seem to appear in 'sentence' from the values set by 'start_idx' and 'end_idx'. Span text according to 'start_idx' and 'end_idx' is: {values['sentence'][values['start_idx']:values['end_idx']]}"
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


class TextBlock(BaseModel):
    """Text block data model. Generic across content types"""

    text: Sequence[str]
    text_block_id: str
    language: Optional[str]
    type: BlockType
    type_confidence: confloat(ge=0, le=1)  # type: ignore
    page_number: conint(ge=-1)  # type: ignore
    coords: Optional[List[Tuple[float, float]]]
    _spans: list[Span] = PrivateAttr(default_factory=list)

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

        LOGGER.warning(
            "This method should not be used if adding spans to a document as it does not check that the document ID of the span matches document. Use `Document.add_spans` instead."
        )

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


class DocumentMetadata(BaseModel):
    """Metadata about a document."""

    publication_ts: Optional[datetime.datetime]
    geography: str
    category: str
    source: str
    type: str
    sectors: Sequence[str]


class Document(BaseModel):
    """
    Document data model. Note this is very similar to the ParserOutput model.

    Special cases for content types:
    - HTML: all text blocks have page_number == -1, block type == BlockType.TEXT, type_confidence == 1.0 and coords == None
    - PDF: all documents have has_valid_text == True
    - no content type: all documents have has_valid_text == False
    """

    document_id: str  # import ID
    document_name: str
    document_description: str
    document_source_url: Optional[AnyHttpUrl]
    document_cdn_object: Optional[str]
    document_content_type: Optional[str]
    document_md5_sum: Optional[str]
    document_slug: str
    document_metadata: DocumentMetadata
    languages: Optional[Sequence[str]]
    translated: bool
    has_valid_text: bool
    text_blocks: Optional[Sequence[TextBlock]]  # None if there is no content type
    page_metadata: Optional[
        Sequence[PageMetadata]
    ]  # Properties such as page numbers and dimensions for paged documents

    @classmethod
    def from_parser_output(cls, parser_document: ParserOutput) -> "Document":  # type: ignore
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

        return Document(
            document_id=parser_document.document_id,
            document_name=parser_document.document_name,
            document_description=parser_document.document_description,
            document_source_url=parser_document.document_source_url,
            document_cdn_object=parser_document.document_cdn_object,
            document_content_type=parser_document.document_content_type,
            document_md5_sum=parser_document.document_md5_sum,
            document_slug=parser_document.document_slug,
            document_metadata=DocumentMetadata.parse_obj(
                parser_document.document_metadata
            ),
            languages=parser_document.languages,
            translated=parser_document.translated,
            has_valid_text=has_valid_text,
            text_blocks=text_blocks,
            page_metadata=page_metadata,
        )

    def with_document_url(self, cdn_domain: str) -> "DocumentWithURL":
        """
        Return a document with a URL set. This is the CDN URL if there is a CDN object, otherwise the source URL.

        :param cdn_domain: domain of CPR CDN
        """

        document_url = self.document_source_url if self.document_cdn_object is None else f"https://{cdn_domain}/{self.document_cdn_object}"  # type: ignore

        return DocumentWithURL(**self.dict(), document_url=document_url)  # type: ignore

    @classmethod
    def load_from_remote(cls, bucket_name: str, document_id: str) -> "Document":
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

        return Document.from_parser_output(parser_output)

    @classmethod
    def load_from_local(cls, path: str, document_id: str) -> "Document":
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

        return Document.from_parser_output(parser_output)

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
        self, spans: Sequence[Span], raise_on_error: bool = False
    ) -> "Document":
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


class DocumentWithURL(Document):
    """Document with a document_url field"""

    document_url: Optional[AnyHttpUrl]


class Dataset:
    """Helper class for accessing the entire corpus."""

    def __init__(
        self,
        cdn_domain: str = "cdn.climatepolicyradar.org",
        documents: Sequence[Document] = [],
    ):
        self.cdn_domain = cdn_domain
        self.documents = documents

    def load_from_remote(
        self, bucket_name: str, limit: Optional[int] = None
    ) -> "Dataset":
        """Load data from s3"""

        parser_outputs = adaptors.S3DataAdaptor().load_dataset(bucket_name, limit)
        self.documents = [
            Document.from_parser_output(doc).with_document_url(
                cdn_domain=self.cdn_domain
            )
            for doc in parser_outputs
        ]

        return self

    def load_from_local(
        self, folder_path: str, limit: Optional[int] = None
    ) -> "Dataset":
        """Load data from local copy of an s3 directory"""

        parser_outputs = adaptors.LocalDataAdaptor().load_dataset(folder_path, limit)
        self.documents = [
            Document.from_parser_output(doc).with_document_url(
                cdn_domain=self.cdn_domain
            )
            for doc in parser_outputs
        ]

        return self

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
            return Dataset(
                documents=[
                    doc for doc in self.documents if value(getattr(doc, attribute))
                ],
                cdn_domain=self.cdn_domain,
            )
        else:
            return Dataset(
                documents=[
                    doc for doc in self.documents if getattr(doc, attribute) == value
                ],
                cdn_domain=self.cdn_domain,
            )

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
