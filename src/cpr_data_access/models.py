"""Data models for data access."""

import itertools
from typing import (
    Sequence,
    Optional,
    List,
    Tuple,
    Any,
    Union,
    TypeVar,
    Literal,
)
from pathlib import Path
import datetime
import hashlib
import logging
from functools import cached_property

from pydantic import (
    BaseModel,
    AnyHttpUrl,
    NonNegativeInt,
    confloat,
    conint,
    root_validator,
    PrivateAttr,
)
import pandas as pd
from tqdm.auto import tqdm

from datasets import Dataset as HFDataset, DatasetInfo
import cpr_data_access.data_adaptors as adaptors
from cpr_data_access.parser_models import (
    ParserOutput,
    CONTENT_TYPE_HTML,
    CONTENT_TYPE_PDF,
    BlockType,
)

LOGGER = logging.getLogger(__name__)

AnyDocument = TypeVar("AnyDocument", bound="BaseDocument")


def _load_and_validate_metadata_csv(
    metadata_csv_path: Path, target_model: type[AnyDocument]
) -> pd.DataFrame:
    """Load a metadata CSV, raising a ValueError if it does not exist or doesn't have the expected columns."""
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
        cpr_expected_cols = expected_cols | cclw_expected_cols
        if missing_cols := cpr_expected_cols - set(metadata_df.columns):
            raise ValueError(f"Metadata CSV is missing columns {missing_cols}")

    if target_model == GSTDocument:
        gst_expected_cols = expected_cols | gst_expected_cols
        if missing_cols := gst_expected_cols - set(metadata_df.columns):
            raise ValueError(f"Metadata CSV is missing columns {missing_cols}")

    return metadata_df


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

        values["type"] = values["type"].upper().replace(" ", "_")
        values["id"] = values["id"].upper().replace(" ", "_")

        return values


class TextBlock(BaseModel):
    """Text block data model. Generic across content types"""

    class Config:  # noqa: D106
        keep_untouched = (cached_property,)

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

    geography: Optional[str]
    publication_ts: Optional[datetime.datetime]


class BaseDocument(BaseModel):
    """Base model for a document."""

    document_id: str
    document_name: str
    document_source_url: Optional[AnyHttpUrl]
    document_content_type: Optional[str]
    document_md5_sum: Optional[str]
    languages: Optional[Sequence[str]]
    translated: bool
    has_valid_text: bool
    text_blocks: Optional[Sequence[TextBlock]]  # None if there is no content type
    page_metadata: Optional[
        Sequence[PageMetadata]
    ]  # Properties such as page numbers and dimensions for paged documents
    document_metadata: BaseMetadata

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
            text_blocks = [
                TextBlock.parse_obj(block)
                for block in (parser_document.pdf_data.text_blocks)  # type: ignore
            ]
            page_metadata = [
                PageMetadata.parse_obj(meta)
                for meta in parser_document.pdf_data.page_metadata  # type: ignore
            ]

        else:
            raise ValueError(
                f"Unsupported content type: {parser_document.document_content_type}"
            )

        parser_document_data = parser_document.dict()
        metadata = {"document_metadata": parser_document.document_metadata}
        text_and_page_data = {
            "text_blocks": text_blocks,  # type: ignore
            "page_metadata": page_metadata,  # type: ignore
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
    collection_id: Optional[str]
    collection_name: Optional[str]
    family_id: str
    family_name: str
    family_slug: str
    role: Optional[str]
    variant: Optional[str]
    status: str
    publication_ts: Optional[datetime.datetime]


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
    document_cdn_object: Optional[str]
    document_metadata: CPRDocumentMetadata


class GSTDocumentMetadata(BaseModel):
    """Metadata for a document in the Global Stocktake dataset."""

    source: str
    author: Sequence[str]
    geography_iso: str
    types: Optional[Sequence[str]]
    date: datetime.date
    link: Optional[str]
    author_is_party: bool
    collection_id: Optional[str]
    family_id: str
    family_name: str
    family_slug: str
    role: Optional[str]
    variant: Optional[str]
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
    :param documents: list of documents to add. Recommended to use `Dataset.load_from_remote` or `Dataset.load_from_local` instead. Defaults to []
    """

    class Config:  # noqa: D106
        keep_untouched = (cached_property,)

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
            doc.dict(exclude={"text_blocks", "document_metadata"})
            | doc.document_metadata.dict()
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
        self, target_model: type[AnyDocument], metadata_csv_path: Path
    ) -> "Dataset":
        """
        Convert all documents in the dataset to the target model, by adding metadata from the metadata CSV.

        :param target_model: model to convert documents in dataset to
        :param metadata_csv_path: path to metadata CSV
        :return self:
        """

        if target_model not in {CPRDocument, GSTDocument}:
            raise ValueError("target_model must be one of {CPRDocument, GSTDocument}")

        # Raises ValueError if metadata CSV doesn't contain the required columns
        metadata_df = _load_and_validate_metadata_csv(metadata_csv_path, target_model)

        new_documents = []

        for document in self.documents:
            if document.document_id not in metadata_df["CPR Document ID"].tolist():
                raise Exception(
                    f"No document exists in the scraper data with ID equal to the document's: {document.document_id}"
                )

            doc_dict = document.dict(
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
                        s.strip() for s in new_metadata_dict.pop("Sectors").split(";")
                    ],
                    status=new_metadata_dict.pop("CPR Document Status"),
                    collection_id=new_metadata_dict.pop("CPR Collection ID"),
                    collection_name=new_metadata_dict.pop("Collection name"),
                    family_id=new_metadata_dict.pop("CPR Family ID"),
                    family_name=new_metadata_dict.pop("Family name"),
                    family_slug=new_metadata_dict.pop("CPR Family Slug"),
                    role=new_metadata_dict.pop("Document role"),
                    variant=new_metadata_dict.pop("Document variant"),
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
                    collection_id=new_metadata_dict.pop("CPR Collection ID"),
                    family_id=new_metadata_dict.pop("CPR Family ID"),
                    family_name=new_metadata_dict.pop("Family Name"),
                    family_slug=new_metadata_dict.pop("CPR Family Slug"),
                    role=new_metadata_dict.pop("Document Role"),
                    variant=new_metadata_dict.pop("Document Variant"),
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

    def _doc_to_text_block_dicts(self, document: AnyDocument) -> list[dict[str, Any]]:
        """
        Create a list of dictionaries with document metadata and text block metadata for each text block in a document.

        :return List[dict[str, Any]]: list of dictionaries with document metadata and text block metadata
        """

        if document.text_blocks is None:
            return []

        doc_metadata_dict = (
            document.dict(exclude={"text_blocks", "page_metadata", "document_metadata"})
            | document.document_metadata.dict()
        )

        return [
            doc_metadata_dict
            | block.dict(exclude={"text"})
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

        huggingface_dataset = HFDataset.from_dict(
            mapping={
                key: [d.get(key, None) for d in text_block_dicts] for key in dict_keys
            },
            info=dataset_info,
        )

        return huggingface_dataset
