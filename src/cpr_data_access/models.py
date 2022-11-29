from typing import Sequence, Optional, List, Tuple, Any
from pathlib import Path
from enum import Enum
import datetime

from pydantic import BaseModel, AnyHttpUrl, NonNegativeInt, confloat

import cpr_data_access.data_adaptors as adaptors
from src.cpr_data_access.parser_models import (
    ParserOutput,
    CONTENT_TYPE_HTML,
    CONTENT_TYPE_PDF,
)


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
    page_number: NonNegativeInt
    coords: Optional[List[Tuple[float, float]]]

    def to_string(self) -> str:
        """Return text in a clean format"""
        raise NotImplementedError


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
    document_cdn_object: Optional[
        str
    ]  # TODO: do we want to manufacture a URL, and if so should it point to the prod or the dev CDN instance?
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
            text_blocks = [TextBlock(block) for block in (parser_document.pdf_data.text_blocks)]  # type: ignore
            page_metadata = [PageMetadata(meta) for meta in parser_document.pdf_data.page_metadata]  # type: ignore

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


class Dataset:
    """Helper class for accessing the entire corpus."""

    def __init__(self, documents: Sequence[Document] = []):
        self.documents = documents

    @classmethod
    def load_from_remote(
        cls, bucket_name: str, limit: Optional[int] = None
    ) -> "Dataset":
        """Load from s3 or local copy of an s3 directory"""

        parser_outputs = adaptors.S3DataAdaptor().load_dataset(bucket_name, limit)
        documents = [Document.from_parser_output(doc) for doc in parser_outputs]

        return Dataset(documents)

    @classmethod
    def load_from_local(
        cls, folder_path: str, limit: Optional[int] = None
    ) -> "Dataset":
        """Load from local copy of an s3 directory"""

        parser_outputs = adaptors.LocalDataAdaptor().load_dataset(folder_path, limit)
        documents = [Document.from_parser_output(doc) for doc in parser_outputs]

        return Dataset(documents)

    @classmethod
    def save(cls, path: Path):
        """Serialise to disk"""
        raise NotImplementedError

    def __len__(self):
        """Number of documents in the dataset"""
        return len(self.documents)

    def filter(self, attribute: str, value: Any) -> "Dataset":
        """Filter documents by attribute"""
        return Dataset(
            [doc for doc in self.documents if getattr(doc, attribute) == value]
        )

    def filter_by_language(self, language: str) -> "Dataset":
        """Return documents whose only language is the given language."""
        return self.filter("languages", [language])

    def sample_text(
        self, n: int, document_ids: Optional[Sequence[str]], replace: bool = False
    ):
        """Randomly sample a number of text blocks. Used for e.g. negative sampling for text classification."""
        raise NotImplementedError
