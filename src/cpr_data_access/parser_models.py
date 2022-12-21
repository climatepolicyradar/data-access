"""A copy of src/base.py from navigator-document-parser, with methods that rely on external libraries removed. These may be duplicated in models.py, but the intention is that these stay in sync with the data pipeline so we can easily update the pipeline should we decide to use these new models."""

import logging
import logging.config
from datetime import date
from enum import Enum
from typing import Optional, Sequence, Tuple, List

from pydantic import BaseModel, AnyHttpUrl, Field, root_validator

logger = logging.getLogger(__name__)

CONTENT_TYPE_HTML = "text/html"
CONTENT_TYPE_PDF = "application/pdf"


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
    AMBIGUOUS = "Ambiguous"  # TODO: remove this when OCRProcessor._infer_block_type is implemented


class TextBlock(BaseModel):
    """
    Base class for a text block.

    :attribute text: list of text lines contained in the text block
    :attribute text_block_id: unique identifier for the text block
    :attribute language: language of the text block. 2-letter ISO code, optional.
    :attribute type: predicted type of the text block
    :attribute type_confidence: confidence score of the text block being of the predicted type
    """

    text: List[str]
    text_block_id: str
    language: Optional[
        str
    ] = None  # TODO: validate this against a list of language ISO codes
    type: BlockType
    type_confidence: float = Field(ge=0, le=1)

    def to_string(self) -> str:
        """Returns the lines in a text block as a string with the lines separated by spaces."""

        return " ".join([line.strip() for line in self.text])


class HTMLTextBlock(TextBlock):
    """
    Text block parsed from an HTML document.

    Type is set to "Text" with a confidence of 1.0 by default, as we do not predict types for text blocks parsed from HTML.
    """

    type: BlockType = BlockType.TEXT
    type_confidence: float = 1.0


class PDFTextBlock(TextBlock):
    """
    Text block parsed from a PDF document.

    Stores the text and positional information for a single text block extracted from a document.

    :attribute coords: list of coordinates of the vertices defining the boundary of the text block.
        Each coordinate is a tuple in the format (x, y). (0, 0) is at the top left corner of
        the page, and the positive x- and y- directions are right and down.
    :attribute page_number: page number of the page containing the text block.
    """

    coords: List[Tuple[float, float]]
    page_number: int = Field(ge=0)

    def to_string(self) -> str:
        """Returns the lines in a text block as a string with the lines separated by spaces."""

        return " ".join([line.strip() for line in self.text])


class ParserInput(BaseModel):
    """Base class for input to a parser."""

    document_id: str
    document_metadata: dict
    document_name: str
    document_description: str
    document_source_url: Optional[AnyHttpUrl]
    document_cdn_object: Optional[str]
    document_content_type: Optional[str]
    document_md5_sum: Optional[str]
    document_slug: str


class HTMLData(BaseModel):
    """Set of metadata specific to HTML documents."""

    detected_title: Optional[str]
    detected_date: Optional[date]
    has_valid_text: bool
    text_blocks: Sequence[HTMLTextBlock]


class PDFPageMetadata(BaseModel):
    """
    Set of metadata for a single page of a PDF document.

    :attribute dimensions: (width, height) of the page in pixels
    """

    page_number: int = Field(ge=0)
    dimensions: Tuple[float, float]


class PDFData(BaseModel):
    """
    Set of metadata unique to PDF documents.

    :attribute pages: List of pages contained in the document
    :attribute filename: Name of the PDF file, without extension
    :attribute md5sum: md5sum of PDF content
    :attribute language: list of 2-letter ISO language codes, optional. If null, the OCR processor didn't support language detection
    """

    page_metadata: Sequence[PDFPageMetadata]
    md5sum: str
    text_blocks: Sequence[PDFTextBlock]


class ParserOutput(BaseModel):
    """Base class for an output to a parser."""

    document_id: str
    document_metadata: dict
    document_name: str
    document_description: str
    document_source_url: Optional[AnyHttpUrl]
    document_cdn_object: Optional[str]
    document_content_type: Optional[str]
    document_md5_sum: Optional[str]
    document_slug: str

    languages: Optional[Sequence[str]] = None
    translated: bool = False
    html_data: Optional[HTMLData] = None
    pdf_data: Optional[PDFData] = None

    @root_validator
    def check_html_pdf_metadata(cls, values):
        """Check that html_data is set if content_type is HTML, or pdf_data is set if content_type is PDF."""
        if (
            values["document_content_type"] == CONTENT_TYPE_HTML
            and values["html_data"] is None
        ):
            raise ValueError("html_metadata must be set for HTML documents")

        if (
            values["document_content_type"] == CONTENT_TYPE_PDF
            and values["pdf_data"] is None
        ):
            raise ValueError("pdf_data must be set for PDF documents")

        if values["document_content_type"] is None and (
            values["html_data"] is not None or values["pdf_data"] is not None
        ):
            raise ValueError(
                "html_metadata and pdf_metadata must be null for documents with no content type."
            )

        return values

    @property
    def text_blocks(self) -> Sequence[TextBlock]:  # type: ignore
        """
        Return the text blocks in the document. These could differ in format depending on the content type.

        :return: Sequence[TextBlock]
        """

        if self.document_content_type == CONTENT_TYPE_HTML:
            return self.html_data.text_blocks  # type: ignore
        elif self.document_content_type == CONTENT_TYPE_PDF:
            return self.pdf_data.text_blocks  # type: ignore

    def to_string(self) -> str:  # type: ignore
        """Return the text blocks in the parser output as a string"""

        return " ".join(
            [text_block.to_string().strip() for text_block in self.text_blocks]
        )
