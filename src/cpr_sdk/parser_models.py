import logging
import logging.config
from collections import Counter
from datetime import date
from enum import Enum
from typing import List, Optional, Sequence, Tuple, TypeVar, Union

from cpr_sdk.pipeline_general_models import (
    CONTENT_TYPE_HTML,
    CONTENT_TYPE_PDF,
    BackendDocument,
    Json,
)
from cpr_sdk.utils import remove_key_if_all_nested_vals_none, unflatten_json
from langdetect import DetectorFactory, LangDetectException, detect
from pydantic import AnyHttpUrl, BaseModel, Field, model_validator

_LOGGER = logging.getLogger(__name__)

PARSER_METADATA_KEY = "parser_metadata"
AZURE_API_VERSION_KEY = "azure_api_version"
AZURE_MODEL_ID_KEY = "azure_model_id"
PARSING_DATE_KEY = "parsing_date"


class VerticalFlipError(Exception):
    """Exception for when a vertical flip fails."""

    pass


class BlockType(str, Enum):
    """
    List of possible block types from the PubLayNet model.

    https://layout-parser.readthedocs.io/en/latest/notes/modelzoo.html#model-label-map
    """

    TEXT = "Text"
    TITLE = "Title"
    LIST = "List"
    TABLE = "Table"
    TABLE_CELL = "TableCell"
    FIGURE = "Figure"
    INFERRED = "Inferred from gaps"
    # TODO: remove this when OCRProcessor._infer_block_type is implemented
    AMBIGUOUS = "Ambiguous"
    GOOGLE_BLOCK = "Google Text Block"
    PAGE_HEADER = "pageHeader"
    PAGE_FOOTER = "pageFooter"
    TITLE_LOWER_CASE = "title"
    SECTION_HEADING = "sectionHeading"
    PAGE_NUMBER = "pageNumber"
    DOCUMENT_HEADER = "Document Header"
    FOOT_NOTE = "footnote"


class TextBlock(BaseModel):
    """
    Base class for a text block.

    :attribute text: list of text lines contained in the text block :attribute
    text_block_id: unique identifier for the text block :attribute language: language
    of the text block. 2-letter ISO code, optional. :attribute type: predicted type of
    the text block :attribute type_confidence: confidence score of the text block
    being of the predicted type
    """

    text: List[str]
    text_block_id: str
    language: Optional[
        str
    ] = None  # TODO: validate this against a list of language ISO codes
    type: BlockType
    type_confidence: float = Field(ge=0, le=1)

    def to_string(self) -> str:
        """Returns lines in a text block separated by spaces as a string."""

        return " ".join([line.strip() for line in self.text])


class HTMLTextBlock(TextBlock):
    """
    Text block parsed from an HTML document.

    Type is set to "Text" with a confidence of 1.0 by default, as we do not predict
    types for text blocks parsed from HTML.
    """

    type: BlockType = BlockType.TEXT
    type_confidence: float = 1.0


class PDFTextBlock(TextBlock):
    """
    Text block parsed from a PDF document.

    Stores the text and positional information for a single text block extracted from
    a document.

    :attribute coords: list of coordinates of the vertices defining the boundary of
    the text block. Each coordinate is a tuple in the format (x, y). (0, 0) is at the
    top left corner of the page, and the positive x- and y- directions are right and
    down. :attribute page_number: page number of the page containing the text block.
    """

    coords: List[Tuple[float, float]]
    page_number: int = Field(ge=0)

    def to_string(self) -> str:
        """Returns lines in a text block separated by spaces as a string."""

        return " ".join([line.strip() for line in self.text])


class ParserInput(BaseModel):
    """Base class for input to a parser."""

    document_id: str
    document_name: str
    document_description: str
    document_source_url: Optional[AnyHttpUrl] = None
    document_cdn_object: Optional[str] = None
    document_content_type: Optional[str] = None
    document_md5_sum: Optional[str] = None
    document_slug: str
    document_metadata: BackendDocument

    pipeline_metadata: Json = {}  # note: defaulting to {} here is safe (pydantic)


class HTMLData(BaseModel):
    """Set of metadata specific to HTML documents."""

    detected_title: Optional[str] = None
    detected_date: Optional[date] = None
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

    :attribute pages: List of pages contained in the document :attribute filename:
    Name of the PDF file, without extension :attribute md5sum: md5sum of PDF content
    :attribute language: list of 2-letter ISO language codes, optional. If null,
    the OCR processor didn't support language detection
    """

    page_metadata: Sequence[PDFPageMetadata]
    md5sum: str
    text_blocks: Sequence[PDFTextBlock]


_PO = TypeVar("_PO", bound="BaseParserOutput")


class BaseParserOutput(BaseModel):
    """Base class for an output to a parser."""

    document_id: str
    document_metadata: dict
    document_name: str
    document_description: str
    document_source_url: Optional[AnyHttpUrl] = None
    document_cdn_object: Optional[str] = None
    document_content_type: Optional[str] = None
    document_md5_sum: Optional[str] = None
    document_slug: str

    languages: Optional[Sequence[str]] = None
    translated: bool = False
    html_data: Optional[HTMLData] = None
    pdf_data: Optional[PDFData] = None
    pipeline_metadata: Json = {}  # note: defaulting to {} here is safe (pydantic)

    @model_validator(mode="after")
    def check_html_pdf_metadata(self):
        """
        Validate the relationship between content-type and the data that is set.

        Check that html_data is set if content_type is HTML, or pdf_data is set if
        content_type is PDF.

        Check that if the content-type is not HTML or PDF, then html_data and pdf_data
        are both null.
        """
        if self.document_content_type == CONTENT_TYPE_HTML and self.html_data is None:
            raise ValueError("html_data must be set for HTML documents")

        if self.document_content_type == CONTENT_TYPE_PDF and self.pdf_data is None:
            raise ValueError("pdf_data must be set for PDF documents")

        if self.document_content_type not in {
            CONTENT_TYPE_HTML,
            CONTENT_TYPE_PDF,
        } and (self.html_data is not None or self.pdf_data is not None):
            raise ValueError(
                "html_data and pdf_data must be null for documents with no content type."
            )

        return self

    def get_text_blocks(self, including_invalid_html=False) -> Sequence[TextBlock]:
        """A method for getting text blocks with the option to include invalid html."""
        if self.document_content_type == CONTENT_TYPE_HTML and self.html_data:
            if not including_invalid_html and not self.html_data.has_valid_text:
                return []
        return self.text_blocks

    @property
    def text_blocks(self) -> Sequence[TextBlock]:
        """
        Return the text blocks in the document.

        These could differ in format depending on the content type.

        :return: Sequence[TextBlock]
        """
        if self.document_content_type == CONTENT_TYPE_HTML:
            html_data: Union[HTMLData, None] = self.html_data
            return html_data.text_blocks if html_data else []
        elif self.document_content_type == CONTENT_TYPE_PDF:
            pdf_data: Union[PDFData, None] = self.pdf_data
            return pdf_data.text_blocks if pdf_data else []
        return []

    def to_string(self) -> str:  # type: ignore
        """Return the text blocks in the parser output as a string"""

        return " ".join(
            [text_block.to_string().strip() for text_block in self.text_blocks]
        )

    def detect_and_set_languages(self: _PO) -> _PO:
        """
        Detect language of the text and set the language attribute.

        Return an instance of ParserOutput with the language attribute set. Assumes
        that a document only has one language.
        """

        if self.document_content_type != CONTENT_TYPE_HTML:
            _LOGGER.warning(
                "Language detection should not be required for non-HTML documents, "
                "but it has been run on one. This will overwrite any document "
                "languages detected via other means, e.g. OCR. "
            )

        # language detection is not deterministic, so we need to set a seed
        DetectorFactory.seed = 0

        if len(self.text_blocks) > 0:
            try:
                detected_language = detect(self.to_string())
            except LangDetectException:
                _LOGGER.warning(
                    "Language detection failed for document with id %s",
                    self.document_id,
                )
                detected_language = None
            self.languages = [detected_language] if detected_language else []
            for text_block in self.text_blocks:
                text_block.language = detected_language

        return self

    def set_document_languages_from_text_blocks(
        self: _PO, min_language_proportion: float = 0.4
    ) -> _PO:
        """
        Store the document languages attribute as part of the object.

        Done by getting all languages with proportion above `min_language_proportion`.

        :attribute min_language_proportion: Minimum proportion of text blocks in a
        language for it to be considered a language of the document.
        """

        all_text_block_languages = [
            text_block.language for text_block in self.text_blocks
        ]

        if all([lang is None for lang in all_text_block_languages]):
            self.languages = None

        else:
            lang_counter = Counter(
                [lang for lang in all_text_block_languages if lang is not None]
            )
            self.languages = [
                lang
                for lang, count in lang_counter.items()
                if count / len(all_text_block_languages) > min_language_proportion
            ]

        return self

    def vertically_flip_text_block_coords(self: _PO) -> _PO:
        """
        Flips the coordinates of all PDF text blocks vertically.

        Acts in-place on the coordinates in the ParserOutput object.

        Should the document fail to flip, a VerticalFlipError is raised. This is most
        commonly due to a page number being referenced in a text block that doesn't
        exist in the page_metadata mapping.
        """

        if self.pdf_data is None:
            return self

        page_height_map = {
            page.page_number: page.dimensions[1] for page in self.pdf_data.page_metadata
        }

        try:
            for text_block in self.pdf_data.text_blocks:
                if text_block.coords is not None and text_block.page_number is not None:
                    text_block.coords = [
                        (x, page_height_map[text_block.page_number] - y)
                        for x, y in text_block.coords
                    ]

                    # flip top and bottom so y values are still increasing as you go
                    # through the coordinates list
                    text_block.coords = [
                        text_block.coords[3],
                        text_block.coords[2],
                        text_block.coords[1],
                        text_block.coords[0],
                    ]
        except Exception as e:
            _LOGGER.exception(
                "Error flipping text block coordinates.",
                extra={"props": {"document_id": self.document_id}},
            )
            raise VerticalFlipError(
                f"Failed to flip text blocks for {self.document_id}"
            ) from e

        return self


class ParserOutput(BaseParserOutput):
    """Output to a parser with the metadata format used by the CPR backend."""

    document_metadata: BackendDocument

    @staticmethod
    def from_flat_json(data: dict):
        """Instantiate a parser output object from flat json."""

        unflattened = unflatten_json(data)

        # We remove optional fields that have complex nested structures.
        # E.g. if html_data had a value of None for has_valid_text, we need to remove
        # it as this would throw a validation error.
        unflattened = remove_key_if_all_nested_vals_none(unflattened, "html_data")
        unflattened = remove_key_if_all_nested_vals_none(unflattened, "pdf_data")

        return ParserOutput.model_validate(unflattened)
