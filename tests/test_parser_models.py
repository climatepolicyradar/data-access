import pydantic
import pytest

from cpr_data_access.parser_models import (
    ParserInput,
    ParserOutput,
    VerticalFlipError,
    PDFTextBlock
)
from cpr_data_access.pipeline_general_models import (
    CONTENT_TYPE_PDF,
    CONTENT_TYPE_HTML,
)


def test_parser_input_object(parser_output_json_pdf) -> None:
    """
    Test that we can correctly instantiate the parser input object.

    Also test the methods on the parser input object.
    """
    # Instantiate the parser input object
    parser_input = ParserInput.parse_obj(parser_output_json_pdf)

    # Test the to_json method
    parser_input.to_json()


def test_parser_output_object(parser_output_json_pdf, parser_output_json_html) -> None:
    """
    Test that we correctly instantiate the parser output object.

    Also test the methods on the parser output object.
    """

    # Instantiate the parser output object
    ParserOutput.parse_obj(parser_output_json_pdf)

    # Test the optional fields
    parser_output_empty_fields = parser_output_json_pdf.copy()
    parser_output_empty_fields["document_cdn_object"] = None
    parser_output_empty_fields["document_md5_sum"] = None

    ParserOutput.parse_obj(parser_output_empty_fields)

    # Test the check html pdf metadata method
    parser_output_no_pdf_data = parser_output_json_pdf.copy()
    parser_output_no_pdf_data["pdf_data"] = None
    parser_output_no_pdf_data["document_content_type"] = CONTENT_TYPE_PDF

    with pytest.raises(pydantic.error_wrappers.ValidationError) as context:
        ParserOutput.parse_obj(parser_output_no_pdf_data)
    assert "pdf_data must be set for PDF documents" in str(context.value)

    parser_output_no_html_data = parser_output_json_pdf.copy()
    parser_output_no_html_data["html_data"] = None
    parser_output_no_html_data["document_content_type"] = CONTENT_TYPE_HTML

    with pytest.raises(pydantic.error_wrappers.ValidationError) as context:
        ParserOutput.parse_obj(parser_output_no_html_data)
    assert "html_data must be set for HTML documents" in str(context.value)

    parser_output_no_content_type = parser_output_json_pdf.copy()
    # PDF data is set as the default
    parser_output_no_content_type["document_content_type"] = None

    with pytest.raises(pydantic.error_wrappers.ValidationError) as context:
        ParserOutput.parse_obj(parser_output_no_content_type)
    assert (
        "html_data and pdf_data must be null for documents with no content type."
    ) in str(context.value)

    parser_output_not_known_content_type = parser_output_json_pdf.copy()
    # PDF data is set as the default
    parser_output_not_known_content_type["document_content_type"] = "not_known"

    with pytest.raises(pydantic.error_wrappers.ValidationError) as context:
        ParserOutput.parse_obj(parser_output_not_known_content_type)
    assert (
        "html_data and pdf_data must be null for documents with no content type."
    ) in str(context.value)

    # Test the text blocks property
    assert ParserOutput.parse_obj(parser_output_json_pdf).text_blocks != []
    parser_output_no_data = parser_output_json_pdf.copy()
    parser_output_no_data["pdf_data"] = None
    parser_output_no_data["document_content_type"] = None
    assert ParserOutput.parse_obj(parser_output_no_data).text_blocks == []

    # Test the to string method
    assert ParserOutput.parse_obj(parser_output_json_pdf).to_string() != ""
    assert ParserOutput.parse_obj(parser_output_no_data).to_string() == ""

    # Test the flip coords method
    parser_output = ParserOutput.parse_obj(parser_output_json_pdf)
    original_text_blocks = parser_output.text_blocks
    assert parser_output.vertically_flip_text_block_coords() != original_text_blocks

    parser_output = ParserOutput.parse_obj(parser_output_json_pdf)
    # Set as page number that doesn't exist in the page_metadata field to throw exception
    assert isinstance(parser_output.text_blocks[0], PDFTextBlock)
    parser_output.text_blocks[0].page_number = 123456  # type: ignore

    with pytest.raises(VerticalFlipError) as context:
        parser_output.vertically_flip_text_block_coords()
    assert str(context.value) == (
        f"Failed to flip text blocks for {parser_output.document_id}"
    )

    # Test the get_text_blocks method
    # The test html document has invalid html data so the text blocks should be empty
    parser_output = ParserOutput.parse_obj(parser_output_json_html)
    assert parser_output.get_text_blocks() == []
    assert (
        parser_output.get_text_blocks(including_invalid_html=True)
        == parser_output.text_blocks
    )

    # Test that the get_text_blocks method works on pdfs. This test pdf has data so we
    # should get text blocks.
    parser_output = ParserOutput.parse_obj(parser_output_json_pdf)
    text_blocks_raw = parser_output.get_text_blocks()
    assert text_blocks_raw
    text_blocks_include_invalid = parser_output.get_text_blocks(
        including_invalid_html=True
    )
    assert text_blocks_include_invalid
    text_blocks_not_include_invalid = parser_output.get_text_blocks(
        including_invalid_html=False
    )
    assert text_blocks_not_include_invalid
    assert (
        text_blocks_raw
        == text_blocks_include_invalid
        == text_blocks_not_include_invalid
    )

    # Test that the correct validation error is thrown during instantiation
    parser_output_json_bad_text_block = parser_output_json_pdf.copy()
    parser_output_json_bad_text_block["pdf_data"]["text_blocks"][0]["type"] = (
        "ThisBlockTypeDoesNotExist"
    )
    with pytest.raises(pydantic.error_wrappers.ValidationError) as context:
        ParserOutput.parse_obj(parser_output_json_bad_text_block)
    assert "value is not a valid enumeration member" in str(context.value)
