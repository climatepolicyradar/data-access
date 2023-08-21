import unittest

import pydantic

from cpr_data_access.parser_models import (
    ParserInput,
    ParserOutput,
)
from cpr_data_access.pipeline_general_models import (
    CONTENT_TYPE_PDF,
    CONTENT_TYPE_HTML,
)


def test_parser_input_object(parser_output_json) -> None:
    """
    Test that we can correctly instantiate the parser input object.

    Also test the methods on the parser input object.
    """
    # Instantiate the parser input object
    parser_input = ParserInput.parse_obj(parser_output_json)

    # Test the to_json method
    parser_input.to_json()


def test_parser_output_object(parser_output_json) -> None:
    """
    Test that we correctly instantiate the parser output object.

    Also test the methods on the parser output object.
    """

    # Instantiate the parser output object
    ParserOutput.parse_obj(parser_output_json)

    # Test the optional fields
    parser_output_empty_fields = parser_output_json.copy()
    parser_output_empty_fields["document_cdn_object"] = None
    parser_output_empty_fields["document_md5_sum"] = None

    ParserOutput.parse_obj(parser_output_empty_fields)

    # Test the check html pdf metadata method
    parser_output_no_pdf_data = parser_output_json.copy()
    parser_output_no_pdf_data["pdf_data"] = None
    parser_output_no_pdf_data["document_content_type"] = CONTENT_TYPE_PDF

    with unittest.TestCase().assertRaises(
        pydantic.error_wrappers.ValidationError
    ) as context:
        ParserOutput.parse_obj(parser_output_no_pdf_data)
    assert "pdf_data must be set for PDF documents" in str(context.exception)

    parser_output_no_html_data = parser_output_json.copy()
    parser_output_no_html_data["html_data"] = None
    parser_output_no_html_data["document_content_type"] = CONTENT_TYPE_HTML

    with unittest.TestCase().assertRaises(
        pydantic.error_wrappers.ValidationError
    ) as context:
        ParserOutput.parse_obj(parser_output_no_html_data)
    assert "html_data must be set for HTML documents" in str(context.exception)

    parser_output_no_content_type = parser_output_json.copy()
    # PDF data is set as the default
    parser_output_no_content_type["document_content_type"] = None

    with unittest.TestCase().assertRaises(
        pydantic.error_wrappers.ValidationError
    ) as context:
        ParserOutput.parse_obj(parser_output_no_content_type)
    assert (
        "html_data and pdf_data must be null for documents with no content type."
    ) in str(context.exception)

    parser_output_not_known_content_type = parser_output_json.copy()
    # PDF data is set as the default
    parser_output_not_known_content_type["document_content_type"] = "not_known"

    with unittest.TestCase().assertRaises(
        pydantic.error_wrappers.ValidationError
    ) as context:
        ParserOutput.parse_obj(parser_output_not_known_content_type)
    assert (
        "html_data and pdf_data must be null for documents with no content type."
    ) in str(context.exception)

    # Test the text blocks property
    assert ParserOutput.parse_obj(parser_output_json).text_blocks != []
    parser_output_no_data = parser_output_json.copy()
    parser_output_no_data["pdf_data"] = None
    parser_output_no_data["document_content_type"] = None
    assert ParserOutput.parse_obj(parser_output_no_data).text_blocks == []

    # Test the to string method
    assert ParserOutput.parse_obj(parser_output_json).to_string() != ""
    assert ParserOutput.parse_obj(parser_output_no_data).to_string() == ""
