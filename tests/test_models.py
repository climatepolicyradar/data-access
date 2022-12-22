import pytest

from cpr_data_access.parser_models import ParserOutput
from cpr_data_access.models import Dataset, Document


@pytest.fixture
def test_dataset() -> Dataset:
    """Create dataset load_from_local and use as a fixture."""
    dataset = Dataset().load_from_local("tests/test_data/valid")

    return dataset


@pytest.fixture
def test_document() -> Document:
    """Test PDF document."""
    return Document.from_parser_output(
        ParserOutput.parse_file("tests/test_data/valid/test_pdf.json")
    )


def test_dataset_filter_by_language(test_dataset):
    """Test Dataset.filter_by_language."""
    dataset = test_dataset.filter_by_language("en")

    assert len(dataset) == 2
    assert dataset.documents[0].languages == ["en"]
    assert dataset.documents[1].languages == ["en"]


def test_document_set_url(test_document):
    doc_with_url = test_document.with_document_url(
        cdn_domain="dev.cdn.climatepolicyradar.org"
    )
    assert (
        doc_with_url.document_url
        == "https://dev.cdn.climatepolicyradar.org/EUR/2013/EUR-2013-01-01-Overview+of+CAP+Reform+2014-2020_6237180d8c443d72c06c9167019ca177.pdf"
    )


def test_dataset_get_all_text_blocks(test_dataset):
    text_blocks = test_dataset.get_all_text_blocks()
    num_text_blocks = sum(
        [
            len(doc.text_blocks) if doc.text_blocks is not None else 0
            for doc in test_dataset.documents
        ]
    )

    assert len(text_blocks) == num_text_blocks

    text_blocks_with_document_context = test_dataset.get_all_text_blocks(
        with_document_context=True
    )
    assert len(text_blocks_with_document_context) == num_text_blocks
    assert all([isinstance(i[1], dict) for i in text_blocks_with_document_context])
    assert all(["text_blocks" not in i[1] for i in text_blocks_with_document_context])
