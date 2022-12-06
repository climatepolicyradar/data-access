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
