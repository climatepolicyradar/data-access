import pytest

from cpr_data_access.parser_models import ParserOutput
from cpr_data_access.models import Dataset, Document, Span


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


@pytest.fixture
def test_spans_valid(test_document) -> list[Span]:
    """Test spans."""
    return [
        Span(
            document_id=test_document.document_id,
            document_text_hash=test_document.text_hash,
            start_idx=0,
            end_idx=5,
            text="test1",
            sentence="test1 first",
            id="test sentence 1",
            type="TEST",
            pred_probability=1,
        ),
        Span(
            document_id=test_document.document_id,
            document_text_hash=test_document.text_hash,
            start_idx=20,
            end_idx=25,
            text="test2",
            sentence="test2 second",
            id="test sentence 2",
            type="TEST",
            pred_probability=0.99,
        ),
    ]


@pytest.fixture
def test_spans_invalid(test_document) -> list[Span]:
    """Test spans."""
    return [
        Span(
            document_id="1234",
            document_text_hash=test_document.text_hash,
            start_idx=0,
            end_idx=5,
            text="test1",
            sentence="test1 first",
            id="test sentence 1",
            type="TEST",
            pred_probability=1,
        ),
        Span(
            document_id="abcd",
            document_text_hash=test_document.text_hash,
            start_idx=20,
            end_idx=25,
            text="test2",
            sentence="test2 second",
            id="test sentence 2",
            type="TEST",
            pred_probability=0.99,
        ),
        Span(
            document_id="abcd",
            document_text_hash="1234",
            start_idx=20,
            end_idx=25,
            text="test3",
            sentence="test3 second",
            id="test sentence 3",
            type="TEST",
            pred_probability=0.99,
        ),
    ]


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


@pytest.mark.parametrize("raise_on_error", [True, False])
def test_add_valid_spans(test_document, test_spans_valid, raise_on_error):

    document_with_spans = test_document.add_spans(
        test_spans_valid, raise_on_error=raise_on_error
    )

    assert len(document_with_spans.spans) == len(test_spans_valid)
    # Check that all spans are unique
    assert len(set(document_with_spans.spans)) == len(document_with_spans.spans)


def test_add_invalid_spans(test_document, test_spans_invalid):
    document_with_spans = test_document.add_spans(
        test_spans_invalid, raise_on_error=False
    )

    assert len(document_with_spans.spans) == 0

    with pytest.raises(ValueError):
        test_document.add_spans(test_spans_invalid, raise_on_error=True)


@pytest.mark.parametrize("raise_on_error", [True, False])
def test_add_spans_empty_document(
    test_document, test_spans_valid, test_spans_invalid, raise_on_error
):
    """Document.add_spans() should always raise if the document is empty."""
    empty_document = test_document.copy()
    empty_document.text_blocks = None

    # When the document is empty, no spans should be added
    all_spans = test_spans_valid + test_spans_invalid

    with pytest.raises(ValueError):
        empty_document.add_spans(all_spans, raise_on_error=raise_on_error)
