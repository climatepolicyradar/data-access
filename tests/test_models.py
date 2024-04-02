from pathlib import Path

import pytest
import pandas as pd
from typing import Iterable

from datasets import Dataset as HuggingFaceDataset
from cpr_sdk.models import (
    Dataset,
    BaseDocument,
    GSTDocument,
    CPRDocument,
    CPRDocumentMetadata,
    Span,
    KnowledgeBaseIDs,
    TextBlock,
    BlockType,
)


@pytest.fixture
def test_dataset() -> Dataset:
    """Create dataset load_from_local and use as a fixture."""
    dataset = (
        Dataset(document_model=BaseDocument)
        .load_from_local("tests/test_data/valid")
        .add_metadata(
            target_model=CPRDocument,
            metadata_csv_path=Path("tests/test_data/CPR_metadata.csv"),
        )
    )

    assert len(dataset) == 3
    return dataset


@pytest.fixture
def test_dataset_languages(test_dataset) -> Dataset:
    """Defines specific languages for filtering on test_dataset"""
    test_dataset.documents[0].languages = ["fr"]
    test_dataset.documents[1].languages = ["en", "fr"]
    test_dataset.documents[2].languages = ["en"]
    return test_dataset


@pytest.fixture
def test_dataset_gst() -> Dataset:
    dataset = (
        Dataset(document_model=BaseDocument)
        .load_from_local("tests/test_data/valid_gst")
        .add_metadata(
            target_model=GSTDocument,
            metadata_csv_path=Path("tests/test_data/GST_metadata.csv"),
        )
    )
    assert len(dataset) == 1
    return dataset


@pytest.fixture
def test_document(test_dataset) -> BaseDocument:
    """Test PDF document."""

    return [
        doc
        for doc in test_dataset.documents
        if doc.document_id == "CCLW.executive.1003.0"
    ][0]


@pytest.fixture
def test_huggingface_dataset_cpr() -> HuggingFaceDataset:
    """Test HuggingFace dataset."""

    return HuggingFaceDataset.from_parquet(
        "tests/test_data/CPR_huggingface_data_sample.parquet"
    )


@pytest.fixture
def test_huggingface_dataset_gst() -> HuggingFaceDataset:
    """Test HuggingFace dataset."""

    return HuggingFaceDataset.from_parquet(
        "tests/test_data/GST_huggingface_data_sample.parquet"
    )


def test_dataset_metadata_df(test_dataset):
    metadata_df = test_dataset.metadata_df

    assert isinstance(metadata_df, pd.DataFrame)
    assert len(metadata_df) == len(test_dataset)
    assert metadata_df.shape[1] > 0

    for col in ("text_blocks", "document_metadata"):
        assert col not in metadata_df.columns

    for col in ("num_text_blocks", "num_pages"):
        assert col in metadata_df.columns

    for key in CPRDocumentMetadata.model_fields.keys() | {"publication_year"}:
        assert key in metadata_df.columns


@pytest.fixture
def test_spans_valid(test_document) -> list[Span]:
    """Test spans."""
    return [
        Span(
            document_id=test_document.document_id,
            text_block_text_hash="0c8f98b268ce90f7bcd7d9bee09863fa__81e9c9f2b0fe330c612f8605b6d1df98ffa8f8df35e98c4e2b6749bda61b8b63",
            start_idx=12,
            end_idx=23,
            text="Agriculture",
            sentence="Contact: DG Agriculture and\nRural Development",
            id="test sentence 1",
            type="TEST",
            pred_probability=1,
            annotator="pytest",
        ),
        Span(
            document_id=test_document.document_id,
            text_block_text_hash="a12ff2b1979c932f07792d57aa6aacdc__ea4e549f185fd2237fdac7719cf0c6d88fc939fa4aa5a3b5574f3f7b4804ac26",
            start_idx=8,
            end_idx=11,
            text="CAP",
            sentence="The new CAP maintains the two pillars, but increases the links\nbetween them, thus offering a more holistic and integrated approach\nto policy support.",
            id="test sentence 2",
            type="TEST",
            pred_probability=0.99,
            annotator="pytest",
        ),
        Span(
            document_id=test_document.document_id,
            text_block_text_hash="a12ff2b1979c932f07792d57aa6aacdc__ea4e549f185fd2237fdac7719cf0c6d88fc939fa4aa5a3b5574f3f7b4804ac26",
            start_idx=4,
            end_idx=7,
            text="new",
            sentence="The new CAP maintains the two pillars, but increases the links\nbetween them, thus offering a more holistic and integrated approach\nto policy support.",
            id="test sentence 2",
            type="TEST",
            pred_probability=0.99,
            annotator="pytest",
            kb_ids=KnowledgeBaseIDs(
                wikidata_id="Q42",
                wikipedia_title="Douglas_Adams",
            ),
        ),
    ]


@pytest.fixture
def test_spans_invalid(test_document) -> list[Span]:
    """Test spans."""
    return [
        # invalid document id
        Span(
            document_id="abcd",
            text_block_text_hash="0c8f98b268ce90f7bcd7d9bee09863fa__81e9c9f2b0fe330c612f8605b6d1df98ffa8f8df35e98c4e2b6749bda61b8b63",
            start_idx=0,
            end_idx=5,
            text="test2",
            sentence="test2 second",
            id="test sentence 2",
            type="TEST",
            pred_probability=0.99,
            annotator="pytest",
        ),
        # invalid text block hash
        Span(
            document_id=test_document.document_id,
            text_block_text_hash="1234",
            start_idx=0,
            end_idx=5,
            text="test3",
            sentence="test3 second",
            id="test sentence 3",
            type="TEST",
            pred_probability=0.99,
            annotator="pytest",
        ),
    ]


def test_dataset_filter_by_language(test_dataset_languages):
    """Test Dataset.filter_by_language."""
    dataset = test_dataset_languages.filter_by_language("en")

    assert len(dataset) == 1, f"found {[d.languages for d in dataset]}"
    assert dataset.documents[0].languages == ["en"]


def test_dataset_filter_by_language__not_strict(test_dataset_languages):
    """Test Dataset.filter_by_language."""
    dataset_2 = test_dataset_languages.filter_by_language("en", strict_match=False)

    assert len(dataset_2) == 2, f"found {[d.languages for d in dataset_2]}"
    assert dataset_2.documents[0].languages == ["en", "fr"]
    assert dataset_2.documents[1].languages == ["en"]


def test_dataset_filter_by_language__strict(test_dataset_languages):
    """Test Dataset.filter_by_language."""
    dataset_3 = test_dataset_languages.filter_by_language("en", strict_match=True)

    assert len(dataset_3) == 1, f"found {[d.languages for d in dataset_3]}"
    assert dataset_3.documents[0].languages == ["en"]


def test_dataset_filter_by_corpus(test_dataset):
    """Test Dataset.filter_by_corpus"""
    dataset = test_dataset.filter_by_corpus("UNFCCC")

    assert len(dataset) == 0

    dataset = test_dataset.filter_by_corpus("CCLW")

    assert len(dataset) == 3


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


def test_dataset_sample_text_blocks(test_dataset):
    text_blocks = test_dataset.sample_text_blocks(2)
    num_text_blocks = sum(
        [
            len(doc.text_blocks) if doc.text_blocks is not None else 0
            for doc in test_dataset.documents
        ]
    )

    assert len(text_blocks) == 2
    assert len(text_blocks) < num_text_blocks


def test_text_block_add_valid_spans(test_document, test_spans_valid):
    block_1 = test_document.text_blocks[0]
    block_2 = test_document.text_blocks[1]

    block_1_span_added = block_1._add_spans([test_spans_valid[0]])
    block_2_span_added = block_2._add_spans([test_spans_valid[1], test_spans_valid[2]])

    assert len(block_1_span_added.spans) == 1
    assert len(block_2_span_added.spans) == 2


def test_text_block_add_invalid_spans(test_document, test_spans_invalid, caplog):
    text_block_with_spans = test_document.text_blocks[0]._add_spans(
        [test_spans_invalid[0]], raise_on_error=False
    )

    # This will add the text block and warn that the incorrect document ID was ignored
    assert len(text_block_with_spans.spans) == 1

    # This won't add the text block, as the text block hash is incorrect
    text_block_with_spans = test_document.text_blocks[1]._add_spans(
        [test_spans_invalid[1]], raise_on_error=False
    )
    assert len(text_block_with_spans.spans) == 0

    # This will raise as the second text block can't be added
    with pytest.raises(ValueError):
        test_document.add_spans(test_spans_invalid, raise_on_error=True)


@pytest.mark.parametrize("raise_on_error", [True, False])
def test_add_spans_empty_text_block(
    test_document, test_spans_valid, test_spans_invalid, raise_on_error
):
    text_block = test_document.text_blocks[0]
    text_block.text = ""

    all_spans = test_spans_valid + test_spans_invalid

    with pytest.raises(ValueError):
        text_block._add_spans(all_spans, raise_on_error=raise_on_error)


@pytest.mark.parametrize("raise_on_error", [True, False])
def test_document_add_valid_spans(test_document, test_spans_valid, raise_on_error):
    document_with_spans = test_document.add_spans(
        test_spans_valid, raise_on_error=raise_on_error
    )

    added_spans = [
        span
        for text_block in document_with_spans.text_blocks
        for span in text_block.spans
    ]

    assert len(added_spans) == len(test_spans_valid)
    # Check that all spans are unique
    assert len(set(added_spans)) == len(test_spans_valid)


def test_document_add_invalid_spans(test_document, test_spans_invalid):
    document_with_spans = test_document.add_spans(
        test_spans_invalid, raise_on_error=False
    )

    added_spans = [
        span
        for text_block in document_with_spans.text_blocks
        for span in text_block.spans
    ]
    assert len(added_spans) == 0

    with pytest.raises(ValueError):
        test_document.add_spans(test_spans_invalid, raise_on_error=True)


@pytest.mark.parametrize("raise_on_error", [True, False])
def test_add_spans_empty_document(
    test_document, test_spans_valid, test_spans_invalid, raise_on_error
):
    """Document.add_spans() should always raise if the document is empty."""
    empty_document = test_document.model_copy()
    empty_document.text_blocks = None

    # When the document is empty, no spans should be added
    all_spans = test_spans_valid + test_spans_invalid

    with pytest.raises(ValueError):
        empty_document.add_spans(all_spans, raise_on_error=raise_on_error)


@pytest.mark.parametrize("raise_on_error", [True, False])
def test_dataset_add_spans(test_dataset, test_spans_valid, raise_on_error):
    dataset_with_spans = test_dataset.add_spans(
        test_spans_valid, raise_on_error=raise_on_error
    )
    added_spans = [
        span
        for document in dataset_with_spans.documents
        if document.text_blocks is not None
        for text_block in document.text_blocks
        for span in text_block.spans
    ]

    assert len(added_spans) == len(test_spans_valid)
    # Check that all spans are unique
    assert len(set(added_spans)) == len(test_spans_valid)


def test_span_validation(test_spans_valid):
    """Test that spans produce uppercase span IDs and types."""
    for span in test_spans_valid:
        assert span.id.isupper()
        assert span.type.isupper()


def test_document_get_text_block_window(test_document):
    """Test Document.get_text_block_window() for success and failure cases."""
    text_block = test_document.text_blocks[3]
    window = test_document.get_text_block_window(text_block, (-2, 2))
    assert window == test_document.text_blocks[1:6]

    text_block = test_document.text_blocks[0]
    window = test_document.get_text_block_window(text_block, (-2, 2))
    assert window == test_document.text_blocks[:3]

    with pytest.raises(ValueError):
        test_document.get_text_block_window(text_block, (2, 2))

    with pytest.raises(ValueError):
        test_document.get_text_block_window(text_block, (2, -2))


def test_document_get_text_window(test_document):
    """Test Document.get_text_window()."""
    text_block = test_document.text_blocks[3]
    text_window = test_document.get_text_window(text_block, (-2, 2))
    assert isinstance(text_window, str)
    assert len(text_window) > len(text_block.to_string())


def test_dataset_to_huggingface(test_dataset, test_dataset_gst):
    """Test that the HuggingFace dataset can be created."""
    dataset_hf = test_dataset.to_huggingface()
    dataset_gst_hf = test_dataset_gst.to_huggingface()
    assert isinstance(dataset_hf, HuggingFaceDataset)
    assert isinstance(dataset_gst_hf, HuggingFaceDataset)
    assert len(dataset_hf) == sum(
        len(doc.text_blocks) for doc in test_dataset.documents if doc.text_blocks
    )
    assert len(dataset_gst_hf) == sum(
        len(doc.text_blocks) for doc in test_dataset_gst.documents if doc.text_blocks
    )


@pytest.mark.parametrize("limit", [None, 2])
def test_dataset_from_huggingface_cpr(test_huggingface_dataset_cpr, limit):
    """Test that a CPR dataset can be created from a HuggingFace dataset."""
    dataset = Dataset(document_model=CPRDocument)._from_huggingface_parquet(
        test_huggingface_dataset_cpr, limit=limit
    )

    assert isinstance(dataset, Dataset)
    assert all(isinstance(doc, CPRDocument) for doc in dataset.documents)

    if limit is None:
        limit = len({d["document_id"] for d in test_huggingface_dataset_cpr})

        # Check huggingface dataset has the same number of text blocks as the dataset
        assert sum(len(doc.text_blocks or []) for doc in dataset.documents) == len(
            test_huggingface_dataset_cpr
        )

    # Check huggingface dataset has the same number of documents as the dataset or the set limit
    assert len(dataset) == limit


def test_dataset_from_huggingface_gst(test_huggingface_dataset_gst):
    """Test that a dataset can be created from a HuggingFace dataset."""
    dataset = Dataset(document_model=GSTDocument)._from_huggingface_parquet(
        test_huggingface_dataset_gst
    )

    assert isinstance(dataset, Dataset)
    assert all(isinstance(doc, GSTDocument) for doc in dataset.documents)

    assert any(doc.languages is not None for doc in dataset.documents)

    # Check hugingface dataset has the same number of documents as the dataset
    assert len(dataset) == len({d["document_id"] for d in test_huggingface_dataset_gst})

    # Check huggingface dataset has the same number of text blocks as the dataset
    assert sum(len(doc.text_blocks or []) for doc in dataset.documents) == len(
        test_huggingface_dataset_gst
    )


def test_dataset_indexable(test_dataset):
    """Tests that the dataset can be indexed to get documents"""
    assert isinstance(test_dataset[0], BaseDocument)


def test_dataset_iterable(test_dataset):
    """Tests that the dataset is an iterable"""
    assert isinstance(test_dataset, Iterable)
    for doc in test_dataset:
        assert isinstance(doc, BaseDocument)


def test_display_text_block(test_document, test_spans_valid):
    document_with_spans = test_document.add_spans(
        test_spans_valid, raise_on_error=False
    )

    block = [block for block in document_with_spans.text_blocks if block.spans][0]

    # TODO: test 'span' as well as 'ent' display style
    block_html = block.display("ent")

    assert isinstance(block_html, str)
    assert len(block_html) > 0
    assert block_html.startswith("<div")


def test_text_block_hashable(test_document):
    doc = test_document

    set(doc.text_blocks)

    first_block_hash = doc.text_blocks[0].__hash__()
    assert isinstance(first_block_hash, int)

    comparison_block = TextBlock(**doc.text_blocks[0].model_dump())

    assert comparison_block == doc.text_blocks[0]

    comparison_block.text_block_id = "0"

    assert comparison_block != doc.text_blocks[0]


def test_dataset_sample(test_dataset):
    dataset = test_dataset

    sample_1 = dataset.sample(1, random_state=20)
    sample_2 = dataset.sample(1, random_state=20)
    sample_3 = dataset.sample(1, random_state=40)

    assert len(sample_1) == 1
    assert sample_1.documents == sample_2.documents
    assert sample_1.documents != sample_3.documents

    sample_4 = dataset.sample(len(dataset) * 2, random_state=20)

    assert len(sample_4) == len(dataset)

    sample_5 = dataset.sample(1 / 3)

    assert len(sample_5) == len(dataset) / 3

    with pytest.raises(
        ValueError,
        match=r"n should be a float in \(0.0, 1.0\) or a positive integer. Provided value: -1",
    ):
        _ = dataset.sample(-1)


def test_dataset_dict(test_dataset):
    dataset = test_dataset

    d2 = Dataset(**dataset.dict())

    for k, v in dataset.__dict__.items():
        assert v == getattr(d2, k)

    d3_dict = dataset.dict(exclude=["documents", "document_model"])

    assert "documents" not in d3_dict.keys()
    assert "document_model" not in d3_dict.keys()

    d4_dict = dataset.dict(exclude="documents")

    assert "documents" not in d4_dict.keys()


def test_document_to_markdown(test_document):
    md = test_document.to_markdown(show_debug_elements=False)

    assert isinstance(md, str)
    assert len(md) > 0

    # Hide text elements - should be shorter
    md_debug = test_document.to_markdown(
        show_debug_elements=False, debug_only_types={BlockType.TEXT}
    )
    assert len(md_debug) < len(md)
