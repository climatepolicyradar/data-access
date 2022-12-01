from pathlib import Path

import pytest
from pydantic.error_wrappers import ValidationError

from src.cpr_data_access.data_adaptors import S3DataAdaptor, LocalDataAdaptor


def test_local_data_adaptor_valid_data():
    adaptor = LocalDataAdaptor()
    dataset = adaptor.load_dataset("tests/test_data/valid")
    assert len(dataset) == 3


def test_local_data_adaptor_invalid_data():
    adaptor = LocalDataAdaptor()
    with pytest.raises(ValidationError):
        _ = adaptor.load_dataset("tests/test_data/invalid")


def test_local_data_adaptor_non_existent_data():
    # Directory contains no JSON files
    adaptor = LocalDataAdaptor()
    with pytest.raises(
        ValueError,
        match=f"Path {Path('tests/test_data/').resolve()} does not contain any json files",
    ):
        _ = adaptor.load_dataset("tests/test_data/")

    # Directory does not exist
    with pytest.raises(
        ValueError,
        match=f"Path {Path('tests/test_data/non_existent_dir').resolve()} does not exist",
    ):
        _ = adaptor.load_dataset("tests/test_data/non_existent_dir")

    # File instead of directory
    with pytest.raises(
        ValueError,
        match=f"Path {Path('tests/test_data/valid/test_html.json').resolve()} is not a directory",
    ):
        _ = adaptor.load_dataset("tests/test_data/valid/test_html.json")


def test_s3_data_adaptor_valid_data(s3_client):
    adaptor = S3DataAdaptor()
    dataset = adaptor.load_dataset("test-bucket")
    assert len(dataset) == 3


def test_s3_data_adaptor_non_existent_data(s3_client):
    adaptor = S3DataAdaptor()
    with pytest.raises(ValueError, match="Bucket non-existent-bucket does not exist"):
        _ = adaptor.load_dataset("non-existent-bucket")

    with pytest.raises(
        ValueError,
        match="No objects found in 'embeddings_input' folder in S3 bucket empty-bucket",
    ):
        _ = adaptor.load_dataset("empty-bucket")
