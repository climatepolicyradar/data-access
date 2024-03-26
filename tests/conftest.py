import json
from pathlib import Path
import tempfile

import pytest
import boto3
from moto import mock_aws


VESPA_TEST_SEARCH_URL = "http://localhost:8080"


@pytest.fixture()
def fake_vespa_credentials():
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(Path(tmpdir) / "cert.pem", "w"):
            pass
        with open(Path(tmpdir) / "key.pem", "w"):
            pass
        yield tmpdir


@pytest.fixture()
def s3_client():
    with mock_aws():
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket="test-bucket")
        s3_client.put_object(
            Bucket="test-bucket", Key="test-prefix/test1.txt", Body="test1 text"
        )
        s3_client.put_object(
            Bucket="test-bucket", Key="test-prefix/subdir/test2.txt", Body="test2 text"
        )
        s3_client.put_object(
            Bucket="test-bucket",
            Key="test-wrongprefix/subdir/test3.txt",
            Body="test3 text",
        )

        for file in Path("tests/test_data/valid").glob("*.json"):
            s3_client.put_object(
                Bucket="test-bucket",
                Key=f"embeddings_input/{file.name}",
                Body=file.read_text(),
            )

        s3_client.create_bucket(Bucket="empty-bucket")

        yield s3_client


@pytest.fixture()
def parser_output_json_pdf() -> dict:
    """A dictionary representation of a parser output"""
    with open("tests/test_data/valid/test_pdf.json") as f:
        return json.load(f)


@pytest.fixture()
def parser_output_json_html() -> dict:
    """A dictionary representation of a parser output"""
    with open("tests/test_data/valid/test_html.json") as f:
        return json.load(f)


@pytest.fixture()
def parser_output_json_flat() -> dict:
    """A dictionary representation of a parser output that is flat"""
    with open("tests/test_data/huggingface/flat_hf_parser_output.json") as f:
        return json.load(f)


@pytest.fixture()
def backend_document_json() -> dict:
    """A dictionary representation of a backend document"""
    return {
        "name": "test_name",
        "description": "test_description",
        "import_id": "test_import_id",
        "slug": "test_slug",
        "family_import_id": "test_family_import_id",
        "family_slug": "test_family_slug",
        "publication_ts": "2021-01-01T00:00:00+00:00",
        "date": "01/01/2021",
        "source_url": "test_source_url",
        "download_url": "test_download_url",
        "type": "test_type",
        "source": "test_source",
        "category": "test_category",
        "geography": "test_geography",
        "languages": ["test_language"],
        "metadata": {"test_metadata": "test_value"},
    }
