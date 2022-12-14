from pathlib import Path

import pytest
import boto3
from moto import mock_s3


@pytest.fixture()
def s3_client():
    with mock_s3():
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
