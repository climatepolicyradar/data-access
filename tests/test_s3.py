import pytest

from src.cpr_data_access.s3 import _s3_object_read_text, _get_s3_keys_with_prefix


def test_s3_get_keys_with_prefix(s3_client):
    keys = _get_s3_keys_with_prefix("s3://test-bucket/test-prefix")
    assert sorted(keys) == sorted(
        [
            "test-prefix/test1.txt",
            "test-prefix/subdir/test2.txt",
        ]
    )

    with pytest.raises(ValueError, match="Bucket non-existent-bucket does not exist"):
        _ = _get_s3_keys_with_prefix("s3://non-existent-bucket/test-prefix")

    files_non_existent_dir = _get_s3_keys_with_prefix(
        "s3://test-bucket/non-existent-prefix"
    )
    assert files_non_existent_dir == []


def test_s3_object_read_text(s3_client):
    text = _s3_object_read_text("s3://test-bucket/test-prefix/test1.txt")
    assert text == "test1 text"

    with pytest.raises(ValueError, match="Bucket non-existent-bucket does not exist"):
        _ = _s3_object_read_text("s3://non-existent-bucket/file.json")

    with pytest.raises(ValueError, match="Key non-existent-file.json does not exist"):
        _ = _s3_object_read_text("s3://test-bucket/non-existent-file.json")
