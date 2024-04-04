import re

import boto3
from aws_error_utils.aws_error_utils import errors

ID_PATTERN = re.compile(r"^[a-zA-Z0-9]+\.[a-zA-Z0-9]+\.[a-zA-Z0-9]+\.[a-zA-Z0-9]+")
S3_PATTERN = re.compile(r"s3://(?P<bucket>[\w-]+)/(?P<prefix>.+)")


def _get_filename(source_folder: str, filepath: str) -> str:
    return filepath[len(source_folder) :].lstrip("/")


def _get_s3_keys_with_prefix(s3_prefix: str) -> list[str]:
    """
    Get a list of keys in an S3 bucket with a given prefix. Returns an empty list if the prefix does not exist or is empty.

    We use this instead of cloudpathlib's glob because it's much faster. Relevant issue: https://github.com/drivendataorg/cloudpathlib/issues/274.

    :param s3_prefix: prefix, including s3:// at the start
    :raises Exception: if prefix does not represent an s3 path
    :return list[str]: list of full paths to objects in bucket, excluding s3:// prefix
    """
    s3_match = S3_PATTERN.match(s3_prefix)
    if s3_match is None:
        raise Exception(f"Prefix does not represent an s3 path: {s3_prefix}")

    bucket = s3_match.group("bucket")
    prefix = s3_match.group("prefix").rstrip("/") + "/"
    s3client = boto3.client("s3")

    try:
        list_response = s3client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    except errors.NoSuchBucket:
        raise ValueError(f"Bucket {bucket} does not exist")
    except Exception as e:
        raise e

    files = [o["Key"] for o in list_response.get("Contents", []) if o["Key"] != prefix]

    finished_listing = not list_response["IsTruncated"]
    while not finished_listing:
        continuation_token = list_response.get("NextContinuationToken")
        list_response = s3client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            ContinuationToken=continuation_token,
        )
        files.extend(
            [o["Key"] for o in list_response["Contents"] if o["Key"] != prefix]
        )
        finished_listing = not list_response["IsTruncated"]

    return files


def _s3_object_read_text(s3_path: str) -> str:
    """
    Read text from an S3 object.

    :param s3_key: path to S3 object, including s3:// prefix
    :return str: text of S3 object
    """
    s3_match = S3_PATTERN.match(s3_path)
    if s3_match is None:
        raise Exception(f"Key does not represent an s3 path: {s3_path}")

    bucket = s3_match.group("bucket")
    key = s3_match.group("prefix")
    s3client = boto3.client("s3")

    try:
        response = s3client.get_object(Bucket=bucket, Key=key)
    except errors.NoSuchBucket:
        raise ValueError(f"Bucket {bucket} does not exist")
    except errors.NoSuchKey:
        raise ValueError(f"Key {key} does not exist")
    except Exception as e:
        raise e

    return response["Body"].read().decode("utf-8")
