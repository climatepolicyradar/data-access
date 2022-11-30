"""Adaptors for getting and storing data from CPR data sources."""

from abc import ABC, abstractmethod
from typing import List, Optional
from pathlib import Path

from tqdm.auto import tqdm

from src.cpr_data_access.s3 import _get_s3_keys_with_prefix, _s3_object_read_text
from src.cpr_data_access.parser_models import ParserOutput


class DataAdaptor(ABC):
    """Base class for data adaptors."""

    @abstractmethod
    def load_dataset(
        self, dataset_key: str, limit: Optional[int] = None
    ) -> List[ParserOutput]:
        """Load entire dataset from data source."""
        raise NotImplementedError


class S3DataAdaptor(DataAdaptor):
    """Adaptor for loading data from S3."""

    def load_dataset(
        self, dataset_key: str, limit: Optional[int] = None
    ) -> List[ParserOutput]:
        """
        Load entire dataset from S3.

        :param dataset_key: S3 bucket
        :param limit: optionally limit number of documents loaded. Defaults to None
        :return List[ParserOutput]: list of parser outputs
        """
        s3_objects = _get_s3_keys_with_prefix(f"s3://{dataset_key}/embeddings_input")

        parsed_files = []

        for filename in tqdm(s3_objects[:limit]):
            if filename.endswith(".json"):
                parsed_files.append(
                    ParserOutput.parse_raw(
                        _s3_object_read_text(f"s3://{dataset_key}/{filename}")
                    )
                )

        return parsed_files


class LocalDataAdaptor(DataAdaptor):
    """Adaptor for loading data from a local path."""

    def load_dataset(
        self, dataset_key: str, limit: Optional[int] = None
    ) -> List[ParserOutput]:
        """
        Load entire dataset from a local path.

        :param str dataset_key: path to local directory containing parser outputs/embeddings inputs
        :param limit: optionally limit number of documents loaded. Defaults to None
        :return List[ParserOutput]: list of parser outputs
        """

        folder_path = Path(dataset_key).resolve()

        if not folder_path.exists():
            raise ValueError(f"Path {folder_path} does not exist")

        if not folder_path.is_dir():
            raise ValueError(f"Path {folder_path} is not a directory")

        if len(list(folder_path.glob("*.json"))) == 0:
            raise ValueError(f"Path {folder_path} does not contain any json files")

        parsed_files = []

        for file in tqdm(list(folder_path.glob("*.json"))[:limit]):
            parsed_files.append(ParserOutput.parse_raw(file.read_text()))

        return parsed_files
