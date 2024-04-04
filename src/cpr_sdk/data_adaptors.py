"""Adaptors for getting and storing data from CPR data sources."""

import logging
from abc import ABC, abstractmethod
from typing import List, Optional
from pathlib import Path
from tqdm.auto import tqdm

from cpr_sdk.parser_models import BaseParserOutput
from cpr_sdk.s3 import _get_s3_keys_with_prefix, _s3_object_read_text

_LOGGER = logging.getLogger(__name__)


class DataAdaptor(ABC):
    """Base class for data adaptors."""

    @abstractmethod
    def load_dataset(
        self, dataset_key: str, limit: Optional[int] = None
    ) -> List[BaseParserOutput]:
        """Load entire dataset from data source."""
        raise NotImplementedError

    @abstractmethod
    def get_by_id(
        self, dataset_key: str, document_id: str
    ) -> Optional[BaseParserOutput]:
        """Get a document by its id."""
        raise NotImplementedError


class S3DataAdaptor(DataAdaptor):
    """Adaptor for loading data from S3."""

    def load_dataset(
        self, dataset_key: str, limit: Optional[int] = None
    ) -> List[BaseParserOutput]:
        """
        Load entire dataset from S3.

        :param dataset_key: path to S3 directory. Should start with 's3://'
        :param limit: optionally limit number of documents loaded. Defaults to None
        :return List[BaseParserOutput]: list of parser outputs
        """
        if not dataset_key.startswith("s3://"):
            _LOGGER.warning(
                f"Dataset key {dataset_key} does not start with 's3://'. "
                "Assuming it is an S3 bucket."
            )
            dataset_key = f"s3://{dataset_key}"

        if dataset_key.endswith("/"):
            dataset_key = dataset_key[:-1]

        s3_objects = _get_s3_keys_with_prefix(dataset_key)

        if len(s3_objects) == 0:
            raise ValueError(f"No objects found at {dataset_key}.")

        parsed_files = []

        for filename in tqdm(s3_objects[:limit]):
            if filename.endswith(".json"):
                parsed_files.append(
                    BaseParserOutput.model_validate_json(
                        _s3_object_read_text(f"{dataset_key}/{filename.split('/')[-1]}")
                    )
                )

        return parsed_files

    def get_by_id(
        self, dataset_key: str, document_id: str
    ) -> Optional[BaseParserOutput]:
        """
        Get a document by its id.

        :param str dataset_key: S3 bucket
        :param str document_id: import ID
        :return Optional[BaseParserOutput]: None if no document was found with the ID
        """

        try:
            return BaseParserOutput.model_validate_json(
                _s3_object_read_text(f"s3://{dataset_key}/{document_id}.json")
            )
        except ValueError as e:
            if "does not exist" in str(e):
                return None
            else:
                raise e
        except Exception as e:
            raise e


class LocalDataAdaptor(DataAdaptor):
    """Adaptor for loading data from a local path."""

    def load_dataset(
        self, dataset_key: str, limit: Optional[int] = None
    ) -> List[BaseParserOutput]:
        """
        Load entire dataset from a local path.

        :param str dataset_key: path to local directory containing parser outputs/embeddings inputs
        :param limit: optionally limit number of documents loaded. Defaults to None
        :return List[BaseParserOutput]: list of parser outputs
        """

        folder_path = Path(dataset_key).resolve()

        if not folder_path.exists():
            raise ValueError(f"Path {folder_path} does not exist")

        if not folder_path.is_dir():
            raise ValueError(f"Path {folder_path} is not a directory")

        if len(list(folder_path.glob("*.json"))) == 0:
            raise ValueError(f"Path {folder_path} does not contain any json files")

        parsed_files = []

        files = list(folder_path.glob("*.json"))[:limit]
        num_batches = len(files) // 1000 + 1

        for batch_idx in range(num_batches):
            parsed_batch_files = self._load_files(
                files[batch_idx * 1000 : (batch_idx + 1) * 1000], batch_idx, num_batches
            )
            parsed_files += parsed_batch_files

        return parsed_files

    @staticmethod
    def _load_files(file_paths: list[Path], batch_idx: int, num_batches: int):
        """Loads the files within a batch with paths provided in file_paths."""
        parsed_files = []

        raw_files = (file.read_text() for file in file_paths)
        for raw_file_text in tqdm(
            raw_files,
            desc=f"Loading files from directory in batch {batch_idx + 1}/{num_batches}",
        ):
            parsed_files.append(BaseParserOutput.model_validate_json(raw_file_text))

        return parsed_files

    def get_by_id(
        self, dataset_key: str, document_id: str
    ) -> Optional[BaseParserOutput]:
        """
        Get a document by its id.

        :param str dataset_key: path to local directory containing parser outputs/embeddings inputs
        :param str document_id: import ID
        :return Optional[BaseParserOutput]: None if no document was found with the ID
        """
        # TODO: these "get_by_id"  methods are almost certainly not what we need
        # because we're unable to load more complex subclasses of BaseParserOutput

        folder_path = Path(dataset_key).resolve()

        if not folder_path.exists():
            raise ValueError(f"Path {folder_path} does not exist")

        file_path = folder_path / f"{document_id}.json"

        if not file_path.exists():
            return None

        return BaseParserOutput.model_validate_json(file_path.read_text())
