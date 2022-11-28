"""Adaptors for getting and storing data from CPR data sources."""

from abc import ABC, abstractmethod
from typing import List, Optional
from pathlib import Path

from tqdm.auto import tqdm

from src.cpr_data_access.s3 import _get_s3_keys_with_prefix, _s3_object_read_text
from src.cpr_data_access.parser_models import ParserOutput


class DataAdapter(ABC):
    """Base class for data adaptors."""

    @abstractmethod
    def load_dataset(
        self, bucket_name_or_folder_path: str, limit: Optional[int] = None
    ) -> List[ParserOutput]:
        """Load entire dataset from data source."""
        raise NotImplementedError


class S3DataAdapter(DataAdapter):
    """Adaptor for loading data from S3."""

    def load_dataset(
        self, bucket_name_or_folder_path: str, limit: Optional[int] = None
    ) -> List[ParserOutput]:
        """Load entire dataset from S3."""
        s3_objects = _get_s3_keys_with_prefix(
            f"s3://{bucket_name_or_folder_path}/embeddings_input"
        )

        parsed_files = []

        for filename in tqdm(s3_objects[:limit]):
            if filename.endswith(".json"):
                parsed_files.append(
                    ParserOutput.parse_raw(
                        _s3_object_read_text(
                            f"s3://{bucket_name_or_folder_path}/{filename}"
                        )
                    )
                )

        return parsed_files


class LocalDataAdapter(DataAdapter):
    """Adaptor for loading data from a local path."""

    def load_dataset(
        self, bucket_name_or_folder_path: str, limit: Optional[int] = None
    ) -> List[ParserOutput]:
        """Load entire dataset from a local path."""

        folder_path = Path(bucket_name_or_folder_path).resolve()
        parsed_files = []

        for file in tqdm(list(folder_path.glob("*.json"))[:limit]):
            parsed_files.append(ParserOutput.parse_raw(file.read_text()))

        return parsed_files
