from datetime import datetime
from enum import Enum
from typing import Mapping, Any, List, Optional, Sequence, Union

from pydantic import BaseModel, model_validator

Json = dict[str, Any]

CONTENT_TYPE_HTML = "text/html"
CONTENT_TYPE_DOCX = (
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
)
CONTENT_TYPE_PDF = "application/pdf"


class BackendDocument(BaseModel):
    """
    A representation of all information expected to be provided for a document.

    This class comprises direct information describing a document, along
    with all metadata values that should be associated with that document.
    """

    name: str
    description: str
    import_id: str
    slug: str
    family_import_id: str
    family_slug: str
    publication_ts: datetime
    date: Optional[str] = None  # Set on import by a validator
    source_url: Optional[str] = None
    download_url: Optional[str] = None

    type: str
    source: str
    category: str
    geography: str
    languages: Sequence[str]

    metadata: Json

    @model_validator(mode="after")
    def convert_publication_ts_to_date(self):
        """
        Convert publication_ts to a datetime string.

        This is necessary as OpenSearch expects a date object.

        TODO: remove when no longer using Opensearch
        """

        self.date = self.publication_ts.strftime("%d/%m/%Y")

        return self


class InputData(BaseModel):
    """Expected input data containing RDS state."""

    documents: Mapping[str, BackendDocument]


class UpdateTypes(str, Enum):
    """Document types supported by the backend API."""

    NAME = "name"
    DESCRIPTION = "description"
    # IMPORT_ID = "import_id"
    SLUG = "slug"
    # PUBLICATION_TS = "publication_ts"
    SOURCE_URL = "source_url"
    # TYPE = "type"
    # SOURCE = "source"
    # CATEGORY = "category"
    # GEOGRAPHY = "geography"
    # LANGUAGES = "languages"
    # DOCUMENT_STATUS = "document_status"
    METADATA = "metadata"
    REPARSE = "reparse"


class Update(BaseModel):
    """Results of comparing db state data against the s3 data to identify updates."""

    s3_value: Optional[Union[str, datetime, dict]] = None
    db_value: Optional[Union[str, datetime, dict]] = None
    type: UpdateTypes


class PipelineUpdates(BaseModel):
    """
    Expected input data containing document updates and new documents.

    This is utilized by the ingest stage of the pipeline.
    """

    new_documents: List[BackendDocument]
    updated_documents: dict[str, List[Update]]


class ExecutionData(BaseModel):
    """Data unique to a step functions execution that is required at later stages."""

    input_dir_path: str
