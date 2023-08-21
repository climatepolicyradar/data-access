from datetime import datetime
from enum import Enum
from typing import Mapping, Any, List, Optional, Sequence, Union

from pydantic import BaseModel, Extra, root_validator

Json = dict[str, Any]

CONTENT_TYPE_HTML = "text/html"
CONTENT_TYPE_DOCX = (
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
)
CONTENT_TYPE_PDF = "application/pdf"


class DocumentMetadata(BaseModel, extra=Extra.allow):
    """Metadata about a document."""

    metadata: Optional[Json] = {}
    publication_ts: datetime
    date: Optional[str] = None  # Set on import by a validator
    geography: str
    category: str
    source: str
    type: str

    @root_validator
    def convert_publication_ts_to_date(cls, values):
        """
        Convert publication_ts to a datetime string.

        This is necessary as OpenSearch expects a date object.
        """

        values["date"] = values["publication_ts"].strftime("%d/%m/%Y")

        return values


class BackendDocument(BaseModel):
    """
    A representation of all information expected to be provided for a document.

    This class comprises direct information describing a document, along
    with all metadata values that should be associated with that document.
    """

    name: str
    description: str
    import_id: str
    family_import_id: str
    slug: str
    publication_ts: datetime
    source_url: Optional[str]
    download_url: Optional[str]

    type: str
    source: str
    category: str
    geography: str
    languages: Sequence[str]

    metadata: Json

    def to_json(self) -> Mapping[str, Any]:
        """Output a JSON serialising friendly dict representing this model."""
        json_dict = self.dict()
        json_dict["publication_ts"] = self.publication_ts.isoformat()
        return json_dict


class InputData(BaseModel):
    """Expected input data containing RDS state."""

    documents: Mapping[str, BackendDocument]


class UpdateTypes(str, Enum):
    """Document types supported by the backend API."""

    NAME = "name"
    DESCRIPTION = "description"
    # IMPORT_ID = "import_id"
    # SLUG = "slug"
    PUBLICATION_TS = "publication_ts"
    SOURCE_URL = "source_url"
    # TYPE = "type"
    # SOURCE = "source"
    # CATEGORY = "category"
    # GEOGRAPHY = "geography"
    # LANGUAGES = "languages"
    # DOCUMENT_STATUS = "document_status"
    # METADATA = "metadata"


class Update(BaseModel):
    """Results of comparing db state data against the s3 data to identify updates."""

    s3_value: Optional[Union[str, datetime]]
    db_value: Union[str, datetime]
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
