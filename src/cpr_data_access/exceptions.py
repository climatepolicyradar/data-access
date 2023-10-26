class DataAccessError(Exception):
    """Base exception for errors in the data access layer."""


class QueryError(DataAccessError):
    """Raised when a query can't be built"""

    def __init__(self, message):
        super().__init__(f"Failed to build query: {message}")


class FetchError(DataAccessError):
    """Raised when the search engine fails to fetch results"""

    def __init__(self, message, status_code=None):
        self.status_code = status_code
        super().__init__(f"Something went wrong when fetching results: {message}")


class DocumentNotFoundError(DataAccessError):
    """Raised when a document can't be found"""

    def __init__(self, document_id):
        self.document_id = document_id
        super().__init__(f"Failed to find document with id: {document_id}")
