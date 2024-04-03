from cpr_cpr_sdk.pipeline_general_models import BackendDocument


def test_backend_document(backend_document_json) -> None:
    """
    Test the instantiation of the backend document class from json.

    These tests validate that we can construct the date field also and use the to_json
    method.
    """
    # Test instantiation with the date field present
    BackendDocument.model_validate(backend_document_json)

    # Test instantiation without the date field present
    backend_document_json_no_date = backend_document_json.copy()
    backend_document_json_no_date["date"] = None
    BackendDocument.model_validate(backend_document_json_no_date)
