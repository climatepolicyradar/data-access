# data-access

Internal library for persistent access to text data.

> **Warning**
> This library is heavily under construction and doesn't work with any of our open data yet. We're working on making it usable for anyone.

## Usage

The base document model of this library is `BaseDocument`, which contains only the metadata fields that are used in the parser. 

``` py
# document_id is also the filename stem

document = BaseDocument.load_from_local(folder_path="path/to/data/", document_id="document_1234")

document = BaseDocument.load_from_remote(dataset_key"s3://cpr-data", document_id="document_1234")
```

To manage metadata, documents need to be loaded into a `Dataset` object.

``` py
from cpr_data_access.models import Dataset, CPRDocument, GSTDocument

dataset = Dataset().load_from_local("path/to/data", limit=1000)
assert all([isinstance(document, BaseDocument) for document in dataset])

dataset_with_metadata = dataset.add_metadata(
    target_model=CPRDocument,
    metadata_csv="path/to/metadata.csv",
)

assert all([isinstance(document, CPRDocument) for document in dataset_with_metadata])
```

Datasets have a number of methods for filtering and accessing documents.

``` py
len(dataset)
>>> 1000

dataset[0]
>>> CPRDocument(...)

# Filtering
dataset.filter("document_id", "1234")
>>> Dataset()

dataset.filter_by_language("en")
>>> Dataset()

# Filtering using a function
dataset.filter("document_id", lambda x: x in ["1234", "5678"])
>>> Dataset()
```
