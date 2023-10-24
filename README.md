# data-access

Internal library for persistent access to text data.

> **Warning**
> This library is heavily under construction and doesn't work with any of our open data yet. We're working on making it usable for anyone.

## Usage

### Documents and Datasets

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

### Search

This library can also be used to run searches against CPR documents and passages in Vespa.

```python
from src.cpr_data_access.search_adaptors import VespaSearchAdapter
from src.cpr_data_access.models.search import SearchRequestBody

adaptor = VespaSearchAdapter(instance_url="YOUR_INSTANCE_URL")

request = SearchRequestBody(query_string="forest fires")

response = adaptor.search(request)
```

The above example will return a `SearchResponse` object, which lists some basic information about the request, and the results, arranged as a list of Families, which each contain relevant Documents and/or Passages.

By default, results are sorted by relevance, but can be sorted by date, or name, eg

```python
request = SearchRequestBody(
    query_string="forest fires",
    sort_by="date",
    sort_order="descending",
)
```

Matching documents can also be filtered by keyword field, and by publication date

```python
request = SearchRequestBody(
    query_string="forest fires",
    keyword_filters={
        "language": ["English", "French"],
        "category": "Executive",
    },
    year_range=(2010, 2020)
)
```

Users can also fetch single documents directly from Vespa, by document ID

```python
adaptor.get_by_id(document_id="id:YOUR_NAMESPACE:YOUR_SCHEMA_NAME::SOME_DOCUMENT_ID")
```

All of the above search functionality assumes that a valid set of vespa credentials is available in `~/.vespa`, or in a directory supplied to the `VespaSearchAdapter` constructor directly.
