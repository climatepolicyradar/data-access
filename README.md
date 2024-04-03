# cpr-sdk

Internal library for persistent access to text data.

> **Warning**
> This library is heavily under construction and doesn't work with any of our open data yet. We're working on making it usable for anyone.

## Documents and Datasets

The base document model of this library is `BaseDocument`, which contains only the metadata fields that are used in the parser.

### Loading from Huggingface Hub (recommended)

The `Dataset` class is automatically configured with the Huggingface repos we use. You can optionally provide a document limit, a dataset version, and override the repo that the data is loaded from.

If the repository is private you must provide a [user access token](https://huggingface.co/docs/hub/security-tokens), either in your environment as `HUGGINGFACE_TOKEN`, or as an argument to `from_huggingface`.

```py
from cpr_sdk.models import Dataset, GSTDocument

dataset = Dataset(GSTDocument).from_huggingface(
    version="d8363af072d7e0f87ec281dd5084fb3d3f4583a9", # commit hash, optional
    limit=1000,
    token="my-huggingface-token", # required for private repos if not in env
)
```

### Loading from local storage or s3

```py
# document_id is also the filename stem

document = BaseDocument.load_from_local(folder_path="path/to/data/", document_id="document_1234")

document = BaseDocument.load_from_remote(dataset_key"s3://cpr-data", document_id="document_1234")
```

To manage metadata, documents need to be loaded into a `Dataset` object.

```py
from cpr_sdk.models import Dataset, CPRDocument, GSTDocument

dataset = Dataset().load_from_local("path/to/data", limit=1000)
assert all([isinstance(document, BaseDocument) for document in dataset])

dataset_with_metadata = dataset.add_metadata(
    target_model=CPRDocument,
    metadata_csv="path/to/metadata.csv",
)

assert all([isinstance(document, CPRDocument) for document in dataset_with_metadata])
```

Datasets have a number of methods for filtering and accessing documents.

```py
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

## Search

This library can also be used to run searches against CPR documents and passages in Vespa.

```python
from src.cpr_sdk.search_adaptors import VespaSearchAdapter
from src.cpr_sdk.models.search import SearchParameters

adaptor = VespaSearchAdapter(instance_url="YOUR_INSTANCE_URL")

request = SearchParameters(query_string="forest fires")

response = adaptor.search(request)
```

The above example will return a `SearchResponse` object, which lists some basic information about the request, and the results, arranged as a list of Families, which each contain relevant Documents and/or Passages.

### Sorting

By default, results are sorted by relevance, but can be sorted by date, or name, eg

```python
request = SearchParameters(
    query_string="forest fires",
    sort_by="date",
    sort_order="descending",
)
```

### Filters

Matching documents can also be filtered by keyword field, and by publication date

```python
request = SearchParameters(
    query_string="forest fires",
    filters={
        "language": ["English", "French"],
        "category": ["Executive"],
    },
    year_range=(2010, 2020)
)
```

### Search within families or documents

A subset of families or documents can be retrieved for search using their ids

```python
request = SearchParameters(
    query_string="forest fires",
    family_ids=["CCLW.family.10121.0", "CCLW.family.4980.0"],
)
```

```python
request = SearchParameters(
    query_string="forest fires",
    document_ids=["CCLW.executive.10121.4637", "CCLW.legislative.4980.1745"],
)
```

### Types of query

The default search approach uses a nearest neighbour search ranking.

Its also possible to search for exact matches instead:

```python
request = SearchParameters(
    query_string="forest fires",
    exact_match=True,
)
```

Or to ignore the query string and search the whole database instead:

```python
request = SearchParameters(
    year_range=(2020, 2024),
    sort_by="date",
    sort_order="descending",
)
```

### Continuing results

The response objects include continuation tokens, which can be used to get more results.

For the next selection of families:

```python
response = adaptor.search(SearchParameters(query_string="forest fires"))

follow_up_request = SearchParameters(
    query_string="forest fires"
    continuation_tokens=[response.continuation_token],

)
follow_up_response = adaptor.search(follow_up_request)
```

It is also possible to get more hits within families by using the continuation token on the family object, rather than at the responses root

Note that `this_continuation_token` is used to mark the current continuation of the families, so getting more passages for a family after getting more families would look like this:

```python
follow_up_response = adaptor.search(follow_up_request)

this_token = follow_up_response.this_continuation_token
passage_token = follow_up_response.families[0].continuation_token

follow_up_request = SearchParameters(
    query_string="forest fires"
    continuation_tokens=[this_token, passage_token],
)
```

## Get a specific document

Users can also fetch single documents directly from Vespa, by document ID

```python
adaptor.get_by_id(document_id="id:YOUR_NAMESPACE:YOUR_SCHEMA_NAME::SOME_DOCUMENT_ID")
```

All of the above search functionality assumes that a valid set of vespa credentials is available in `~/.vespa`, or in a directory supplied to the `VespaSearchAdapter` constructor directly. See [the docs](docs/vespa-auth.md) for more information on how vespa expects credentials.

# Test setup

Some tests rely on a local running instance of vespa.

This requires the [vespa cli](https://docs.vespa.ai/en/vespa-cli.html) to be installed.

Setup can then be run with:

```
poetry install --all-extras --with dev
poetry shell
make vespa_dev_setup
make test
```

Alternatively, to only run non-vespa tests:

```
make test_not_vespa
```

For clean up:

```
make vespa_dev_down
```
