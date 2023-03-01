# data-access

Internal library for persistent access to text data.

> **Warning**
> This library is heavily under construction and doesn't work with any of our open data yet. We're working on making it usable for anyone.

## Usage

### Document models

Two document models are available at the moment:

* `CPRDocument` - a document from the [Climate Policy Radar tool](https://app.climatepolicyradar.org)
* `GSTDocument` - a document from the [Global Stocktake project](https://github.com/climatepolicyradar/global-stocktake)

Individual documents can be loaded from an S3 bucket or local directory.

``` py
from cpr_data_access.models import CPRDocument, GSTDocument

# Load CPR document from S3
document = CPRDocument.load_from_remote(dataset_key="cpr-data", document_id="1234")

# Load GST document from local
document = GSTDocument.load_from_local(dataset_key="~/data", document_id="1234")
```

### Datasets

Once provided with a document model, JSON-serialised documents can be loaded from an s3 bucket or a local directory using the `Dataset` class. These can then be used to view the documents or filter them.

``` py
from cpr_data_access.models import Dataset, CPRDocument, GSTDocument

# Load from remote, or 
dataset = Dataset(document_model=CPRDocument).load_from_remote(dataset_key="cpr-data", limit=1000)

# load from local
dataset = Dataset(document_model=GSTDocument).load_from_local(dataset_key="~/data")

# Using the dataset
len(dataset)
>>> 1000

dataset[0]
>>> Document(...)

# Filtering
dataset.filter("document_id", "1234")
>>> Dataset()

dataset.filter_by_language("en")
>>> Dataset()

# Filtering using a function
dataset.filter("document_id", lambda x: x in ["1234", "5678"])
>>> Dataset()
```
