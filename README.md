# data-access

Internal library for persistent access to text data.

> **Warning**
> This library is heavily under construction and doesn't work with any of our open data yet. We're working on making it usable for anyone.

## Usage 

``` py
from cpr_data_access.models import Dataset

# Load from remote, or 
dataset = Dataset(cdn_domain="cdn_dev.climatepolicyradar.org").load_from_remote(dataset_key="cpr-data", limit=1000)

# load from local
dataset = Dataset().load_from_local(dataset_key="~/data")

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
```