# data-access

Internal library for persistent access to text data.

## Usage 

``` py
from src.cpr_data_access.models import Dataset

# Load from remote, or 
dataset = Dataset.load_from_remote(bucket_name="cpr-data", limit=1000)

# load from local
dataset = Dataset.load_from_local(folder_path="~/data")

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