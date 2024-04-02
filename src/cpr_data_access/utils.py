import csv
import re
from pathlib import Path
from typing import Any, Union


def is_sensitive_query(text: str, sensitive_terms: set) -> bool:
    """
    Scans text to determine if the query should be considered sensitive

    It does this by evaluating it against a set of predefined sensitive terms.
    These, as well as the specific logic are reproduced from previous work, which
    stated that "If the query contains any sensitive terms, and the length of the
    shortest sensitive term is >=50% of the length of the query by number of words..."
    then it is considered sensitive. Further details on the original can be found here:
    https://github.com/climatepolicyradar/navigator/pull/815

    This updated version builds on the above to avoid a loophole where a string of
    sensitive terms can be incorrectly flagged as not being sensitive. This happens
    because it is the shortest sensitive term that is compared to the rest of the
    query, and the rest of the query at that point can contain other sensitive terms.

    """
    sensitive_terms_in_query = [
        term
        for term in sensitive_terms
        if re.findall(r"\b" + re.escape(term) + r"\b", text.lower())
    ]

    if sensitive_terms_in_query:
        shortest_sensitive_term = min(sensitive_terms_in_query, key=len)
        shortest_sensitive_word_count = len(shortest_sensitive_term.split(" "))

        remaining_sensitive_word_count = sum(
            [
                len(term.split())
                for term in sensitive_terms_in_query
                if term != shortest_sensitive_term
            ]
        )

        query_word_count = len(text.split())
        remaining_query_word_count = query_word_count - remaining_sensitive_word_count

        if remaining_query_word_count <= 0:
            return True

        proportion_sensitive = (
            shortest_sensitive_word_count / remaining_query_word_count
        )
        if proportion_sensitive >= 0.5:
            return True

    return False


def load_sensitive_query_terms() -> set[str]:
    """
    Return sensitive query terms from the first column of a TSV file.

    Outputs are lowercased for case-insensitive matching.

    :return [set[str]]: sensitive query terms
    """
    tsv_path = Path(__file__).parent / "resources" / "sensitive_query_terms.tsv"
    with open(tsv_path, "r") as tsv_file:
        reader = csv.DictReader(tsv_file, delimiter="\t")

        sensitive_terms = set([row["keyword"].lower().strip() for row in reader])

    return sensitive_terms


def dig(obj: Union[list, dict], *fields: Any, default: Any = None) -> Any:
    """
    An interface for retrieving data from complicated objects

    Behaviour is to return the default if the path is invalid thereby avoiding errors
    Example: `dig(nested_dict, "parent", "child", "child_items", 1)`
    """
    for field in fields:
        if isinstance(obj, list):
            if isinstance(field, int) and len(obj) > field:
                obj = obj[field]
            else:
                return default
        elif isinstance(obj, dict):
            obj = obj.get(field, default)
        elif not obj:
            return default
    return obj


def unflatten_json(data: dict) -> dict:
    """
    Unflatten a dictionary with keys that are dot-separated strings.

    I.e. metadata.data respresents {"metadata": {"data": {}}}
    """
    unflattened = {}
    for key, value in data.items():
        parts = key.split(".")
        current = unflattened
        for part in parts[:-1]:
            current = current.setdefault(part, {})
        current[parts[-1]] = value
    return unflattened


def remove_key_if_all_nested_vals_none(data: dict, key: str) -> dict:
    """
    Remove the value for a given key if it's a dict with all None values.

    E.g. {"key": {"a": None, "b": None}} -> {}
    """
    if key not in data:
        return data
    if isinstance(data[key], dict):
        if all(value is None for value in data[key].values()):
            data.pop(key)
    return data
