import csv
from pathlib import Path


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
        term for term in sensitive_terms if term in text.lower()
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
