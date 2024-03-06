import random
from time import time

from cpr_data_access.models.search import SearchParameters
from cpr_data_access.vespa import build_yql
from cpr_data_access.yql_builder import YQLBuilder


def diff(left, right):
    diff = []

    for left_char_i in range(len(left)):
        left_char = left[left_char_i]
        if len(right) > left_char_i:
            old_char = right[left_char_i]
        else:
            old_char = ""

        if left_char != old_char:
            diff.append(left[left_char_i])

    return "".join(diff)


queries = 0
new_ms = []
old_ms = []

for terms in [
    "forests",
    "climate change",
    "peru",
    "france",
    "!@£$%^&*()_7896325412>?:{}|{:><>./'[]['[-0987654321§§±]]}",
]:
    for exact in [True, False]:
        limit = random.randint(1, 100)
        famhits = random.randint(1, 100)
        for sensitive in [True, False]:
            for keyword_filters in [
                {"document_languages": "value"},
                {"family_source": "value"},
            ]:
                for year_range in [(2000, 2020), (2000, None), (None, 2020)]:
                    for continuation in [None, "something"]:
                        params = SearchParameters(
                            query_string=terms,
                            exact_match=exact,
                            limit=limit,
                            max_hits_per_family=famhits,
                            keyword_filters=keyword_filters,
                            year_range=year_range,
                            continuation_token=continuation,
                        )

                        queries += 1

                        start_n = time()
                        yql_new = YQLBuilder(
                            params=params,
                            sensitive=sensitive,
                        ).to_str()
                        new_ms.append(time() - start_n)

                        start = time()
                        yql_old = build_yql(params, sensitive=sensitive)
                        old_ms.append(time() - start)

                        assert (
                            yql_new == yql_old
                        ), f"With params: \n{params} \n\nNew:\n{yql_new} \n\nOld: \n\n{yql_old}"

print(f"Ran: {queries} queries")
