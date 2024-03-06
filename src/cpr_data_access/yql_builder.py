from string import Template
from typing import Optional

from cpr_data_access.models.search import SearchParameters
from cpr_data_access.vespa import sanitize


class YQLBuilder:
    """Used to assemble yql queries"""

    yql_base = Template(
        """
        select * from sources family_document, document_passage
            where $WHERE_CLAUSE
        limit 0 
        |
            $CONTINUATION
        all(
            group(family_import_id)
            max($LIMIT)
            each(
                max($MAX_HITS_PER_FAMILY)
                each(output(summary(search_summary))
                )
            )
        )
    """
    )

    def __init__(self, params: SearchParameters, sensitive: bool = False) -> None:
        self.params = params
        self.sensitive = sensitive

    def build_search_term(self) -> str:
        """Create the part of the query that matches a users search text"""
        query = sanitize(self.params.query_string)
        if self.params.exact_match:
            return Template(
                """
                (
                    (family_name contains({stem: false}"$QUERY")) or
                    (family_description contains({stem: false}"$QUERY")) or
                    (text_block contains ({stem: false}"$QUERY"))
                )
            """
            ).substitute(QUERY=query)
        elif self.sensitive:
            return Template(
                """
                (
                    {"targetHits": 1000} weakAnd(
                        family_name contains "$QUERY",
                        family_description contains "$QUERY",
                        text_block contains "$QUERY"
                ))
            """
            ).substitute(QUERY=query)
        else:
            return Template(
                """
                ((
                    {"targetHits": 1000} weakAnd(
                        family_name contains "$QUERY",
                        family_description contains "$QUERY",
                        text_block contains "$QUERY"
                    )
                ) or (
                    [{"targetNumHits": 1000}]
                    nearestNeighbor(family_description_embedding,query_embedding)
                ) or (
                    [{"targetNumHits": 1000}]
                    nearestNeighbor(text_embedding,query_embedding)
                ))
            """
            ).substitute(QUERY=query)

    def build_keyword_filter(self) -> Optional[str]:
        """Create the part of the query that adds keyword filters"""
        keyword_filters = self.params.keyword_filters
        if keyword_filters:
            filters = []
            for field_name, values in keyword_filters.items():
                for value in values:
                    filters.append(f'({field_name} contains "{sanitize(value)}")')
            if filters:
                return f"({' or '.join(filters)})"
        return None

    def build_year_start_filter(self) -> Optional[str]:
        """Create the part of the query that filters on a year range"""
        if self.params.year_range:
            start, _ = self.params.year_range
            if start:
                return f"(family_publication_year >= {start})"
        return None

    def build_year_end_filter(self) -> Optional[str]:
        """Create the part of the query that filters on a year range"""
        if self.params.year_range:
            _, end = self.params.year_range
            if end:
                return f"(family_publication_year <= {end})"
        return None

    def build_where_clause(self) -> str:
        """Create the part of the query that adds filters"""
        filters = []
        filters.append(self.build_search_term())
        filters.append(self.build_keyword_filter())
        filters.append(self.build_year_start_filter())
        filters.append(self.build_year_end_filter())
        return " and ".join([f for f in filters if f])  # Remove empty

    def build_continuation(self) -> str:
        """Create the part of the query that adds a continuation token"""
        continuation = self.params.continuation_token
        if continuation:
            return f"{{ 'continuations':['{continuation}'] }}"
        else:
            return ""

    def build_limit(self) -> int:
        """Create the part of the query limiting the number of families returned"""
        return self.params.limit

    def build_max_hits_per_family(self) -> int:
        """Create the part of the query limiting passages within a family returned"""
        return self.params.max_hits_per_family

    def to_str(self) -> str:
        """Assemble the yql from parts using the template"""
        yql = self.yql_base.substitute(
            WHERE_CLAUSE=self.build_where_clause(),
            CONTINUATION=self.build_continuation(),
            LIMIT=self.build_limit(),
            MAX_HITS_PER_FAMILY=self.build_max_hits_per_family(),
        )
        return " ".join(yql.split())


if __name__ == "__main__":
    # YQL example
    params = SearchParameters(
        query_string="climate",
        exact_match=False,
        limit=10,
        max_hits_per_family=10,
        keyword_filters={"document_languages": "value", "family_source": "value"},
        year_range=(2000, 2020),
        continuation_token=None,
    )

    yql_new = YQLBuilder(
        params=params,
        sensitive=False,
    ).to_str()
