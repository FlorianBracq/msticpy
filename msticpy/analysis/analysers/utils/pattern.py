"""Analysers pattern matching base functions."""
import re
from typing import List

import pandas as pd

from .common import converter, get_mask


@converter(to_convert=["columns", "keywords"], c_type=list)
def contains_keyword(
    data: pd.DataFrame,
    columns: List[str],
    keywords: List[str],
    **kwargs,
) -> pd.DataFrame:
    """
    Return Dataframe values containing one of the keywords (partial match).

    Parameters
    ----------
    data: pd.DataFrame
        Data to analyze.
    columns: List[str]
        List of columns to run the analysis against.
    keywords: List[str]
        List of keywords to search for

    Additional Parameters
    ----------
    assign_match: boolean
        Indicates if matching pattern should be stored in an extra column
    matching_flag: string
        Indicates if the pattern should match in a specific position.
        Defaults to None, meaning the match would be anywhere on the string
        Supports:
            * end
            * start
            * exact (do not use regular expression)

    Returns
    -------
    pd.DataFrame
        DataFrame rows where columns contains one of the keywords.
        If assign_match is set to True, an extra column "match" is added highlighting
        what matched.
    """
    if any(column not in data.columns for column in columns):
        raise ValueError(f"At least one of {columns} not in dataframe columns.")
    # First normalize the entries to be formatted as: (?:<pattern>)
    # to prevent issue when joining with other patterns
    escaped_keywords = None
    if "matching_flag" in kwargs:
        if kwargs["matching_flag"] == "end":
            escaped_keywords = [
                f"(?:{re.escape(keyword)}$)" for keyword in map(str, keywords)
            ]
        elif kwargs["matching_flag"] == "start":
            escaped_keywords = [
                f"(?:^{re.escape(keyword)})" for keyword in map(str, keywords)
            ]
        # This option is useful when you only want to be calling one function.
        elif kwargs["matching_flag"] == "exact":
            match_keyword(data, columns, keywords, **kwargs)
    if not escaped_keywords:
        escaped_keywords = [
            f"(?:{re.escape(keyword)})" for keyword in map(str, keywords)
        ]
    search_pattern = "|".join(escaped_keywords)

    regular_expression = re.compile(search_pattern)

    def df_filter(column: str) -> pd.Series:
        return ~data[column].isna() & (
            data[column]
            .str.casefold()
            .str.contains(regular_expression, regex=True, na=False)
        )

    reduced_mask = get_mask(df_filter, columns)

    result = data[reduced_mask]
    if kwargs.get("assign_match", False) and reduced_mask.any():
        result = result.assign(
            match=lambda x: (x[columns].agg(" ".join, axis=1))
            .str.casefold()
            .str.findall(regular_expression)
        )
    return result


@converter(to_convert=["columns", "keywords"], c_type=list)
def match_keyword(
    data: pd.DataFrame,
    columns: List[str],
    keywords: List[str],
    **kwargs,
) -> pd.DataFrame:
    """
    Return Dataframe values for containing one of the keywords (exact match).

    Parameters
    ----------
    data: pd.DataFrame
        Data to analyze.
    columns: List[str]
        List of columns to run the analysis against.
    keywords: List[str]
        List of keywords to search for

    Additional Parameters
    ----------
    assign_match: boolean
        Indicates if matching pattern should be stored in an extra column

    Returns
    -------
    pd.DataFrame
        DataFrame rows where columns contains one of the keywords
    """
    if any(column not in data.columns for column in columns):
        raise ValueError(f"At least one of {columns} not in dataframe columns.")

    def df_filter(column: str) -> pd.Series:
        return data[~data[column].isna()][column].str.casefold().isin(keywords)

    reduced_mask = get_mask(df_filter, columns)

    result = data[reduced_mask]
    if kwargs.get("assign_match", False) and reduced_mask.any():
        result = result.assign(match=lambda x: (x[columns].agg(" ".join, axis=1)))

    return result
