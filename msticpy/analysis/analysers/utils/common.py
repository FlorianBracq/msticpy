"""Generic functions to be ease usage of analysers."""
from typing import Any, Callable, Dict, List, Type

import numpy as np
import pandas as pd


def converter(to_convert: List, c_type: Type) -> Callable:
    """
    Convert argument to type if not already.

    To be used as a decorator.

    Parameters:
    ----------
    to_convert: List
        list of parameters names to convert
    c_type: Type
        Type to convert to

    Return
    ------
        Callable

    """
    def apply_convert(function):
        def wrapper(*args, **kwargs):
            new_kwargs = {}
            for key, value in kwargs.items():
                if key in to_convert:
                    if not isinstance(value, c_type):
                        value = c_type(value) if c_type != list else [value]
                new_kwargs[key] = value
            result = function(*args, **new_kwargs)
            return result

        return wrapper

    return apply_convert


@converter(to_convert=["columns"], c_type=list)
def get_mask(df_filter: Callable, columns: List[str]) -> pd.Series:
    """
    Create masks for dataframes to extract relevant data.

    Parameters:
    ----------
    df_filter: Callable
        Function that will be executed to filter data
    columns: List[str]
        List of columns to apply df_filter function against

    Returns:
    --------
        Boolean mask of resulting filters.

    """
    if len(columns) > 1:
        masks = [np.array(df_filter(column)) for column in columns]
        return np.logical_or.reduce(masks)
    return df_filter(columns[0])


def analyse(
    dataframe: pd.DataFrame,
    analysers: List[Dict[str, Any]],
    **kwargs,
) -> pd.DataFrame:
    """
    Analyse an object to assign a score based on different analysers.

    Parameters
    ----------
    dataframe: pd.DataFrame
        Data to analyze.
    analysers: List[Dict[str, Any]]
        List of functions to run to analyze the data and score the result.
        Expected format: [
            {
                "function": function_name,
                "score": 10,
                "name": "My analysis"
            }
        ]

    Returns
    -------
    pd.DataFrame
        List of rows that matched at least one of the analysers with 3 extra columns:
            * Score: integer to indicate analysis result
            * tags: List of keywords to know which function had hits
            * match: List of patterns that matched

    """
    reset_df = dataframe.reset_index(drop=True)
    final_df = reset_df.copy()
    final_df["score"] = 0
    final_df["tags"] = final_df.apply(lambda _: [], axis=1)
    final_df["match"] = final_df.apply(lambda _: [], axis=1)
    for analyser in analysers:
        analyser_func = analyser["function"]
        name = analyser.get("name", analyser_func.__name__)
        score = analyser.get("score", 1)
        result = analyser_func(
            reset_df,
            **kwargs,
        )
        if not result.empty:
            final_df.loc[result.index, "score"] += score
            final_df.loc[result.index, "tags"] = final_df.loc[
                result.index, "tags"
            ].apply(
                # pylint: disable=cell-var-from-loop
                lambda x: x
                + [name]
            )  # pylint: enable=cell-var-from-loop
            if "match" in result.columns:
                try:
                    final_df.loc[result.index, "match"] += result["match"]
                except TypeError:
                    final_df.loc[result.index, "match"] = (
                        pd.concat(
                            [
                                final_df.loc[result.index, "match"].explode().dropna(),
                                result.match,
                            ]
                        )
                        .reset_index()
                        .groupby("index")
                        .agg(list)
                    )
    return final_df[final_df.score > 0].sort_values(by="score", ascending=False)
