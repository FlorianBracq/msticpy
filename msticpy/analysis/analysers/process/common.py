"""Process analysers common functions."""
from typing import List, Union

import pandas as pd

from ..utils.pattern import contains_keyword


def assess_process_by_process_name(
    dataframe: pd.DataFrame,
    process_names: Union[str, List[str]],
    process_name_column: str = "process_name",
    **kwargs,
):
    """
    Assess processes based on their names.

    Parameters
    ----------
    dataframe: pd.DataFrame
        Data to analyze.
    process_names: Union[str, List[str]]
        Process names to search for.
    process_name_column: str
        Column containing process names. Default to process_name.

    Returns
    -------
    pd.DataFrame
        DataFrame rows where columns contain one of the keywords

    """
    assign_match = kwargs.pop("assign_match", True)
    return contains_keyword(
        dataframe,
        columns=process_name_column,
        keywords=process_names,
        assign_match=assign_match,
        **kwargs,
    )


def assess_process_by_command_line(
    dataframe: pd.DataFrame,
    command_lines: Union[str, List[str]],
    process_command_line_column: str = "process_command_line",
    **kwargs,
):
    """
    Assess processes based on their command lines.

    Parameters
    ----------
    dataframe: pd.DataFrame
        Data to analyze.
    command_lines: List[str]
        Command lines to search for.
    process_command_line_column: str
        Column containing the process' command line. Defaults to process_command_line.

    Returns
    -------
    pd.DataFrame
        DataFrame rows where columns contain one of the keywords

    """
    assign_match = kwargs.pop("assign_match", True)
    return contains_keyword(
        dataframe,
        columns=process_command_line_column,
        keywords=command_lines,
        assign_match=assign_match,
        **kwargs,
    )


def assess_process_by_process_name_and_command_line(
    dataframe: pd.DataFrame,
    process_names: Union[str, List[str]],
    command_lines: Union[str, List[str]],
    process_name_column: str = "process_name",
    process_command_line_column: str = "process_command_line",
    **kwargs,
):
    """
    Assess processes based on their process name and command line.

    Parameters
    ----------
    dataframe: pd.DataFrame
        Data to analyze.
    process_names: Union[str, List[str]]
        name of processes for investigation
    command_lines: List[str]
        Command lines to search for
    process_name_column: str
        process column name
    process_command_line_column: str
        command line column

    Returns
    -------
    pd.DataFrame
        DataFrame rows where columns do not contain one of the keywords
    """
    assign_match = kwargs.pop("assign_match", True)
    return assess_process_by_command_line(
        assess_process_by_process_name(
            dataframe,
            process_names=process_names,
            process_name_column=process_name_column,
            assign_match=False,
        ),
        process_command_line_column=process_command_line_column,
        command_lines=command_lines,
        assign_match=assign_match,
        **kwargs,
    )
