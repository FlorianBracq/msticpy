"""
MSTICPy Process Analysis Tools.

This sub-package contains process analysers.
"""

from typing import Any, Dict, List, Optional

import pandas as pd

from ...._version import VERSION
from .generic import ANALYSER_GENERIC_PROCESS
from .malwares import ANALYSER_MIMIKATZ
from ..utils.common import analyse

__version__ = VERSION

ANALYSER_PROCESS: List[Dict[str, Any]] = ANALYSER_MIMIKATZ + ANALYSER_GENERIC_PROCESS


def assess_processes(
    dataframe: pd.DataFrame,
    analysers: Optional[List[Dict[str, Any]]] = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Analyse an object to assign a score based on analyser functions.

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
        List of rows that matched at least one of the analyser with 3 extra columns:
            * Score: integer to indicate analysis result
            * tags: List of keywords to know which function had hits
            * match: List of patterns that matched
    """
    return analyse(
        dataframe,
        analysers=analysers or ANALYSER_PROCESS,
        **kwargs,
    )
