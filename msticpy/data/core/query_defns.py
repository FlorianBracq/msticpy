# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""Query helper definitions."""
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Type, TypeVar, Union

from ..._version import VERSION
from ...common.utility import export

DFAMILY = TypeVar("DFAMILY", bound="DataFamily")
DENV = TypeVar("DENV", bound="DataEnvironment")

__version__ = VERSION
__author__ = "Ian Hellen"


# pylint: disable=invalid-name
@export
class DataFamily(Enum):
    """
    Enumeration of data families.

    Used to identify which queries are relevant for which
    data sources.
    """

    Unknown = 0
    WindowsSecurity = 1
    LinuxSecurity = 2
    SecurityAlert = 3
    SecurityGraphAlert = 4
    LinuxSyslog = 5
    AzureNetwork = 6
    MDATP = 7
    Splunk = 8
    ResourceGraph = 9
    Sumologic = 10
    Cybereason = 11
    Elastic = 14

    @classmethod
    def parse(cls: Type[DFAMILY], value: Union[str, int, DFAMILY, None]) -> DFAMILY:
        """
        Convert string or int to enum.

        Parameters
        ----------
        value : Union[str, int]
            value to parse

        """
        if isinstance(value, cls):
            return value

        parsed_enum: DFAMILY = cls(0)
        if isinstance(value, str):
            try:
                parsed_enum = cls[value]
            except KeyError:
                # match to value if case is incorrect
                # pylint: disable=no-member
                return next(
                    (
                        e_val
                        for e_name, e_val in cls.__members__.items()
                        if e_name.upper() == value.upper()
                    ),
                    cls(0),
                )
                # pylint: enable=no-member
        if isinstance(value, int):
            try:
                parsed_enum = cls(value)
            except ValueError:
                parsed_enum = cls(0)
        return parsed_enum


@export
class DataEnvironment(Enum):
    """
    Enumeration of data environments.

    Used to identify which queries are relevant for which
    data sources.
    """

    Unknown = 0
    MSSentinel = 1
    AzureSentinel = 1  # alias of MSSentinel
    LogAnalytics = 1  # alias of MSSentinel
    MSSentinel_New = 1  # alias of MSSentinel
    Kusto = 2  # alias of Kusto
    AzureDataExplorer = 2  # alias of Kusto
    Kusto_New = 2  # alias of Kusto
    MSGraph = 4
    SecurityGraph = 4

    MDE = 5
    MDATP = 5  # alias of MDE
    LocalData = 6
    Splunk = 7
    OTRF = 8
    Mordor = 8
    ResourceGraph = 9
    Sumologic = 10
    M365D = 11
    Cybereason = 12
    Elastic = 14
    OSQueryLogs = 15
    OSQuery = 15
    MSSentinel_Legacy = 16
    MSSentinel_KQLM = 16
    Kusto_Legacy = 17
    Kusto_KQLM = 17
    VelociraptorLogs = 18
    Velociraptor = 18
    M365DGraph = 20

    @classmethod
    def parse(cls: Type[DENV], value: Union[str, int, DENV, None]) -> DENV:
        """
        Convert string or int to enum.

        Parameters
        ----------
        value : Union[str, int]
            value to parse

        """
        if isinstance(value, cls):
            return value

        if isinstance(value, str):
            try:
                return cls[value]
            except KeyError:
                pass
        if isinstance(value, int):
            return cls(value)
        return cls(0)


# pylint: disable=too-few-public-methods
@export
class QueryParamProvider(ABC):
    """
    Abstract type for QueryParamProvider.

    Method query_params must be overridden by derived classes.

    """

    @property
    @abstractmethod
    def query_params(self) -> Dict:
        """
        Return dict of query parameters.

        These parameters are sourced in the object
        implementing this method.

        Returns
        -------
            dict -- dictionary of query parameter values.

        """
        return {}


class Formatters:
    """Names of custom format handlers specified by driver."""

    DATETIME = "datetime"
    LIST = "list"
    PARAM_HANDLER = "custom_param_handler"
