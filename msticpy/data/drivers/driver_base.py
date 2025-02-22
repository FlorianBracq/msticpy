# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""Data driver base class."""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from typing import TYPE_CHECKING, Any, ClassVar, Iterable

from typing_extensions import Self

from ..._version import VERSION
from ...common.exceptions import MsticpyNotConnectedError
from ...common.pkg_config import get_http_timeout
from ...common.provider_settings import ProviderSettings, get_provider_settings
from ..core.query_defns import DataEnvironment

if TYPE_CHECKING:
    import pandas as pd
    from httpx import Timeout

    from ..core.query_source import QuerySource

__version__ = VERSION
__author__ = "Ian Hellen"


class DriverProps:
    """Defined driver properties."""

    PUBLIC_ATTRS = "public_attribs"
    FORMATTERS = "formatters"
    USE_QUERY_PATHS = "use_query_paths"
    HAS_DRIVER_QUERIES = "has_driver_queries"
    EFFECTIVE_ENV = "effective_environment"
    SUPPORTS_THREADING = "supports_threading"
    SUPPORTS_ASYNC = "supports_async"
    MAX_PARALLEL = "max_parallel"
    FILTER_ON_CONNECT = "filter_queries_on_connect"

    PROPERTY_TYPES: ClassVar[dict[str, Any]] = {
        PUBLIC_ATTRS: dict,
        FORMATTERS: dict,
        USE_QUERY_PATHS: bool,
        HAS_DRIVER_QUERIES: bool,
        EFFECTIVE_ENV: (str, DataEnvironment),
        SUPPORTS_THREADING: bool,
        SUPPORTS_ASYNC: bool,
        MAX_PARALLEL: int,
        FILTER_ON_CONNECT: bool,
    }

    @classmethod
    def defaults(cls: type[Self]) -> dict[str, Any]:
        """Return default values for driver properties."""
        return {
            cls.PUBLIC_ATTRS: {},
            cls.FORMATTERS: {},
            cls.USE_QUERY_PATHS: True,
            cls.HAS_DRIVER_QUERIES: False,
            cls.EFFECTIVE_ENV: None,
            cls.SUPPORTS_THREADING: False,
            cls.SUPPORTS_ASYNC: False,
            cls.MAX_PARALLEL: 4,
            cls.FILTER_ON_CONNECT: False,
        }

    @classmethod
    def valid_type(cls: type[Self], property_name: str, value: Any) -> bool:
        """Return expected property type."""
        if property_name not in cls.PROPERTY_TYPES:
            return True
        return isinstance(value, cls.PROPERTY_TYPES[property_name])


# pylint: disable=too-many-instance-attributes
class DriverBase(ABC):
    """Base class for data providers."""

    def __init__(self: DriverBase, **kwargs) -> None:
        """Initialize new instance."""
        self._kwargs: dict[str, Any] = kwargs
        self._loaded: bool = False
        self._connected: bool = False
        self.current_connection: str | None = None
        self._previous_connection: bool = False
        self.data_environment: str | DataEnvironment | None = kwargs.get(
            "data_environment",
        )
        self._query_filter: dict[str, set[str]] = defaultdict(set)
        self._instance: str | None = None

        self.properties: dict[str, Any] = DriverProps.defaults()
        self.set_driver_property(
            name=DriverProps.EFFECTIVE_ENV,
            value=(
                self.data_environment.name
                if isinstance(self.data_environment, DataEnvironment)
                else self.data_environment or ""
            ),
        )
        self.set_driver_property(DriverProps.SUPPORTS_THREADING, value=False)
        self.set_driver_property(DriverProps.MAX_PARALLEL, kwargs.get("max_threads", 4))

    def __getattr__(self: Self, attrib: Any) -> Any:
        """Return item from the properties dictionary as an attribute."""
        if attrib in self.properties:
            return self.properties[attrib]
        err_msg: str = f"{self.__class__.__name__} has no attribute '{attrib}'"
        raise AttributeError(err_msg)

    @property
    def loaded(self: Self) -> bool:
        """
        Return true if the provider is loaded.

        Returns
        -------
        bool
            True if the provider is loaded.

        Notes
        -----
        This is not relevant for some providers.

        """
        return self._loaded

    @property
    def connected(self: Self) -> bool:
        """
        Return true if at least one connection has been made.

        Returns
        -------
        bool
            True if a successful connection has been made.

        Notes
        -----
        This does not guarantee that the last data source
        connection was successful. It is a best effort to track
        whether the provider has made at least one successful
        authentication.

        """
        return self._connected

    @property
    def instance(self: Self) -> str | None:
        """
        Return instance name, if one is set.

        Returns
        -------
        str | None
            The name of driver instance or None if
            the driver does not support multiple instances

        """
        return self._instance

    @property
    def schema(self: Self) -> dict[str, dict[str, Any]]:
        """
        Return current data schema of connection.

        Returns
        -------
        dict[str, Dict]
            Data schema of current connection.

        """
        return {}

    @abstractmethod
    def connect(self: Self, connection_str: str | None = None, **kwargs) -> None:
        """
        Connect to data source.

        Parameters
        ----------
        connection_str : str | None
            Connect to a data source

        """

    @abstractmethod
    def query(
        self: Self,
        query: str,
        query_source: QuerySource | None = None,
        **kwargs,
    ) -> pd.DataFrame | str | None:
        """
        Execute query string and return DataFrame of results.

        Parameters
        ----------
        query : str
            The query to execute
        query_source : QuerySource
            The query definition object

        Other Parameters
        ----------------
        kwargs :
            Are passed to the underlying provider query method,
            if supported.

        Returns
        -------
        Union[pd.DataFrame, Any]
            A DataFrame (if successful) or
            the underlying provider result if an error.

        """

    @abstractmethod
    def query_with_results(
        self: Self,
        query: str,
        **kwargs,
    ) -> tuple[pd.DataFrame, Any]:
        """
        Execute query string and return DataFrame plus native results.

        Parameters
        ----------
        query : str
            The query to execute

        Returns
        -------
        tuple[pd.DataFrame,Any]
            A DataFrame and native results.

        """

    @property
    def service_queries(self: Self) -> tuple[dict[str, str], str]:
        """
        Return queries retrieved from the service after connecting.

        Returns
        -------
        tuple[dict[str, str], str]
            Dictionary of query_name, query_text.
            Name of container to add queries to.

        """
        return {}, ""

    @property
    def driver_queries(self: Self) -> Iterable[dict[str, Any]]:
        """
        Return queries retrieved from the service after connecting.

        Returns
        -------
        List[dict[str, str]]
            List of Dictionary of query_name, query_text.
            Name of container to add queries to.

        """
        return [{}]

    @property
    def query_attach_spec(self: Self) -> dict[str, set[str]]:
        """Parameters that determine whether a query is relevant for the driver."""
        return self._query_filter

    def add_query_filter(self: Self, name: str, query_filter: str | Iterable) -> None:
        """Add an expression to the query attach filter."""
        allowed_names: set[str] = {"data_environments", "data_families", "data_sources"}
        if name not in allowed_names:
            err_msg: str = f"'name' {name} must be one of: " + ", ".join(
                f"'{name}'" for name in allowed_names
            )
            raise ValueError(err_msg)
        if isinstance(query_filter, str):
            self._query_filter[name].add(query_filter)
        else:
            self._query_filter[name].update(query_filter)

    def set_driver_property(self: Self, name: str, value: Any) -> None:
        """Set an item in driver properties."""
        if not DriverProps.valid_type(name, value):
            err_msg = (
                f"Property '{name}' is not the correct type. "
                f"Expected: '{DriverProps.PROPERTY_TYPES[name]}'."
            )
            raise TypeError(err_msg)
        self.properties[name] = value

    def get_driver_property(self: Self, name: str) -> Any:
        """Return value or KeyError from driver properties."""
        return self.properties[name]

    def query_usable(self: Self, query_source: QuerySource) -> bool:
        """Return True if query should be exposed for this driver."""
        del query_source
        return True

    # Read values from configuration
    @staticmethod
    def _get_config_settings(prov_name: str) -> dict[str, Any]:
        """Get config from msticpyconfig."""
        data_provs: dict[str, ProviderSettings] = get_provider_settings(
            config_section="DataProviders",
        )
        splunk_settings: ProviderSettings | None = data_provs.get(prov_name)
        return getattr(splunk_settings, "args", {})

    @staticmethod
    def _create_not_connected_err(prov_name: str) -> MsticpyNotConnectedError:
        return MsticpyNotConnectedError(
            "Please run the connect() method before running this method.",
            title=f"not connected to {prov_name}.",
            help_uri="https://msticpy.readthedocs.io/en/latest/DataProviders.html",
        )

    @staticmethod
    def get_http_timeout(**kwargs) -> Timeout:
        """Get http timeout from settings or kwargs."""
        return get_http_timeout(**kwargs)
