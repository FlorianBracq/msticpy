# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""Data provider loader."""
import logging
from functools import partial
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Type, Union

import pandas as pd

from ..._version import VERSION
from ...common.pkg_config import get_config
from ...common.utility import export, valid_pyname
from ...nbwidgets.query_time import QueryTime
from .. import drivers
from ..drivers.driver_base import DriverBase, DriverProps
from .param_extractor import extract_query_params
from .query_container import QueryContainer
from .query_defns import DataEnvironment
from .query_provider_connections_mixin import QueryProviderConnectionsMixin
from .query_provider_utils_mixin import QueryProviderUtilsMixin
from .query_source import QuerySource
from .query_store import QueryStore

__version__ = VERSION
__author__ = "Ian Hellen"

_HELP_FLAGS: List[str] = ["help", "?"]
_DEBUG_FLAGS: List[str] = ["print", "debug_query", "print_query"]
_COMPATIBLE_DRIVER_MAPPINGS: Dict[str, List[str]] = {
    "mssentinel": ["m365d"],
    "mde": ["m365d"],
    "mssentinel_new": ["mssentinel", "m365d"],
    "kusto_new": ["kusto"],
}

logger: logging.Logger = logging.getLogger(__name__)


# These are mixin classes that do not have an __init__ method
# pylint: disable=super-init-not-called
@export
class QueryProvider(QueryProviderConnectionsMixin, QueryProviderUtilsMixin):
    """
    Container for query store and query execution provider.

    Instances of this class hold the query set and execution
    methods for a specific data environment.

    """

    def __init__(  # noqa: MC0001
        self,
        data_environment: Union[str, DataEnvironment],
        *,
        driver: Optional[DriverBase] = None,
        query_paths: Optional[List[str]] = None,
        **prov_args: Any,
    ) -> None:
        """
        Query provider interface to queries.

        Parameters
        ----------
        data_environment : Union[str, DataEnvironment]
            Name or Enum of environment for the QueryProvider
        driver : DriverBase, optional
            Override the builtin driver (query execution class)
            and use your own driver (must inherit from
            `DriverBase`)
        query_paths : List[str]
            Additional paths to look for query definitions.
        prov_args :
            Other arguments are passed to the data provider driver.

        Notes
        -----
        Additional keyword arguments are passed to the data provider driver.
        The driver may support additional keyword arguments, use
        the QueryProvider.driver_help() method to get a list of these
        parameters.

        See Also
        --------
        DataProviderBase : base class for data query providers.

        """
        # import at runtime to prevent circular import
        # pylint: disable=import-outside-toplevel, cyclic-import
        from ...init.pivot_init.pivot_data_queries import add_data_queries_to_entities

        # pylint: enable=import-outside-toplevel
        setattr(self.__class__, "_add_pivots", add_data_queries_to_entities)

        parsed_environment, self.environment_name = QueryProvider._check_environment(
            data_environment
        )

        self._driver_kwargs = prov_args.copy()
        if driver is None:
            if parsed_environment == DataEnvironment.Unknown:
                self.driver_class: Type[DriverBase] = drivers.import_driver(
                    self.environment_name
                )
            else:
                self.driver_class = drivers.import_driver(parsed_environment)
            if issubclass(self.driver_class, DriverBase):
                driver = self.driver_class(
                    data_environment=data_environment, **prov_args
                )
            else:
                raise LookupError(
                    "Could not find suitable data provider for", f" {data_environment}"
                )
        else:
            self.driver_class = driver.__class__
        # allow the driver to override the data environment used for selecting queries
        effective_env: str = driver.get_driver_property(DriverProps.EFFECTIVE_ENV)
        if effective_env != DataEnvironment.Unknown.name:
            self.environment_name = driver.get_driver_property(
                DriverProps.EFFECTIVE_ENV
            )
        logger.info("Using data environment %s", self.environment_name)
        logger.info("Driver class: %s", self.driver_class.__name__)

        self._additional_connections: Dict[str, DriverBase] = {}
        self._query_provider = driver
        # replace the connect method docstring with that from
        # the driver's connect method
        self.__class__.connect.__doc__ = driver.connect.__doc__
        self.all_queries = QueryContainer()

        # Add any query files
        data_env_queries: Dict[str, QueryStore] = {}
        self._query_paths: Optional[List[str]] = query_paths
        if driver.use_query_paths:
            logger.info("Using query paths %s", query_paths)
            data_env_queries.update(
                self._read_queries_from_paths(query_paths=query_paths)
            )
        self.query_store = data_env_queries.get(
            self.environment_name, QueryStore(self.environment_name)
        )
        logger.info("Adding query functions to provider")
        self._add_query_functions()
        self._query_time = QueryTime(units="day")
        logger.info("Initialization complete.")

    @classmethod
    def _check_environment(
        cls,
        data_environment: Any,
    ) -> Tuple[DataEnvironment, str]:
        """Check environment against known names."""
        if isinstance(data_environment, str):
            data_env: DataEnvironment = DataEnvironment.parse(data_environment)
            if data_env != DataEnvironment.Unknown:
                return data_env, data_env.name
            if data_environment in drivers.CUSTOM_PROVIDERS:
                return data_env, data_environment
            raise TypeError(f"Unknown data environment {data_environment}")
        if isinstance(data_environment, DataEnvironment):
            return data_environment, data_environment.name
        error_msg: str = f"Unknown data environment type {data_environment} ({type(data_environment)})"
        raise TypeError(error_msg)

    def __getattr__(self, name):
        """Return the value of the named property 'name'."""
        if "." in name:
            parent_name, child_name = name.split(".", maxsplit=1)
            parent = getattr(self, parent_name, None)
            if parent:
                return getattr(parent, child_name)
        raise AttributeError(f"{name} is not a valid attribute.")

    @property
    def environment(self) -> str:
        """Return the environment name."""
        return self.environment_name

    def connect(
        self,
        connection_str: Optional[str] = None,
        **prov_args: Any,
    ) -> None:
        """
        Connect to data source.

        Parameters
        ----------
        connection_str : str
            Connection string for the data source

        """
        logger.info("Calling connect on driver")
        self._query_provider.connect(connection_str=connection_str, **prov_args)

        # If the driver has any attributes to expose via the provider
        # add those here.
        for attr_name, attr in self._query_provider.public_attribs.items():
            setattr(self, attr_name, attr)

        refresh_query_funcs = False
        # if the driver supports dynamic filtering of queries,
        # filter the query store based on connect-time parameters
        if self._query_provider.filter_queries_on_connect:
            self.query_store.apply_query_filter(self._query_provider.query_usable)
            refresh_query_funcs = True
        # Add any built-in or dynamically retrieved queries from driver
        if self._query_provider.has_driver_queries:
            logger.info("Adding driver queries to provider")
            driver_queries = self._query_provider.driver_queries
            self._add_driver_queries(queries=driver_queries)
            refresh_query_funcs = True

        if refresh_query_funcs:
            self._add_query_functions()

        # Since we're now connected, add Pivot functions
        logger.info("Adding query pivot functions")
        self._add_pivots(lambda: self._query_time.timespan)

    @property
    def query_time(self) -> QueryTime:
        """Return the default QueryTime control for queries."""
        return self._query_time

    def _execute_query(  # pylint: disable=too-many-locals, too-many-arguments
        self,
        *args,
        query_name: str,
        query_path: str,
        split_query_by: Union[str, None] = None,
        split_by: Union[str, None] = None,
        progress: bool = False,
        retry_on_error: bool = False,
        connection_str: Union[str, None] = None,
        print: bool = False,  # pylint: disable=redefined-builtin
        debug_query: bool = False,
        print_query: bool = False,
        **kwargs,
    ) -> Union[pd.DataFrame, str, None]:
        if not self._query_provider.loaded:
            raise ValueError("Provider is not loaded.")
        debug: bool = _debug_flag(
            *args,
            print=print,
            debug_query=debug_query,
            print_query=print_query,
        )
        if not self._query_provider.connected and not _help_flag(*args) and not debug:
            raise ValueError(
                "No connection to a data source.",
                "Please call connect(connection_str) and retry.",
            )

        query_source: QuerySource = self.query_store.get_query(
            query_path=query_path,
            query_name=query_name,
        )
        if _help_flag(*args):
            query_source.help()
            return None

        params, missing = extract_query_params(query_source, *args, **kwargs)
        logger.debug("Template query: %s", query_source.query)
        logger.info("Parameters for query: %s", params)

        default_time_params: bool = self._check_for_time_params(params, missing)

        if missing:
            query_source.help()
            raise ValueError(f"No values found for these parameters: {missing}")

        split: Union[str, None] = split_by or split_query_by

        if split:
            logger.info("Split query selected - interval - %s", split)
            split_result: Union[pd.DataFrame, str, None] = self._exec_split_query(
                split_by=split,
                query_source=query_source,
                debug=debug,
                args=args,
                **params,
            )
            if split_result is not None:
                return split_result

        # if split queries could not be created, fall back to default
        query_str: str = query_source.create_query(
            formatters=self._query_provider.formatters, **params
        )
        # This looks for any of the "print query" debug args in args or kwargs
        if debug:
            return query_str

        logger.info(
            "Running query '%s...' with params: progress: %s, retry_on_error: %s",
            query_str[:40],
            progress,
            retry_on_error,
        )

        query_result: pd.DataFrame = self.exec_query(
            query_str,
            query_source=query_source,
            progress=progress,
            retry_on_error=retry_on_error,
            default_time_params=default_time_params,
            connection_str=connection_str,
            **{key: value for key, value in kwargs.items() if key not in params},
        )

        return query_result

    def _check_for_time_params(self, params, missing) -> bool:
        """Fall back on builtin query time if no time parameters were supplied."""
        defaults_added = False
        if "start" in missing:
            missing.remove("start")
            params["start"] = self._query_time.start
            defaults_added = True
        if "end" in missing:
            missing.remove("end")
            params["end"] = self._query_time.end
            defaults_added = True
        return defaults_added

    def _get_query_folder_for_env(self, root_path: str, environment: str) -> List[Path]:
        """Return query folder for current environment."""
        data_envs: List[str] = [environment]
        if environment.casefold() in _COMPATIBLE_DRIVER_MAPPINGS:
            data_envs += _COMPATIBLE_DRIVER_MAPPINGS[environment.casefold()]
        return [
            _resolve_package_path(root_path).joinpath(data_env.casefold())
            for data_env in data_envs
        ]

    def _read_queries_from_paths(self, query_paths) -> Dict[str, QueryStore]:
        """Fetch queries from YAML files in specified paths."""
        settings: Dict[str, Any] = get_config("QueryDefinitions", {})
        all_query_paths: List[Union[Path, str]] = []
        for def_qry_path in settings.get("Default"):  # type: ignore
            # only read queries from environment folder
            builtin_qry_paths: List[Path] = self._get_query_folder_for_env(
                def_qry_path, self.environment_name
            )
            all_query_paths.extend(
                str(qry_path) for qry_path in builtin_qry_paths if qry_path.is_dir()
            )

        for custom_path in settings.get("Custom", []):  # type: ignore
            custom_qry_path: Optional[str] = _resolve_path(custom_path)
            if custom_qry_path:
                all_query_paths.append(custom_qry_path)
        if query_paths:
            for param_path in query_paths:
                param_qry_path: Optional[str] = _resolve_path(param_path)
                if param_qry_path:
                    all_query_paths.append(param_qry_path)
        if all_query_paths:
            logger.info("Reading queries from %s", all_query_paths)
            return QueryStore.import_files(
                source_path=all_query_paths,
                recursive=True,
                driver_query_filter=self._query_provider.query_attach_spec,
            )
        # if no queries - just return an empty store
        return {self.environment_name: QueryStore(self.environment_name)}

    def _add_query_functions(self):
        """Add queries to the module as callable methods."""
        for qual_query_name in self.list_queries():
            query_path: List[str] = qual_query_name.split(".")
            query_name: str = query_path[-1]
            current_node = self
            for container_name in query_path[:-1]:
                container_name = valid_pyname(container_name)
                if hasattr(current_node, container_name):
                    current_node = getattr(current_node, container_name)
                else:
                    new_node = QueryContainer()
                    setattr(current_node, container_name, new_node)
                    current_node = new_node

            query_cont_name: str = ".".join(query_path[:-1])

            # Create the partial function
            query_func = partial(
                self._execute_query,
                query_path=query_cont_name,
                query_name=query_name,
            )
            query_func.__doc__ = self.query_store.get_query(
                query_path=query_cont_name, query_name=query_name
            ).create_doc_string()

            query_name = valid_pyname(query_name)
            setattr(current_node, query_name, query_func)
            setattr(self.all_queries, query_name, query_func)

    def _add_driver_queries(self, queries: Iterable[Dict[str, str]]):
        """Add driver queries to the query store."""
        for query in queries:
            self.query_store.add_query(
                name=query["name"],
                query=query["query"],
                query_paths=query.get(
                    "query_paths", query.get("query_container", "default")
                ),
                description=query["description"],
            )
        # For now, just add all of the functions again (with any connect-time acquired
        # queries) - we could be more efficient than this but unless there are 1000s of
        # queries it should not be noticeable.
        self._add_query_functions()

    @staticmethod
    def _get_query_options(
        params: Dict[str, Any],
        *,
        query_options: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        # sourcery skip: inline-immediately-returned-variable, use-or-for-fallback
        """Return any kwargs not already in params."""
        if not query_options:
            # Any kwargs left over we send to the query provider driver
            query_options = {
                key: val for key, val in kwargs.items() if key not in params
            }
        query_options["time_span"] = {
            "start": params.get("start"),
            "end": params.get("end"),
        }
        return query_options


def _resolve_package_path(config_path: str) -> Path:
    """Resolve path relative to current package."""
    return (
        Path(config_path)
        if Path(config_path).is_absolute()
        else Path(__file__).resolve().parent.parent.joinpath(config_path)
    )


def _resolve_path(config_path: str) -> Optional[str]:
    """Resolve path."""
    if not Path(config_path).is_absolute():
        config_path = str(Path(config_path).expanduser().resolve())
    if not Path(config_path).is_dir():
        print(f"Warning: Custom query definitions path {config_path} not found")
        return None
    return config_path


def _help_flag(*args) -> bool:
    """Return True if help parameter passed."""
    return any(help_flag for help_flag in _HELP_FLAGS if help_flag in args)


def _debug_flag(
    *args,
    print: bool = False,  # pylint: disable=redefined-builtin
    debug_query: bool = False,
    print_query: bool = False,
) -> bool:
    """Return True if debug/print args passed."""
    return (
        any(db_arg for db_arg in _DEBUG_FLAGS if db_arg in args)
        or print
        or debug_query
        or print_query
    )
