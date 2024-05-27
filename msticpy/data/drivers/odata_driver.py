# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""OData Driver class."""
import abc
import re
import urllib.parse
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import httpx
import pandas as pd

from ..._version import VERSION
from ...auth.msal_auth import MSALDelegatedAuth
from ...common.exceptions import MsticpyConnectionError, MsticpyUserConfigError
from ...common.pkg_config import get_config
from ...common.provider_settings import ProviderSettings, get_provider_settings
from ...common.utility import mp_ua_header
from ..core.query_defns import DataEnvironment
from ..core.query_source import QuerySource
from .driver_base import DriverBase, DriverProps

__version__ = VERSION
__author__ = "Pete Bryan"

_HELP_URI = (
    "https://msticpy.readthedocs.io/en/latest/data_acquisition"
    "/DataProviders.html#connecting-to-an-odata-source"
)

# pylint: disable=too-many-instance-attributes


class OData(DriverBase):
    """Parent class to retrieve date from an oauth based API."""

    CONFIG_NAME: str = ""
    _ALT_CONFIG_NAMES: Iterable[str] = []

    def __init__(
        self,
        data_environment: Optional[Union[str, DataEnvironment]] = None,
        *,
        max_threads: int = 4,
        debug: bool = False,
    ) -> None:
        """
        Instantiate OData driver and optionally connect.

        Parameters
        ----------
        connect: bool, optional
            Set true if you want to connect to the provider at initialization

        """
        super().__init__(data_environment=data_environment, max_threads=max_threads)
        self.oauth_url: Optional[str] = None
        self.req_body: Optional[Dict[str, Optional[str]]] = None
        self.api_ver: Optional[str] = None
        self.api_root: Optional[str] = None
        self.request_uri: Optional[str] = None
        self.req_headers: Dict[str, Any] = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": None,
            **mp_ua_header(),
        }
        self._loaded = True
        self.aad_token = None
        self._debug: bool = debug
        self.token_type = "AAD"  # nosec
        self.scopes: Optional[List[str]] = None
        self.msal_auth: Optional[MSALDelegatedAuth] = None

        self.set_driver_property(DriverProps.SUPPORTS_THREADING, value=True)
        self.set_driver_property(DriverProps.MAX_PARALLEL, value=max_threads)

    @abc.abstractmethod
    def query(
        self,
        query: str,
        *,
        query_source: Optional[QuerySource] = None,
    ) -> pd.DataFrame:
        """
        Execute query string and return DataFrame of results.

        Parameters
        ----------
        query : str
            The query to execute
        query_source : QuerySource
            The query definition object

        Returns
        -------
        Union[pd.DataFrame, Any]
            A DataFrame (if successful) or
            the underlying provider result if an error.

        """

    def connect(
        self,
        connection_str: Optional[str] = None,
        *,
        delegated_auth: bool = False,
        instance: Optional[str] = None,
        timeout: Optional[int] = None,
        client_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        client_secret: Optional[str] = None,
    ) -> None:
        """
        Connect to oauth data source.

        Parameters
        ----------
        connection_str: Optional[str], optional
            Connect to a data source
        instance : Optional[str], optional
            Optional name of configuration instance - this
            is added as a prefix to the driver configuration key name
            when searching for configuration in the msticpyconfig.yaml

        Notes
        -----
        Connection string fields:
        tenant_id
        client_id
        client_secret
        apiRoot
        apiVersion

        """
        cs_dict: Dict[str, Any] = {}
        if connection_str:
            self.current_connection = connection_str
            cs_dict = self._parse_connection_str(connection_str)
        else:
            cs_dict = _get_driver_settings(
                self.CONFIG_NAME, self._ALT_CONFIG_NAMES, instance
            )

        missing_settings: List[str] = [
            setting for setting in ("tenant_id", "client_id") if setting not in cs_dict
        ]
        if missing_settings:
            raise MsticpyUserConfigError(
                "You must supply the following required connection parameter(s)",
                "to the connect function or add them to your msticpyconfig.yaml.",
                ", ".join(f"'{param}'" for param in missing_settings),
                title="Missing connection parameters.",
                help_uri=("Connecting to OData sources.", _HELP_URI),
            )
        client_id = client_id or cs_dict.get("client_id")
        tenant_id = tenant_id or cs_dict.get("tenant_id", "")
        client_secret = client_secret or cs_dict.get("client_secret")
        if not (client_id and tenant_id) or "username" in cs_dict:
            raise MsticpyUserConfigError(
                "You must supply either a client_secret, or username with which to",
                "to the connect function or add them to your msticpyconfig.yaml.",
                title="Missing connection parameters.",
                help_uri=("Connecting to OData sources.", _HELP_URI),
            )

        # Default to using application based authentication
        if not delegated_auth:
            json_response: Dict[str, Any] = self._get_token_standard_auth(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
                timeout=timeout,
            )
        else:
            json_response = self._get_token_delegate_auth(
                tenant_id=tenant_id,
                client_id=client_id,
                username=cs_dict.get("username"),
            )

        self.req_headers["Authorization"] = f"Bearer {self.aad_token}"
        self.api_root = cs_dict.get("apiRoot", self.api_root)
        if not self.api_root:
            raise ValueError(
                f"Sub class {self.__class__.__name__}", "did not set self.api_root"
            )
        api_ver: Optional[str] = cs_dict.get("apiVersion", self.api_ver)
        self.request_uri = self.api_root + str(api_ver)

        print("Connected.")
        self._connected = True

        json_response["access_token"] = None

    def _get_token_standard_auth(
        self,
        *,
        tenant_id: str,
        client_id: str,
        client_secret: Optional[str] = None,
        timeout: Optional[int] = None,
    ) -> Dict[str, Any]:
        _check_config(client_secret, "application authentication")
        # self.oauth_url and self.req_body are correctly set in concrete
        # instances __init__
        if not self.oauth_url:
            error_msg: str = "OAuth URL cannot be none"
            raise ValueError(error_msg)
        if not self.req_body:
            error_msg = "req_body cannot be none"
            raise ValueError(error_msg)
        req_url: str = self.oauth_url.format(tenantId=tenant_id)
        req_body = dict(self.req_body)
        req_body["client_id"] = client_id
        req_body["client_secret"] = client_secret

        # Authenticate and obtain AAD Token for future calls
        data: bytes = urllib.parse.urlencode(req_body).encode("utf-8")
        response: httpx.Response = httpx.post(
            url=req_url,
            content=data,
            timeout=self.get_http_timeout(timeout=timeout),
            headers=mp_ua_header(),
        )
        json_response: Dict[str, Any] = response.json()
        self.aad_token = json_response.get("access_token", None)
        if not self.aad_token:
            raise MsticpyConnectionError(
                f"Could not obtain access token - {json_response['error_description']}"
            )
        return json_response

    def _get_token_delegate_auth(
        self,
        *,
        client_id: str,
        tenant_id: str,
        location: str = "token_cache.bin",
        auth_type: str = "device",
        username: Optional[str] = None,
    ) -> Dict[str, Any]:
        _check_config(username, "delegated authentication")
        if not self.oauth_url:
            error_msg: str = "OAuth URL cannot be none"
            raise ValueError(error_msg)
        if not username:
            error_msg = "username cannot be none"
            raise ValueError(error_msg)
        if not self.scopes:
            error_msg = "scopes cannot be none"
            raise ValueError(error_msg)
        authority: str = self.oauth_url.format(tenantId=tenant_id)
        if authority.startswith("https://login"):
            auth_url: urllib.parse.ParseResult = urllib.parse.urlparse(authority)
            authority = f"{auth_url.scheme}://{auth_url.netloc}/{{tenantId}}".format(
                tenantId=tenant_id
            )
        self.msal_auth = MSALDelegatedAuth(
            client_id=client_id,
            authority=authority,
            username=username,
            scopes=self.scopes,
            auth_type=auth_type,
            location=location,
            connect=True,
        )
        self.aad_token = self.msal_auth.token
        self.token_type = "MSAL"  # nosec
        return {}

    # pylint: disable=too-many-branches
    def query_with_results(
        self,
        query: str,
        *,
        api_end: Optional[str] = None,
        body: bool = True,
        timeout: Optional[int] = None,
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Execute query string and return DataFrame of results.

        Parameters
        ----------
        query : str
            The kql query to execute

        Returns
        -------
        Tuple[pd.DataFrame, results.ResultSet]
            A DataFrame (if successful) and
            Kql ResultSet.

        """
        if not self.connected:
            self.connect(self.current_connection)
        if not self.connected:
            raise ConnectionError(
                "Source is not connected. ", "Please call connect() and retry."
            )

        if self._debug:
            print(query)

        # Build request based on whether endpoint requires data to be passed in
        # request body in or URL
        if body:
            req_url: str = f"{self.request_uri}{api_end}"
            req_url = urllib.parse.quote(req_url, safe="%/:=&?~#+!$,;'@()*[]")
            response: httpx.Response = httpx.post(
                url=req_url,
                headers=self.req_headers,
                json={"Query": query},
                timeout=self.get_http_timeout(timeout=timeout),
            )
        else:
            # self.request_uri set if self.connected
            req_url = f"{self.request_uri}{query}"
            response = httpx.get(
                url=req_url,
                headers=self.req_headers,
                timeout=self.get_http_timeout(timeout=timeout),
            )

        self._check_response_errors(response)

        json_response: Dict[str, Any] = response.json()
        if isinstance(json_response, int):
            print(
                "Warning - query did not complete successfully.",
                "Check returned response.",
            )
            return pd.DataFrame(), json_response

        results_key: str = "Results" if "Results" in json_response else "results"
        result: List[Dict] = json_response.get(results_key, json_response)

        if not result:
            print("Warning - query did not return any results.")
            return pd.DataFrame(), json_response
        return pd.json_normalize(result), json_response

    # pylint: enable=too-many-branches

    @staticmethod
    def _check_response_errors(response) -> None:
        """Check the response for possible errors."""
        if response.status_code == httpx.codes.OK:
            return
        print(response.json()["error"]["message"])
        if response.status_code == 401:
            raise ConnectionRefusedError(
                "Authentication failed - possible timeout. Please re-connect."
            )
        # Raise an exception to handle hitting API limits
        if response.status_code == 429:
            raise ConnectionRefusedError("You have likely hit the API limit. ")
        response.raise_for_status()

    @staticmethod
    def _parse_connection_str(connection_str: str) -> Dict[str, str]:
        """
        Split connection string components into dictionary.

        Parameters
        ----------
        connection_str : str
            Semi-colon delimited connection string

        Returns
        -------
        Dict[str, str]
            dict of key/pair values

        """
        cs_items: List[str] = connection_str.split(";")
        return {
            prop[0]: prop[1]
            for prop in [item.strip().split("=") for item in cs_items]
            if prop[0] and prop[1]
        }

    @staticmethod
    def _prepare_param_dict_from_filter(filterstr: str) -> Dict[str, str]:
        """
        Parse filter string into dictionary.

        Parameters
        ----------
        filterstr : str
            OData filter string

        """
        get_params: Dict[str, Any] = {}
        for filter_param in re.split(r"[\?\&]+", filterstr):
            if filter_param:
                attr: str = filter_param.split("=")[0]
                val: str = filter_param.split("=")[1]
                get_params[attr] = val
        return get_params


_CONFIG_NAME_MAP: Dict[str, Tuple[str, str]] = {
    "tenant_id": ("tenantid", "tenant_id"),
    "client_id": ("clientid", "client_id"),
    "client_secret": ("clientsecret", "client_secret"),
    "username": ("username", "user_name"),
}


def _map_config_dict_name(config_dict: Dict[str, str]) -> Dict[str, str]:
    """Map configuration parameter names to expected values."""
    mapped_dict: Dict[str, str] = config_dict.copy()
    for provided_name in config_dict:
        for req_name, alternates in _CONFIG_NAME_MAP.items():
            if provided_name.casefold() in alternates:
                mapped_dict[req_name] = config_dict[provided_name]
                break
    return mapped_dict


def _get_driver_settings(
    config_name: str,
    alt_names: Iterable[str],
    instance: Optional[str] = None,
) -> Dict[str, str]:
    """Try to retrieve config settings for OAuth drivers."""
    config_key: str = f"{config_name}-{instance}" if instance else config_name
    drv_config: Optional[ProviderSettings] = get_provider_settings("DataProviders").get(
        config_key
    )

    app_config: Dict[str, str] = {}
    if drv_config:
        app_config = dict(drv_config.args)
    else:
        # Otherwise fall back on legacy settings location
        for alt_name in alt_names:
            alt_key: str = f"{alt_name}-{instance}" if instance else alt_name
            app_config = get_config(f"{alt_key}.Args", {})
            if app_config:
                break

    if not app_config:
        return {}
    # map names to allow for different spellings
    return _map_config_dict_name(app_config)


def _check_config(item_name: Optional[str], scope: str) -> None:
    """Check if an iteam is present in a config."""
    if not item_name:
        raise MsticpyUserConfigError(
            f"To use {scope}, you must define {item_name}",
            "or add them to your msticpyconfig.yaml.",
            title="Missing connection parameters.",
            help_uri=("Connecting to OData sources.", _HELP_URI),
        )
