# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""MDATP OData Driver class."""
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd

from ..._version import VERSION
from ...auth.azure_auth_core import AzureCloudConfig
from ...auth.cloud_mappings import (
    get_defender_endpoint,
    get_m365d_endpoint,
    get_m365d_login_endpoint,
)
from ...common.data_utils import ensure_df_datetimes
from ...common.utility import export
from ..core.query_defns import DataEnvironment
from .odata_driver import OData, _get_driver_settings

__version__ = VERSION
__author__ = "Pete Bryan"


@export
class MDATPDriver(OData):
    """KqlDriver class to retrieve date from MS Defender APIs."""

    CONFIG_NAME = "MicrosoftDefender"
    _ALT_CONFIG_NAMES: List[str] = ["MDATPApp"]

    def __init__(  # pylint: disable = too-many-arguments
        self,
        connection_str: Optional[str] = None,
        instance: str = "Default",
        data_environment: Optional[Union[str, DataEnvironment]] = None,
        *,
        max_threads: int = 4,
        debug: bool = False,
        cloud: Optional[str] = None,
        api_ver: Optional[str] = "v1.0",
    ) -> None:
        """
        Instantiate MSDefenderDriver and optionally connect.

        Parameters
        ----------
        connection_str : str, optional
            Connection string
        instance : str, optional
            The instance name from config to use

        """
        super().__init__(
            data_environment=data_environment,
            max_threads=max_threads,
            debug=debug,
        )
        cs_dict: Dict[str, str] = _get_driver_settings(
            self.CONFIG_NAME, self._ALT_CONFIG_NAMES, instance
        )

        self.cloud: str = cloud or cs_dict.pop("cloud", "global")

        api_uri, oauth_uri, api_suffix = _select_api_uris(
            self.data_environment, self.cloud
        )
        self.add_query_filter(
            "data_environments", ("MDE", "M365D", "MDATP", "GraphHunting")
        )

        self.req_body = {
            "client_id": None,
            "client_secret": None,
            "grant_type": "client_credentials",
            "resource": api_uri,
        }
        self.oauth_url = oauth_uri
        self.api_root = api_uri
        self.api_ver = "api"
        self.api_suffix: str = api_suffix
        if self.data_environment == DataEnvironment.M365D:
            self.scopes: List[str] = [f"{api_uri}/AdvancedHunting.Read"]
        elif self.data_environment == DataEnvironment.M365DGraph:
            self.api_ver = api_ver
            self.req_body = {
                "client_id": None,
                "client_secret": None,
                "grant_type": "client_credentials",
                "scope": f"{self.api_root}.default",
            }
            self.scopes = [f"{api_uri}/ThreatHunting.Read.All"]
        else:
            self.scopes = [f"{api_uri}/AdvancedQuery.Read"]

        if connection_str:
            self.current_connection = connection_str
            self.connect(connection_str)

    def query(
        self,
        query: str,
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
        Union[pd.DataFrame, results.ResultSet]
            A DataFrame (if successful) or
            the underlying provider result if an error.

        """
        data, response = self.query_with_results(
            query, body=True, api_end=self.api_suffix
        )
        if isinstance(data, pd.DataFrame):
            # If we got a schema we should convert the DateTimes to pandas datetimes
            if ("Schema" or "schema") not in response:
                return data

            if self.data_environment == DataEnvironment.M365DGraph:
                date_fields: List[str] = [
                    field["name"]
                    for field in response["schema"]
                    if field["type"] == "DateTime"
                ]
            else:
                date_fields = [
                    field["Name"]
                    for field in response["Schema"]
                    if field["Type"] == "DateTime"
                ]
            data = ensure_df_datetimes(data, columns=date_fields)
            return data
        return response


def _select_api_uris(data_environment, cloud) -> Tuple[str, str, str]:
    """Return API and login URIs for selected provider type."""
    login_uri: str = get_m365d_login_endpoint(cloud)
    if data_environment == DataEnvironment.M365D:
        return (
            get_m365d_endpoint(cloud),
            f"{login_uri}{{tenantId}}/oauth2/token",
            "/advancedhunting/run",
        )
    if data_environment == DataEnvironment.M365DGraph:
        az_cloud_config = AzureCloudConfig(cloud=cloud)
        api_uri: str = az_cloud_config.endpoints.get("microsoftGraphResourceId", "")
        graph_login = az_cloud_config.authority_uri
        return (
            api_uri,
            f"{graph_login}{{tenantId}}/oauth2/v2.0/token",
            "/security/runHuntingQuery",
        )
    return (
        get_defender_endpoint(cloud),
        f"{login_uri}{{tenantId}}/oauth2/token",
        "/advancedqueries/run",
    )
