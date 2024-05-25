# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""Security Graph OData Driver class."""
from typing import List, Optional, Union

import pandas as pd

from ..._version import VERSION
from ...auth.azure_auth_core import AzureCloudConfig
from ...common.utility import export
from ..core.query_defns import DataEnvironment
from .odata_driver import OData

__version__ = VERSION
__author__ = "Ian Hellen"


@export
class SecurityGraphDriver(OData):
    """Driver to query security graph."""

    CONFIG_NAME = "MicrosoftGraph"
    _ALT_CONFIG_NAMES: List[str] = ["SecurityGraphApp"]

    def __init__(
        self,
        data_environment: Optional[
            Union[str, DataEnvironment]
        ] = DataEnvironment.SecurityGraph,
        connection_str: Optional[str] = None,
        *,
        max_threads: int = 4,
        debug: bool = False,
        cloud: Optional[str] = None,
        api_ver: str = "v1.0",
    ) -> None:
        """
        Instantiate MSGraph driver and optionally connect.

        Parameters
        ----------
        connection_str : str, optional
            Connection string

        """
        super().__init__(
            data_environment=data_environment,
            max_threads=max_threads,
            debug=debug,
        )
        az_cloud_config = AzureCloudConfig(cloud=cloud)
        self.scopes = ["User.Read"]
        self.api_root = az_cloud_config.endpoints.get("microsoftGraphResourceId")
        self.req_body = {
            "client_id": None,
            "client_secret": None,
            "grant_type": "client_credentials",
            "scope": f"{self.api_root}.default",
        }
        login_endpoint: str = az_cloud_config.authority_uri
        self.oauth_url = f"{login_endpoint}{{tenantId}}/oauth2/v2.0/token"
        self.api_ver = api_ver

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

        Returns
        -------
        Union[pd.DataFrame, results.ResultSet]
            A DataFrame (if successful) or
            the underlying provider result if an error.

        """
        return self.query_with_results(query, body=False)[0]
