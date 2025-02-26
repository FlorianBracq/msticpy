# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""Data uploader base class."""
from __future__ import annotations

import abc
from abc import ABC

import pandas as pd
from httpx import Timeout
from typing_extensions import Self

from ..._version import VERSION
from ...common.pkg_config import get_http_timeout

__version__ = VERSION
__author__ = "Pete Bryan"


class UploaderBase(ABC):
    """Base class for data providers."""

    def __init__(self: UploaderBase, **kwargs) -> None:
        """Initialize new instance."""
        self._kwargs = kwargs
        self.workspace = None
        self.workspace_secret = None
        self._connected = False
        self._debug = False

    @abc.abstractmethod
    def upload_file(
        self: Self,
        file_path: str,
        table_name: str,
        delim: str = ",",
        **kwargs,
    ) -> None:
        """
        Upload a file to the data store.

        Parameters
        ----------
        file_path : str
            Path to the file to upload
        table_name : str
            The name of the table to upload the file to
        delim : Optional[str]
            Column deliminator in data file, default is ,

        """

    @abc.abstractmethod
    def upload_folder(
        self: Self,
        folder_path: str,
        table_name: str | None = None,
        delim: str = ",",
        **kwargs,
    ) -> None:
        """
        Upload a folder of files to the data store.

        Parameters
        ----------
        folder_path : str
            Path to the folder of files to upload
        table_name : Optional[str]
            The name of the table to upload the file to, if not set file name is used as table name
        delim : Optional[str]
            Column deliminator in data file, default is ,

        """

    @abc.abstractmethod
    def upload_df(self: Self, data: pd.DataFrame, table_name: str, **kwargs) -> None:
        """
        Upload a Pandas DataFrame to the data store.

        Parameters
        ----------
        data : pd.DataFrame
            The DataFrame to upload
        table_name : str
            The name of the table to upload the DataFrame to

        """

    @staticmethod
    def get_http_timeout(
        *,
        timeout: int | None = None,
        def_timeout: int | None = None,
    ) -> Timeout:
        """Get http timeout from settings or kwargs."""
        return get_http_timeout(
            timeout=timeout,
            def_timeout=def_timeout,
        )
