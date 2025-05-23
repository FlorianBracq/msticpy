# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""Checker functions for Azure ML notebooks."""
import logging
import os
import sys
import warnings
from pathlib import Path
from typing import Any, List, Mapping, Optional, Tuple, Union

import yaml
from IPython import get_ipython
from IPython.display import HTML, display
from pkg_resources import Requirement, WorkingSet, parse_version  # type: ignore

from .._version import VERSION
from ..common.pkg_config import _HOME_PATH, get_config, refresh_config
from ..common.utility import search_for_file, unit_testing
from ..config import MpConfigFile  # pylint: disable=no-name-in-module

__version__ = VERSION

logger = logging.getLogger(__name__)

AZ_GET_STARTED = (
    "https://github.com/Azure/Azure-Sentinel-Notebooks/blob/master/A%20Getting"
    "%20Started%20Guide%20For%20Azure%20Sentinel%20ML%20Notebooks.ipynb"
)
TROUBLE_SHOOTING = (
    "https://github.com/Azure/Azure-Sentinel-Notebooks/blob/master/"
    "TroubleShootingNotebooks.ipynb"
)
MISSING_PKG_ERR = """
    <h4><font color='orange'>The package '<b>{package}</b>' is not
    installed or has an unsupported version (installed version = '{inst_ver}')</font></h4>
    Please install or upgrade before continuing: required version is {package}>={req_ver}
    """
MP_INSTALL_FAILED = """
    <h4><font color='red'>The notebook may not run correctly without
    the correct version of '<b>{pkg}</b>' ({ver} or later).</font></h4>
    Please see the <a href="{nbk_uri}">
    Getting Started Guide For Azure Sentinel ML Notebooks</a></b>
    for more information<br><hr>
"""
RELOAD_MP = """
    <h4><font color='orange'>Kernel restart needed</h4>
    An error was detected trying to load the updated version of MSTICPy.<br>
    Please restart the notebook kernel and re-run this cell - it should
    run without error.
    """
_AZ_CLI_WIKI_URI = (
    "https://github.com/Azure/Azure-Sentinel-Notebooks/wiki/"
    "Caching-credentials-with-Azure-CLI"
)
_CLI_WIKI_MSSG_GEN = (
    f"For more information see <a href='{_AZ_CLI_WIKI_URI}'>"
    "Caching credentials with Azure CLI</>"
)
_CLI_WIKI_MSSG_SHORT = (
    f"see <a href='{_AZ_CLI_WIKI_URI}'>Caching credentials with Azure CLI</>"
)
_PYSPARK_KERNEL_NOT_SUPPORTED = """
    <h4><font color='red'>WARNING: This notebook may not run correctly with the Azure Machine
    Learning PySpark kernel.</font></h4> Please create a standard Azure Machine Learning
    Compute instance and select the Python 3.10 or later kernel.
    Please see the <a href="{nbk_uri}">
    Getting Started Guide For Azure Sentinel ML Notebooks</a></b>
    for more information<br><hr>
"""

MIN_PYTHON_VER_DEF = "3.10.0"
MSTICPY_REQ_VERSION = __version__

VER_RGX = r"(?P<maj>\d+)\.(?P<min>\d+).(?P<pnt>\d+)(?P<suff>.*)"
MP_ENV_VAR = "MSTICPYCONFIG"
MP_FILE = "msticpyconfig.yaml"


def is_in_aml_pyspark():
    """Return True if running in Spark on Azure Machine Learning."""
    return os.environ.get("AZUREML_FRAMEWORK") == "PySpark"


def is_in_aml():
    """Return True if running in Azure Machine Learning."""
    return os.environ.get("APPSETTING_WEBSITE_SITE_NAME") == "AMLComputeInstance"


def check_aml_settings(
    min_py_ver: Union[str, Tuple] = MIN_PYTHON_VER_DEF,
    min_mp_ver: Union[str, Tuple] = MSTICPY_REQ_VERSION,
    extras: Optional[List[str]] = None,
    mp_release: Optional[str] = None,
    **kwargs,
):
    """
    Check the current versions of the Python kernel and MSTICPy.

    Parameters
    ----------
    min_py_ver : Union[Tuple[int, int], str]
        Minimum Python version
    min_mp_ver : Union[Tuple[int, int], str]
        Minimum MSTICPy version
    extras : Optional[List[str]], optional
        A list of extras required for MSTICPy
    mp_release : Optional[str], optional
        Override the MSTICPy release version. This
        can also be specified in the environment variable 'MP_TEST_VER'

    Raises
    ------
    RuntimeError
        If the Python version does not support the notebook.
        If the MSTICPy version does not support the notebook
        and the user chose not to upgrade

    """
    del kwargs
    _disp_html("<h4>Starting AML notebook pre-checks...</h4>")
    _check_pyspark()
    if isinstance(min_py_ver, str):
        min_py_ver = _get_pkg_version(min_py_ver).release  # type: ignore
    check_python_ver(min_py_ver=min_py_ver)

    _check_mp_install(min_mp_ver, mp_release, extras)
    if _kql_magic_installed():
        _check_kql_prereqs()
        _set_kql_env_vars(extras)
    _run_user_settings()
    _set_mpconfig_var()
    _check_azure_cli_status()
    _check_aml_auth_method_order()
    _disp_html("<h4>Notebook pre-checks complete.</h4>")


# retain previous name for backward compatibility
check_versions = check_aml_settings


def _check_pyspark():
    """Check if we are running in PySpark kernel."""
    if not is_in_aml_pyspark():
        return

    _disp_html(_PYSPARK_KERNEL_NOT_SUPPORTED.format(nb_uri=AZ_GET_STARTED))


def _kql_magic_installed():
    try:
        # pylint: disable=import-outside-toplevel, unused-import
        from Kqlmagic import kql  # noqa: F401

        return True
    except ImportError:
        return False


def check_python_ver(min_py_ver: Union[str, Tuple] = MIN_PYTHON_VER_DEF):
    """
    Check the current version of the Python kernel.

    Parameters
    ----------
    min_py_ver : Tuple[int, int]
        Minimum Python version

    Raises
    ------
    RuntimeError
        If the Python version does not support the notebook.

    """
    minimum_py_version = max(
        _get_pkg_version(min_py_ver), _get_pkg_version(MIN_PYTHON_VER_DEF)
    )
    sys_ver = _get_pkg_version(sys.version_info[:3])
    _disp_html("Checking Python kernel version...")
    if sys_ver < minimum_py_version:
        # Bandit SQL inject error found here
        _disp_html(
            f"""
            <h4><font color='red'>This notebook requires a later
            (Python) kernel version.</h4></font>
            Choose a kernel from the notebook toolbar (above), that is Python
            {minimum_py_version} or later (Python 3.10 recommended)<br>
            """
        )
        _disp_html(
            f"""
            Please see the <a href="{TROUBLE_SHOOTING}">TroubleShootingNotebooks</a>
            for more information<br><br><hr>
            """
        )
        # Bandit SQL inject error found here
        raise RuntimeError(f"Python {minimum_py_version} or later kernel is required.")

    _disp_html(f"Info: Python kernel version {sys_ver} - OK<br>")


def check_mp_ver(min_msticpy_ver: Union[str, Tuple], extras: Optional[List[str]]):
    """
    Check and optionally update the current version of msticpy.

    Parameters
    ----------
    min_msticpy_ver : Tuple[int, int]
        Minimum MSTICPy version
    extras : Optional[List[str]], optional
        A list of extras required for MSTICPy

    Raises
    ------
    ImportError
        If MSTICPy version is insufficient and we need to upgrade

    """
    mp_min_pkg_ver = _get_pkg_version(min_msticpy_ver)

    _disp_html("Checking msticpy version...<br>")

    # Check currently loaded MP version against notebook requirement
    loaded_version = _get_pkg_version(__version__)
    if loaded_version < mp_min_pkg_ver:
        _disp_html(
            MISSING_PKG_ERR.format(
                package="msticpy",
                inst_ver=loaded_version,
                req_ver=mp_min_pkg_ver,
            )
        )
        mp_pkg_spec = f"msticpy[{','.join(extras)}]" if extras else "msticpy"
        mp_pkg_spec = f"{mp_pkg_spec}>={min_msticpy_ver}"

        _disp_html(
            f"Please run the following command to upgrade MSTICPy<br>"
            f"<pre>%pip install --upgrade {mp_pkg_spec}</pre><br>"
        )
        raise ImportError(
            "Unsupported version of MSTICPy installed",
            f"Installed version: {loaded_version}",
            f"Required version: {mp_min_pkg_ver}",
        )

    # Check loaded version against installed version in the environment
    # If the version was updated after this version was loaded by Python
    # we need to warn the user to restart the kernel.
    installed_version = _get_installed_mp_version()
    if installed_version and installed_version > loaded_version:
        _disp_html(
            f"A newer version of MSTICPy ({installed_version})"
            "has been installed but has not been loaded.<br>"
            "Please restart the notebook kernel and re-run this cell."
        )
    _disp_html(f"Info: msticpy version {loaded_version} (>= {mp_min_pkg_ver}) - OK<br>")


def populate_config_to_mp_config(mp_path):
    """Populate new or existing msticpyconfig with settings from config.json."""
    # Look for a config.json
    config_json = search_for_file("config.json", paths=[get_aml_user_folder()])
    if not config_json:
        return None

    # if we found one, use it to populate msticpyconfig.yaml
    mp_path = mp_path or str(get_aml_user_folder().joinpath("msticpyconfig.yaml"))
    mp_config_convert = MpConfigFile(file=config_json)
    azs_settings = mp_config_convert.map_json_to_mp_ws()
    def_azs_settings = next(
        iter(azs_settings.get("AzureSentinel", {}).get("Workspaces", {}).values())
    )
    if def_azs_settings:
        mp_config_convert.settings["AzureSentinel"]["Workspaces"][
            "Default"
        ] = def_azs_settings.copy()

    if Path(mp_path).exists():
        # If there is an existing file read it in
        mp_config_text = Path(mp_path).read_text(encoding="utf-8")
        mp_config_settings = yaml.safe_load(mp_config_text)
        # update exist settings with the AzSent settings from config.json
        mp_config_settings.update(mp_config_convert.settings)
        # update MpConfigFile with the merged settings
        mp_config_convert.settings = mp_config_settings
        mssg = (
            f"Updated '{mp_path}' with Microsoft Sentinel settings"
            f" imported from {config_json}."
        )
    else:
        mssg = (
            f"Created '{mp_path}' with Microsoft Sentinel settings"
            f" imported from {config_json}."
        )
    # Save the file
    mp_config_convert.save_to_file(mp_path, backup=True)
    logger.info(mssg)
    return mssg


def _check_mp_install(
    min_mp_ver: Union[str, Tuple],
    mp_release: Optional[str],
    extras: Optional[List[str]],
):
    """Check for and try to install required MSTICPy version."""
    # Use the release ver specified in params, in the environment or
    # the notebook default.
    pkg_version = _get_pkg_version(min_mp_ver)
    mp_install_version = mp_release or os.environ.get("MP_TEST_VER") or str(pkg_version)

    check_mp_ver(min_msticpy_ver=mp_install_version, extras=extras)


def _set_kql_env_vars(extras: Optional[List[str]]):
    """Set environment variables for Kqlmagic based on MP extras."""
    jp_extended = ("azsentinel", "azuresentinel", "kql")
    if extras and any(extra for extra in extras if extra in jp_extended):
        os.environ["KQLMAGIC_EXTRAS_REQUIRE"] = "jupyter-extended"
    else:
        os.environ["KQLMAGIC_EXTRAS_REQUIRE"] = "jupyter-basic"
    if is_in_aml():
        os.environ["KQLMAGIC_AZUREML_COMPUTE"] = _get_vm_fqdn()


def _get_pkg_version(version: Union[str, Tuple]):
    """Return pkg_resources parsed version from string or tuple."""
    if isinstance(version, str):
        return parse_version(version)
    if isinstance(version, tuple):
        return parse_version(".".join(str(ver) for ver in version))
    raise TypeError(f"Version {version} not cannot be parsed.")


def _get_installed_mp_version():
    """Return the installed version of MSTICPY."""
    working_set = WorkingSet()
    mp_installed = working_set.find(Requirement("msticpy"))  # type: ignore
    if mp_installed:
        return mp_installed.parsed_version
    return None


def _disp_html(text: str):
    """Display the HTML text."""
    display(HTML(text))


def get_aml_user_folder() -> Optional[Path]:
    """Return the root of the user folder."""
    path_parts = Path(".").absolute().parts
    if "Users" not in path_parts:
        return Path(_HOME_PATH).expanduser()
    # find the index of the last occurrence of "users"
    users_idx = len(path_parts) - path_parts[::-1].index("Users")
    # the user folder is one item below this
    if len(path_parts) < users_idx + 1:
        return None
    return Path("/".join(path_parts[: users_idx + 1]))


# pylint: disable=import-outside-toplevel, unused-import, import-error
def _run_user_settings():
    """Check for nbuser_settings.py and warn that it is no longer used."""
    user_folder = get_aml_user_folder()
    if user_folder and user_folder.joinpath("nbuser_settings.py").is_file():
        warnings.warn(
            "Automatic processing of nbuser_settings.py is no longer supported in MSTICPy."
        )


# pylint: enable=import-outside-toplevel, unused-import, import-error


def _set_mpconfig_var():
    """Set MSTICPYCONFIG to file in user directory if no other found."""
    mp_path_val = os.environ.get(MP_ENV_VAR)
    if (
        # If a valid MSTICPYCONFIG value is found - return
        (mp_path_val and Path(mp_path_val).is_file())
        # Or if there is a msticpyconfig in the current folder.
        or Path(".").joinpath(MP_FILE).is_file()
    ):
        return
    # Otherwise check the user's root folder
    user_dir = get_aml_user_folder()
    if user_dir:
        mp_path = Path(user_dir).joinpath(MP_FILE)
        if mp_path.is_file():
            # If there's a file there, set the env variable to that.
            os.environ[MP_ENV_VAR] = str(mp_path)
            # Since we have already imported msticpy to check the version
            # it will have already configured settings so we need to refresh.
            refresh_config()
            _disp_html(
                f"<br>No {MP_FILE} found. Will use {MP_FILE} in user folder {user_dir}<br>"
            )


_NBVM_PATH = "/mnt/azmnt/.nbvm"


def _get_vm_metadata() -> Mapping[str, Any]:
    """Read VM metadata from definition file."""
    with open(_NBVM_PATH, "r", encoding="utf-8") as nbvm_handle:
        nbvm_lines = nbvm_handle.readlines()
    return {
        item[0]: item[1]
        for item in map(lambda x: x.split("=", maxsplit=1), nbvm_lines)
        if item
    }


def _get_vm_fqdn() -> str:
    """Get the FQDN of the host."""
    vm_metadata = _get_vm_metadata()
    if vm_metadata and "instance" in vm_metadata:
        return (
            f"https://{vm_metadata.get('instance')}.{vm_metadata.get('domainsuffix')}"
        )
    return ""


def _check_kql_prereqs():
    """
    Check and install packages for Kqlmagic/msal_extensions.

    Notes
    -----
    Kqlmagic may trigger warnings about a missing PyGObject package
    and some system library dependencies. To fix this do the
    following:<br>
    From a notebook run:

        %pip uninstall enum34
        !sudo apt-get --yes install libgirepository1.0-dev
        !sudo apt-get --yes install gir1.2-secret-1
        %pip install pygobject

    You can also do this from a terminal - but ensure that you've
    activated the environment corresponding to the kernel you are
    using prior to running the pip commands.

        # Install the libgi dependency
        sudo apt install libgirepository1.0-dev
        sudo apt install gir1.2-secret-1

        # activate the environment
        # conda activate azureml_py38
        # source ./env_path/scripts/activate

        # Uninstall enum34
        python -m pip uninstall enum34
        # Install pygobject
        python -m install pygobject

    """
    if not is_in_aml():
        return
    try:
        # If this successfully imports, we are ok
        # pylint: disable=import-outside-toplevel
        import gi

        # pylint: enable=import-outside-toplevel
        del gi
    except ImportError:
        # Check for system packages
        ip_shell = get_ipython()
        if not ip_shell:
            return
        apt_list = ip_shell.run_line_magic("sx", "apt list")
        apt_list = [apt.split("/", maxsplit=1)[0] for apt in apt_list]
        missing_lx_pkg = [
            apt_pkg
            for apt_pkg in ("libgirepository1.0-dev", "gir1.2-secret-1")
            if apt_pkg not in apt_list
        ]
        if missing_lx_pkg:
            _disp_html(
                "Kqlmagic/msal-extensions pre-requisite PyGObject not installed."
            )
            _disp_html(
                "To prevent warnings when loading the Kqlmagic data provider,"
                " Please run the following command:<br>"
                "!conda install --yes -c conda-forge pygobject<br>"
            )


def _check_azure_cli_status():
    """Check for Azure CLI credentials."""
    # import these only if we need them at runtime
    # pylint: disable=import-outside-toplevel
    from ..auth.azure_auth_core import AzureCliStatus, check_cli_credentials

    if unit_testing():
        return

    status, message = check_cli_credentials()
    if status == AzureCliStatus.CLI_OK:
        _disp_html(message)
        return

    _disp_html(
        "<h4>Azure CLI authentication recommended</h4>"
        "Azure CLI gives the most seamless authentication experience for "
        "authenticating to Azure Resources from Azure Machine Learning.<br>"
        "Create a new notebook cell and enter the following command:<br>"
        "<pre>!az login</pre>"
        "You will be prompted to authenticate to Azure. Once you have "
        "completed this, return to this notebook and continue.<br>"
    )
    if status == AzureCliStatus.CLI_NOT_INSTALLED:
        _disp_html("Azure CLI not installed." f" ({_CLI_WIKI_MSSG_SHORT})")
    elif message:
        _disp_html("\n".join([message, _CLI_WIKI_MSSG_GEN]))


_MSI_WARNING = """
<h4><font color='red'>WARNING: MSI authentication to MS Sentinel not supported</font></h4>
Your have MSI authentication as higher priority than CLI or DeviceCode.
This may cause authentication to MS Sentinel to fail since MSI authentication from
Azure Machine Learning is no longer supported.<br>
We recommend that you change your msticpyconfig.yaml so that
you either remove MSI or place it below CLI and device code methods.<br>
Example:<br>
<pre>
Azure:
    auth_methods:
        - cli
        - devicecode
        - msi
</pre>
"""


def _check_aml_auth_method_order():
    """Reorder the auth methods to put Azure CLI first."""
    try:
        current_methods = get_config("Azure.auth_methods")
    except KeyError:
        return
    if not current_methods:
        return
    auth_index = {meth: idx for idx, meth in enumerate(current_methods)}
    msi_lower_than_cli = auth_index.get("msi", 99) > auth_index.get("cli", 99)
    msi_lower_than_devcode = auth_index.get("msi", 99) > auth_index.get(
        "devicecode", auth_index.get("device_code", 99)
    )
    if msi_lower_than_cli or msi_lower_than_devcode:
        return
    _disp_html(_MSI_WARNING)
    logger.warning("MSI authentication is higher priority than CLI or DeviceCode.")
    if "msi" in current_methods:
        _disp_html("Reordering auth_methods to move MSI to lowest priority.")
        current_methods.remove("msi")
        current_methods.append("msi")
        logger.info("Reordering auth_methods to move MSI to the end.")
