"""This is a set of sample functions to demonstrate how process analysers could work."""

from typing import Any, Dict, List, Optional, Union

import pandas as pd

from .common import assess_process_by_process_name_and_command_line


def generic_suspicious_process_execution(
    dataframe: pd.DataFrame,
    process_names: Optional[Union[str, List[str]]] = None,
    command_lines: Optional[Union[str, List[str]]] = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Search for generic supsicious process executions.

    Based on:
    https://github.com/SigmaHQ/sigma/blob/master/rules/windows/process_creation/
    proc_creation_win_susp_system_user_anomaly.yml

    Parameters
    ----------
    dataframe: pd.DataFrame
        Data to analyze.
    process_names: Optional[Union[str, List[str]]]
        Common process names used for exploitation.
    command_lines: Optional[Union[str, List[str]]]
        Known command lines associated with exploits.

    Returns
    -------
    pd.DataFrame
        DataFrame rows where columns contain one of the keywords

    """
    if not process_names:
        process_names = [
            "calc.exe",
            "wscript.exe",
            "cscript.exe",
            "hh.exe",
            "mshta.exe",
            "forfiles.exe",
            "ping.exe",
        ]
    if not command_lines:
        command_lines = [
            " -NoP ",  # Often used in malicious PowerShell commands
            " -W Hidden ",  # Often used in malicious PowerShell commands
            " -decode ",  # Used with certutil
            " /decode ",  # Used with certutil
            " /urlcache ",  # Used with certutil
            " -urlcache ",  # Used with certutil
            " -e* JAB",  # PowerShell encoded commands
            " -e* SUVYI",  # PowerShell encoded commands
            " -e* SQBFAFgA",  # PowerShell encoded commands
            " -e* aWV4I",  # PowerShell encoded commands
            " -e* IAB",  # PowerShell encoded commands
            " -e* PAA",  # PowerShell encoded commands
            " -e* aQBlAHgA",  # PowerShell encoded commands
            "vssadmin delete shadows",  # Ransomware
            "reg SAVE HKLM",  # save registry SAM - syskey extraction
            " -ma ",  # ProcDump
            r"Microsoft\Windows\CurrentVersion\Run",  # Run key in command line
            # often in combination with REG ADD
            ".downloadstring(",  # PowerShell download command
            ".downloadfile(",  # PowerShell download command
            " /ticket:",  # Rubeus
            "dpapi::",  # Mimikatz
            "event::clear",  # Mimikatz
            "event::drop",  # Mimikatz
            "id::modify",  # Mimikatz
            "kerberos::",  # Mimikatz
            "lsadump::",  # Mimikatz
            "misc::",  # Mimikatz
            "privilege::",  # Mimikatz
            "rpc::",  # Mimikatz
            "sekurlsa::",  # Mimikatz
            "sid::",  # Mimikatz
            "token::",  # Mimikatz
            "vault::cred",  # Mimikatz
            "vault::list",  # Mimikatz
            " p::d ",  # Mimikatz
            ";iex(",  # PowerShell IEX
            "MiniDump",  # Process dumping method apart from procdump
            "net user ",
        ]
    return assess_process_by_process_name_and_command_line(
        dataframe,
        process_names=process_names,
        command_lines=command_lines,
        **kwargs,
    )


ANALYSER_GENERIC_PROCESS: List[Dict[str, Any]] = [
    {"function": generic_suspicious_process_execution, "score": 70},
]
