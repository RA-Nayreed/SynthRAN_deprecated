"""One strict SSH policy for controller, cluster, and delegated transports."""

from __future__ import annotations

from pathlib import Path
import shlex
from typing import Iterable


def strict_ssh_options(
    *,
    known_hosts: str | Path | None = None,
    identity: str | Path | None = None,
    port: int | None = None,
    connect_timeout: int = 10,
    isolated_config: bool = True,
) -> tuple[str, ...]:
    """Return fail-closed OpenSSH options without disabling host-key trust."""

    if connect_timeout < 1 or connect_timeout > 300:
        raise ValueError("SSH connect timeout must be between 1 and 300 seconds")
    if port is not None and (port < 1 or port > 65535):
        raise ValueError("SSH port must be between 1 and 65535")

    options: list[str] = []
    if isolated_config:
        options.extend(("-F", "/dev/null"))
    options.extend(
        (
            "-o",
            "BatchMode=yes",
            "-o",
            f"ConnectTimeout={connect_timeout}",
            "-o",
            "StrictHostKeyChecking=yes",
        )
    )
    if known_hosts is not None:
        options.extend(("-o", f"UserKnownHostsFile={known_hosts}"))
    if identity is not None:
        options.extend(("-o", "IdentitiesOnly=yes", "-i", str(identity)))
    if port is not None:
        options.extend(("-p", str(port)))
    return tuple(options)


def strict_ssh_command(
    target: str,
    *remote: str,
    known_hosts: str | Path | None = None,
    identity: str | Path | None = None,
    port: int | None = None,
    connect_timeout: int = 10,
    isolated_config: bool = True,
    quote_remote: bool = False,
) -> tuple[str, ...]:
    """Build one deterministic strict-SSH argv boundary."""

    if not target or any(character in target for character in "\r\n\x00"):
        raise ValueError("SSH target is malformed")
    command = [
        "ssh",
        *strict_ssh_options(
            known_hosts=known_hosts,
            identity=identity,
            port=port,
            connect_timeout=connect_timeout,
            isolated_config=isolated_config,
        ),
        "--",
        target,
    ]
    if remote:
        command.extend((shlex.join(remote),) if quote_remote else remote)
    return tuple(command)


def strict_scp_command(
    sources: Iterable[str | Path],
    destination: str,
    *,
    known_hosts: str | Path | None = None,
    identity: str | Path | None = None,
    port: int | None = None,
    connect_timeout: int = 10,
    isolated_config: bool = True,
) -> tuple[str, ...]:
    """Build an SCP command using the same fail-closed SSH policy."""

    source_values = tuple(str(source) for source in sources)
    if not source_values:
        raise ValueError("SCP requires at least one source")
    if not destination or any(character in destination for character in "\r\n\x00"):
        raise ValueError("SCP destination is malformed")
    options = list(
        strict_ssh_options(
            known_hosts=known_hosts,
            identity=identity,
            port=None,
            connect_timeout=connect_timeout,
            isolated_config=isolated_config,
        )
    )
    if port is not None:
        if port < 1 or port > 65535:
            raise ValueError("SSH port must be between 1 and 65535")
        options.extend(("-P", str(port)))
    return ("scp", *options, "--", *source_values, destination)


def ansible_ssh_common_args(
    *,
    known_hosts: str | Path | None = None,
    connect_timeout: int = 10,
    isolated_config: bool = True,
) -> str:
    """Render the same SSH policy for Ansible inventory host variables."""

    return shlex.join(
        strict_ssh_options(
            known_hosts=known_hosts,
            connect_timeout=connect_timeout,
            isolated_config=isolated_config,
        )
    )
