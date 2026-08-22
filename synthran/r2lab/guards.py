"""Shared read-only guards for physical R2Lab mutation boundaries.

This module centralizes the current-path proof that must be refreshed before a
qfit/PDU/user-plane/workload mutation may proceed.  It performs no provider,
Kubernetes, radio, modem, attach, or packet-session mutation.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

from synthran.live_preflight import CommandResult
from synthran.r2lab.acceptance import PhysicalRunEvidence
from synthran.r2lab.deployment import PhysicalStartAuthority
from synthran.r2lab.readiness import (
    QfitReadinessEvidence,
    R2LabQfitReadinessError,
    execute_qfit_readiness,
)
from synthran.r2lab.runtime import GnbN2Evidence, verify_gnb_n2


Runner = Callable[[Sequence[str], int], CommandResult]


class R2LabMutationGuardError(RuntimeError):
    """Raised when current physical authority/path readiness is not proven."""


@dataclass(frozen=True)
class QfitMutationGuardEvidence:
    """Sanitized proof required immediately before qfit-path mutation."""

    authority: PhysicalStartAuthority
    gnb_n2: GnbN2Evidence
    readiness: QfitReadinessEvidence

    @property
    def proven(self) -> bool:
        return (
            self.authority.lease_verified is True
            and self.authority.radio_state == "on"
            and self.gnb_n2.proven
            and self.readiness.ready
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.authority.run_id,
            "radio": self.authority.radio,
            "ue": self.authority.ue,
            "authority": "current",
            "gnb_n2": self.gnb_n2.to_dict(),
            "qfit_readiness": self.readiness.to_dict(),
            "proven": self.proven,
        }


def _same_authority(
    authority: PhysicalStartAuthority, evidence: PhysicalRunEvidence
) -> bool:
    if evidence.gnb_start is None:
        return False
    return (
        authority.run_id == evidence.run_id
        and authority.radio == "n300"
        and authority.ue_kind == "qfit"
        and authority.claim_sha256 == evidence.gnb_start.claim_sha256
        and authority.lease_verified is True
        and authority.radio_state == "on"
    )


def prove_qfit_mutation_guard(
    *,
    evidence: PhysicalRunEvidence,
    slice_name: str,
    run_root: Path,
    known_hosts: Path,
    qfit_known_hosts_remote: str,
    r2lab_runner: Runner,
    cluster_runner: Runner,
    expected_gnb_n2_peer: str | None = None,
    timeout_seconds: int = 30,
) -> QfitMutationGuardEvidence:
    """Refresh exact authority, N2, and qfit/FIT readiness without mutation."""

    if evidence.gnb_start is None:
        raise R2LabMutationGuardError(
            "qfit mutation guard requires bound singleton gNB start evidence"
        )
    if timeout_seconds < 5 or timeout_seconds > 60:
        raise R2LabMutationGuardError(
            "qfit mutation guard timeout must be between 5 and 60 seconds"
        )
    if not qfit_known_hosts_remote:
        raise R2LabMutationGuardError(
            "qfit mutation guard requires an explicit strict remote known-hosts path"
        )

    from synthran.r2lab.controller import authorize_physical_start, gateway_command

    authority = authorize_physical_start(
        run_id=evidence.run_id,
        slice_name=slice_name,
        run_root=run_root,
        runner=r2lab_runner,
        timeout_seconds=timeout_seconds,
    ).validate()
    if not _same_authority(authority, evidence):
        raise R2LabMutationGuardError(
            "R2Lab claim or selected-resource authority changed"
        )

    gnb = verify_gnb_n2(
        evidence=evidence,
        known_hosts=known_hosts,
        runner=cluster_runner,
        expected_gnb_n2_peer=expected_gnb_n2_peer,
        timeout_seconds=timeout_seconds,
    )
    if not gnb.proven:
        raise R2LabMutationGuardError(
            "current singleton gNB/N2 proof is not established"
        )

    def faraday_runner(
        command: Sequence[str], command_timeout: int
    ) -> CommandResult:
        return r2lab_runner(
            gateway_command(slice_name, *tuple(command)), command_timeout
        )

    try:
        readiness = execute_qfit_readiness(
            qfit=authority.ue,
            remote_known_hosts=qfit_known_hosts_remote,
            runner=faraday_runner,
            timeout_seconds=min(timeout_seconds, 30),
        )
    except R2LabQfitReadinessError as exc:
        raise R2LabMutationGuardError(str(exc)) from exc
    if not readiness.ready:
        raise R2LabMutationGuardError(
            "selected qfit/FIT management path is not ready"
        )

    return QfitMutationGuardEvidence(
        authority=authority,
        gnb_n2=gnb,
        readiness=readiness,
    )
