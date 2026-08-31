"""Exact TCP proof for traffic sourced from the accepted RFSIM PDU address."""

from __future__ import annotations

import shlex
import time

from synthran.experiment import ExperimentError
from synthran.experiment.live import _remote
from synthran.fiveg_ansible import NetworkInventory


KUBERNETES_NAMESPACE = "open5gs"


def pdu_bound_tcp_connected(
    inventory: NetworkInventory,
    pod: str,
    *,
    pdu_address: str,
    remote_address: str,
    remote_port: int,
) -> bool:
    """Attempt one TCP connect explicitly bound to the live UE PDU address."""

    probe = r'''
import socket, sys
local_ip, remote_ip, remote_port = sys.argv[1], sys.argv[2], int(sys.argv[3])
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(2.0)
try:
    s.bind((local_ip, 0))
    s.connect((remote_ip, remote_port))
except OSError:
    print('0')
else:
    print('1')
finally:
    s.close()
'''
    output = _remote(
        inventory,
        "sh",
        "-c",
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl exec "
        f"-n {KUBERNETES_NAMESPACE} {shlex.quote(pod)} -c ue -- "
        "python3 -c "
        f"{shlex.quote(probe)} {shlex.quote(pdu_address)} "
        f"{shlex.quote(remote_address)} {int(remote_port)}",
        label="Amber PDU-bound central TCP probe",
    ).strip()
    if output not in {"0", "1"}:
        raise ExperimentError("Amber PDU-bound central TCP probe returned invalid data")
    return output == "1"


def wait_pdu_bound_tcp_connected(
    inventory: NetworkInventory,
    pod: str,
    *,
    pdu_address: str,
    remote_address: str,
    remote_port: int,
    timeout_seconds: int = 60,
) -> None:
    """Wait for an exact PDU-sourced TCP path independently of MQTT."""

    deadline = time.monotonic() + timeout_seconds
    latest = "PDU-bound TCP connection not yet observed"
    while time.monotonic() < deadline:
        try:
            if pdu_bound_tcp_connected(
                inventory,
                pod,
                pdu_address=pdu_address,
                remote_address=remote_address,
                remote_port=remote_port,
            ):
                return
            latest = "connect attempt failed"
        except Exception as exc:
            latest = str(exc)
        time.sleep(1)
    raise ExperimentError(
        "Amber PDU-bound TCP path to central MQTT did not connect "
        f"within {timeout_seconds}s ({latest})"
    )
