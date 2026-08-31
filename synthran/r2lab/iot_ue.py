"""Selected-UE primitives for the physical Amber transport."""

from __future__ import annotations

import ipaddress
import json
import socketserver
import subprocess
import threading
from typing import Sequence

from synthran.experiment import ExperimentError, validate_run_id
from synthran.experiment.live import _run
from synthran.experiment.resources import CENTRAL_PORT
from synthran.live_preflight import CommandResult
from synthran.r2lab.hardware import UES, UeProfile
from synthran.r2lab.resources import ue_gateway_command


LOCAL_UE_RELAY_PORT = 18887
UE_INTERFACE = "wwan0"
_RELAY_MARKER = "SYNTHRAN_R2LAB_RELAY"


class R2LabIoTUeError(ExperimentError):
    """Raised when the selected physical UE cannot satisfy the IoT boundary."""


def _validate_ue(ue: str) -> UeProfile:
    value = ue.strip().lower()
    profile = UES.get(value)
    if profile is None or not profile.executable or not profile.is_fr1_quectel:
        raise R2LabIoTUeError(
            "physical workload requires one executable FR1 Quectel UE"
        )
    if profile.data_interface != UE_INTERFACE:
        raise R2LabIoTUeError("selected physical UE does not expose wwan0")
    return profile


_RELAY_SCRIPT = r'''
import os, select, socket, sys
MARKER = "SYNTHRAN_R2LAB_RELAY"
run_id, host, port_text, interface = sys.argv[1:5]
port = int(port_text)
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_BINDTODEVICE, (interface + "\0").encode("ascii"))
sock.settimeout(10)
sock.connect((host, port))
sock.settimeout(None)
stdin_open = True
while True:
    readers = [sock]
    if stdin_open:
        readers.append(0)
    ready, _, _ = select.select(readers, [], [])
    if stdin_open and 0 in ready:
        data = os.read(0, 65536)
        if data:
            sock.sendall(data)
        else:
            stdin_open = False
            try:
                sock.shutdown(socket.SHUT_WR)
            except OSError:
                pass
    if sock in ready:
        data = sock.recv(65536)
        if not data:
            break
        os.write(1, data)
sock.close()
'''.strip()


def build_physical_ue_stdio_relay_command(
    *,
    slice_name: str,
    ue: str,
    run_id: str,
    central_address: str,
    central_port: int = CENTRAL_PORT,
    interface: str = UE_INTERFACE,
) -> tuple[str, ...]:
    """Build one strict SSH command whose remote socket is bound to wwan0."""

    validate_run_id(run_id)
    profile = _validate_ue(ue)
    try:
        address = ipaddress.ip_address(central_address)
    except ValueError as exc:
        raise R2LabIoTUeError("central broker address must be a literal IP") from exc
    if not isinstance(address, ipaddress.IPv4Address):
        raise R2LabIoTUeError("current physical workload is IPv4-only")
    if not 1 <= central_port <= 65535:
        raise R2LabIoTUeError("central broker port is invalid")
    if interface != UE_INTERFACE:
        raise R2LabIoTUeError("physical relay must bind to wwan0")
    return ue_gateway_command(
        slice_name,
        profile,
        "python3",
        "-c",
        _RELAY_SCRIPT,
        run_id,
        str(address),
        str(central_port),
        interface,
    )


def route_uses_wwan0(text: str, destination: str) -> bool:
    """Accept only an exact JSON route observation through wwan0."""

    try:
        wanted = str(ipaddress.ip_address(destination))
        payload = json.loads(text)
    except (ValueError, json.JSONDecodeError):
        return False
    if not isinstance(payload, list) or not payload:
        return False
    return any(
        isinstance(item, dict)
        and item.get("dev") == UE_INTERFACE
        and str(item.get("dst", wanted)) == wanted
        for item in payload
    )


def _ue_read(
    *,
    slice_name: str,
    profile: UeProfile,
    command: Sequence[str],
    label: str,
    timeout_seconds: int = 30,
) -> CommandResult:
    result = _run(
        ue_gateway_command(slice_name, profile, *tuple(command)),
        timeout_seconds=timeout_seconds,
    )
    if result.returncode != 0:
        raise R2LabIoTUeError(f"{label} returned nonzero")
    return result


def _ue_counter(slice_name: str, profile: UeProfile, counter: str) -> int:
    if counter not in {"rx_bytes", "tx_bytes"}:
        raise R2LabIoTUeError("unsupported physical UE interface counter")
    result = _ue_read(
        slice_name=slice_name,
        profile=profile,
        command=("cat", f"/sys/class/net/{UE_INTERFACE}/statistics/{counter}"),
        label=f"physical UE {counter} counter probe",
    )
    value = result.stdout.strip()
    if not value.isdigit():
        raise R2LabIoTUeError(
            "physical UE interface counter returned malformed data"
        )
    return int(value)


def _prove_ue_route(
    slice_name: str,
    profile: UeProfile,
    central_address: str,
) -> None:
    route = _ue_read(
        slice_name=slice_name,
        profile=profile,
        command=(
            "ip",
            "-j",
            "route",
            "get",
            central_address,
            "oif",
            UE_INTERFACE,
        ),
        label="physical UE interface-bound route probe",
    )
    if not route_uses_wwan0(route.stdout, central_address):
        raise R2LabIoTUeError(
            "central MQTT destination is not selectable through wwan0"
        )


def _ue_relay_process_count(
    slice_name: str,
    profile: UeProfile,
    run_id: str,
) -> int:
    validate_run_id(run_id)
    probe = r'''
import os, sys
marker, run_id = sys.argv[1:3]
self_pid = os.getpid()
count = 0
for entry in os.listdir('/proc'):
    if not entry.isdigit() or int(entry) == self_pid:
        continue
    try:
        raw = open(f'/proc/{entry}/cmdline', 'rb').read()
    except (FileNotFoundError, PermissionError, OSError):
        continue
    text = raw.replace(b'\0', b' ').decode('utf-8', 'replace')
    if marker in text and run_id in text:
        count += 1
print(count)
'''.strip()
    result = _ue_read(
        slice_name=slice_name,
        profile=profile,
        command=("python3", "-c", probe, _RELAY_MARKER, run_id),
        label="physical UE relay cleanup probe",
    )
    value = result.stdout.strip()
    if not value.isdigit():
        raise R2LabIoTUeError(
            "physical UE relay cleanup probe returned malformed data"
        )
    return int(value)


def _stop_relay_process(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        try:
            process.terminate()
            process.wait(timeout=3)
        except (OSError, subprocess.TimeoutExpired):
            try:
                process.kill()
                process.wait(timeout=3)
            except (OSError, subprocess.TimeoutExpired):
                pass
    for stream in (process.stdin, process.stdout):
        if stream is not None and not stream.closed:
            stream.close()


class _RelayTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = False
    daemon_threads = True

    def __init__(self, address: tuple[str, int], command: tuple[str, ...]) -> None:
        self.command = command
        self.children: set[subprocess.Popen[bytes]] = set()
        self.children_lock = threading.Lock()
        super().__init__(address, _RelayHandler)

    def register_child(self, process: subprocess.Popen[bytes]) -> None:
        with self.children_lock:
            self.children.add(process)

    def unregister_child(self, process: subprocess.Popen[bytes]) -> None:
        with self.children_lock:
            self.children.discard(process)

    def terminate_children(self) -> None:
        with self.children_lock:
            children = list(self.children)
        for process in children:
            _stop_relay_process(process)


class _RelayHandler(socketserver.BaseRequestHandler):
    server: _RelayTCPServer

    def handle(self) -> None:
        try:
            process = subprocess.Popen(
                self.server.command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                bufsize=0,
                start_new_session=True,
            )
        except (FileNotFoundError, OSError):
            return
        self.server.register_child(process)
        assert process.stdin is not None
        assert process.stdout is not None

        def to_process() -> None:
            try:
                while data := self.request.recv(65536):
                    process.stdin.write(data)
                    process.stdin.flush()
            except (BrokenPipeError, ConnectionError, OSError):
                pass
            finally:
                try:
                    process.stdin.close()
                except OSError:
                    pass

        def to_client() -> None:
            try:
                while data := process.stdout.read(65536):
                    self.request.sendall(data)
            except (BrokenPipeError, ConnectionError, OSError):
                pass
            finally:
                try:
                    self.request.shutdown(1)
                except OSError:
                    pass

        outgoing = threading.Thread(target=to_process, daemon=True)
        incoming = threading.Thread(target=to_client, daemon=True)
        outgoing.start()
        incoming.start()
        outgoing.join()
        incoming.join()
        _stop_relay_process(process)
        self.server.unregister_child(process)


class ManagedPhysicalUeRelay:
    """Local TCP endpoint creating one selected-UE stdio relay per client."""

    def __init__(self, *, port: int, command: tuple[str, ...]) -> None:
        self.server = _RelayTCPServer(("127.0.0.1", port), command)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def port(self) -> int:
        return int(self.server.server_address[1])

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.server.terminate_children()
        self.thread.join(timeout=5)
