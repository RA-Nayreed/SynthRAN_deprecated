"""Counted TCP ingress for the AMBER-to-RFSIM edge transport."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import selectors
import signal
import socket
import sys
import tempfile
import threading
import time
from typing import Any, Mapping, Sequence


class IngressError(RuntimeError):
    """Raised when the counted AMBER ingress cannot operate safely."""


@dataclass(frozen=True)
class IngressSnapshot:
    accepted_connections: int
    upstream_bytes: int
    downstream_bytes: int

    def to_dict(self) -> dict[str, int]:
        return {
            "accepted_connections": self.accepted_connections,
            "upstream_bytes": self.upstream_bytes,
            "downstream_bytes": self.downstream_bytes,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "IngressSnapshot":
        if not isinstance(data, dict):
            raise IngressError("ingress snapshot is malformed")
        for key in ("accepted_connections", "upstream_bytes", "downstream_bytes"):
            value = data.get(key)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise IngressError(f"ingress snapshot has invalid {key}")
        return cls(
            accepted_connections=int(data["accepted_connections"]),
            upstream_bytes=int(data["upstream_bytes"]),
            downstream_bytes=int(data["downstream_bytes"]),
        )


class CountedTcpIngress:
    """Forward one run-owned TCP listener to the UE-edge broker and count bytes."""

    def __init__(
        self,
        *,
        listen_host: str = "127.0.0.1",
        listen_port: int = 18886,
        target_host: str = "127.0.0.1",
        target_port: int = 18883,
        connect_timeout: float = 10.0,
    ) -> None:
        if not 1 <= listen_port <= 65535 or not 1 <= target_port <= 65535:
            raise IngressError("ingress ports must be between 1 and 65535")
        if connect_timeout <= 0:
            raise IngressError("ingress connect timeout must be positive")
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.target_host = target_host
        self.target_port = target_port
        self.connect_timeout = connect_timeout
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._listener: socket.socket | None = None
        self._lock = threading.Lock()
        self._accepted_connections = 0
        self._upstream_bytes = 0
        self._downstream_bytes = 0
        self._error: BaseException | None = None

    def start(self) -> None:
        if self._thread is not None:
            raise IngressError("ingress is already running")
        try:
            family = socket.AF_INET if ":" not in self.listen_host else socket.AF_INET6
            listener = socket.socket(family, socket.SOCK_STREAM)
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((self.listen_host, self.listen_port))
            listener.listen(128)
            listener.settimeout(0.5)
            self._listener = listener
        except OSError as exc:
            raise IngressError(
                f"unable to bind ingress on {self.listen_host}:{self.listen_port}"
            ) from exc
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._listener is not None:
            try:
                self._listener.close()
            except OSError:
                pass
        if self._thread is not None:
            self._thread.join(timeout=5.0)
        if self._error is not None:
            raise IngressError("ingress failed during execution") from self._error

    def snapshot(self) -> IngressSnapshot:
        with self._lock:
            return IngressSnapshot(
                accepted_connections=self._accepted_connections,
                upstream_bytes=self._upstream_bytes,
                downstream_bytes=self._downstream_bytes,
            )

    def write_snapshot_file(self, destination: Path) -> None:
        destination = destination.resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        content = json.dumps(self.snapshot().to_dict(), indent=2, sort_keys=True) + "\n"
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            dir=destination.parent,
            delete=False,
        ) as temporary:
            temporary.write(content)
            temporary_path = Path(temporary.name)
        temporary_path.replace(destination)

    def _run(self) -> None:
        assert self._listener is not None
        try:
            while not self._stop.is_set():
                try:
                    client, _ = self._listener.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break
                with self._lock:
                    self._accepted_connections += 1
                threading.Thread(
                    target=self._forward_connection,
                    args=(client,),
                    daemon=True,
                ).start()
        except BaseException as exc:
            self._error = exc
            self._stop.set()

    def _forward_connection(self, client: socket.socket) -> None:
        family = socket.AF_INET if ":" not in self.target_host else socket.AF_INET6
        upstream = socket.socket(family, socket.SOCK_STREAM)
        try:
            upstream.settimeout(self.connect_timeout)
            upstream.connect((self.target_host, self.target_port))
            upstream.setblocking(False)
            client.setblocking(False)
            selector = selectors.DefaultSelector()
            selector.register(client, selectors.EVENT_READ, (upstream, self._count_upstream))
            selector.register(upstream, selectors.EVENT_READ, (client, self._count_downstream))
            try:
                while not self._stop.is_set():
                    events = selector.select(timeout=0.5)
                    if not events:
                        continue
                    for key, _ in events:
                        destination, counter = key.data
                        try:
                            chunk = key.fileobj.recv(65536)
                        except BlockingIOError:
                            continue
                        if not chunk:
                            return
                        destination.sendall(chunk)
                        counter(len(chunk))
            finally:
                selector.close()
        except OSError:
            return
        finally:
            for connection in (client, upstream):
                try:
                    connection.close()
                except OSError:
                    pass

    def _count_upstream(self, count: int) -> None:
        with self._lock:
            self._upstream_bytes += count

    def _count_downstream(self, count: int) -> None:
        with self._lock:
            self._downstream_bytes += count


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Counted TCP ingress helper for AMBER RFSIM transport."
    )
    parser.add_argument("--listen-host", default="127.0.0.1")
    parser.add_argument("--listen-port", type=int, default=18886)
    parser.add_argument("--target-host", default="127.0.0.1")
    parser.add_argument("--target-port", type=int, default=18883)
    parser.add_argument("--snapshot-path", type=Path, required=True)
    parser.add_argument("--connect-timeout", type=float, default=10.0)
    parser.add_argument("--poll-interval", type=float, default=0.5)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        ingress = CountedTcpIngress(
            listen_host=args.listen_host,
            listen_port=args.listen_port,
            target_host=args.target_host,
            target_port=args.target_port,
            connect_timeout=args.connect_timeout,
        )
        ingress.start()
    except IngressError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    stop_event = threading.Event()

    def handle_signal(_signum: int, _frame: Any) -> None:
        stop_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    ingress.write_snapshot_file(args.snapshot_path)
    try:
        while not stop_event.is_set():
            time.sleep(args.poll_interval)
            ingress.write_snapshot_file(args.snapshot_path)
    finally:
        try:
            ingress.stop()
        except IngressError:
            pass
        ingress.write_snapshot_file(args.snapshot_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
