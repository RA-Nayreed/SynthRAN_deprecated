"""Atomic ID allocation and rebuildable workspace index."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import re
from pathlib import Path
import sqlite3

from synthran.workspace.model import (
    ExperimentRecord,
    ExperimentStatus,
    OperationRecord,
    RunRecord,
    WorkspaceError,
    format_utc,
    parse_operation_id,
    parse_run_id,
    utc_now,
    validate_experiment_id,
)
from synthran.workspace.records import (
    load_operation_record,
    load_run_record,
    operation_directory,
    run_directory,
    save_operation_record,
    save_run_record,
)
from synthran.workspace.status import load_experiment_status, save_experiment_status
from synthran.workspace.store import (
    experiment_directory,
    load_experiment_record,
    save_experiment_record,
    set_active_experiment,
    workspace_directory,
)


REGISTRY_SCHEMA = 1
RUN_LABEL_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,47}$")


class WorkspaceRegistry:
    """SQLite index whose research records remain recoverable from durable folders."""

    def __init__(self, workspace_root: Path):
        self.workspace_root = workspace_root.resolve()
        self.path = workspace_directory(self.workspace_root) / "registry.sqlite3"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @contextmanager
    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        try:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA journal_mode = WAL")
            yield connection
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS counters (
                    key TEXT PRIMARY KEY,
                    value INTEGER NOT NULL CHECK(value >= 0)
                );
                CREATE TABLE IF NOT EXISTS experiments (
                    experiment_id TEXT PRIMARY KEY,
                    created_at_utc TEXT NOT NULL,
                    status TEXT NOT NULL,
                    path TEXT NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS runs (
                    experiment_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL CHECK(ordinal > 0),
                    label TEXT,
                    created_at_utc TEXT NOT NULL,
                    PRIMARY KEY (experiment_id, run_id),
                    UNIQUE (experiment_id, ordinal),
                    FOREIGN KEY (experiment_id) REFERENCES experiments(experiment_id)
                );
                CREATE TABLE IF NOT EXISTS operations (
                    operation_id TEXT PRIMARY KEY,
                    ordinal INTEGER NOT NULL UNIQUE CHECK(ordinal > 0),
                    experiment_id TEXT,
                    kind TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL,
                    FOREIGN KEY (experiment_id) REFERENCES experiments(experiment_id)
                );
                """
            )
            row = connection.execute(
                "SELECT value FROM metadata WHERE key = 'schema'"
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO metadata(key, value) VALUES('schema', ?)",
                    (str(REGISTRY_SCHEMA),),
                )
            elif row[0] != str(REGISTRY_SCHEMA):
                raise WorkspaceError("workspace registry schema is unsupported")

    @staticmethod
    def _next_counter(connection: sqlite3.Connection, key: str) -> int:
        row = connection.execute(
            "SELECT value FROM counters WHERE key = ?", (key,)
        ).fetchone()
        value = (int(row[0]) if row else 0) + 1
        connection.execute(
            "INSERT INTO counters(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        return value

    @staticmethod
    def _set_counter_max(connection: sqlite3.Connection, key: str, value: int) -> None:
        row = connection.execute(
            "SELECT value FROM counters WHERE key = ?", (key,)
        ).fetchone()
        if row is None or int(row[0]) < value:
            connection.execute(
                "INSERT INTO counters(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    def issue_experiment_id(self, now: datetime | None = None) -> str:
        current = (now or utc_now()).astimezone(timezone.utc)
        date_stamp = current.strftime("%Y%m%d")
        created = format_utc(current)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            ordinal = self._next_counter(connection, f"experiment:{date_stamp}")
            experiment_id = f"sran-{date_stamp}-{ordinal:03d}"
            validate_experiment_id(experiment_id)
            relative = f"experiments/{experiment_id}"
            connection.execute(
                "INSERT INTO experiments(experiment_id, created_at_utc, status, path) "
                "VALUES(?, ?, 'issued', ?)",
                (experiment_id, created, relative),
            )
            connection.execute("COMMIT")
        directory = experiment_directory(self.workspace_root, experiment_id)
        try:
            directory.mkdir(parents=True, exist_ok=False)
        except FileExistsError as exc:
            self.mark_experiment_status(experiment_id, "failed")
            raise WorkspaceError(
                f"issued experiment directory already exists for {experiment_id}; ID remains consumed"
            ) from exc
        except OSError as exc:
            self.mark_experiment_status(experiment_id, "failed")
            raise WorkspaceError(
                f"unable to create experiment directory for {experiment_id}; ID remains consumed in the registry"
            ) from exc
        return experiment_id

    def create_experiment(
        self,
        *,
        profile: str,
        project: str,
        label: str | None = None,
        slices_experiment: str | None = None,
        network_intent: str = "unspecified",
        radio_mode: str = "automatic",
        now: datetime | None = None,
        activate: bool = True,
    ) -> ExperimentRecord:
        current = (now or utc_now()).astimezone(timezone.utc)
        experiment_id = self.issue_experiment_id(current)
        record = ExperimentRecord(
            experiment_id=experiment_id,
            created_at_utc=format_utc(current),
            profile=profile,
            project=project,
            label=label,
            slices_experiment=slices_experiment,
            network_intent=network_intent,
            radio_mode=radio_mode,
        )
        directory = experiment_directory(self.workspace_root, experiment_id)
        try:
            for name in ("providers", "operations", "runs", "evidence", "datasets"):
                (directory / name).mkdir(exist_ok=False)
            save_experiment_record(self.workspace_root, record)
            save_experiment_status(
                self.workspace_root,
                ExperimentStatus(
                    experiment_id=experiment_id,
                    state="configured",
                    updated_at_utc=format_utc(current),
                ),
            )
            self.mark_experiment_status(experiment_id, "configured")
            if activate:
                set_active_experiment(self.workspace_root, experiment_id)
        except Exception as exc:
            self.mark_experiment_status(experiment_id, "failed")
            try:
                if (directory / "experiment.toml").is_file():
                    save_experiment_status(
                        self.workspace_root,
                        ExperimentStatus(
                            experiment_id=experiment_id,
                            state="failed",
                            updated_at_utc=format_utc(utc_now()),
                            notes=("experiment initialization did not complete",),
                        ),
                    )
            except Exception:
                pass
            if isinstance(exc, WorkspaceError):
                raise
            raise WorkspaceError(
                f"experiment {experiment_id} could not be initialized; its ID remains consumed"
            ) from exc
        return record

    def mark_experiment_status(self, experiment_id: str, status: str) -> None:
        validate_experiment_id(experiment_id)
        if status not in {"issued", "configured", "active", "expired", "closed", "failed"}:
            raise WorkspaceError("registry experiment status is unsupported")
        with self._connect() as connection:
            updated = connection.execute(
                "UPDATE experiments SET status = ? WHERE experiment_id = ?",
                (status, experiment_id),
            ).rowcount
        if updated != 1:
            raise WorkspaceError(f"experiment {experiment_id} is not indexed")

    def issue_run_id(
        self,
        *,
        experiment_id: str,
        label: str | None = None,
        now: datetime | None = None,
    ) -> str:
        validate_experiment_id(experiment_id)
        if label is not None and not RUN_LABEL_RE.fullmatch(label):
            raise WorkspaceError(
                "run label must start with a lowercase letter or number and contain only lowercase letters, numbers, or '-'"
            )
        current = (now or utc_now()).astimezone(timezone.utc)
        created = format_utc(current)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            exists = connection.execute(
                "SELECT 1 FROM experiments WHERE experiment_id = ?", (experiment_id,)
            ).fetchone()
            if exists is None:
                connection.execute("ROLLBACK")
                raise WorkspaceError(f"experiment {experiment_id} is not indexed")
            ordinal = self._next_counter(connection, f"run:{experiment_id}")
            run_id = f"run-{ordinal:03d}" + (f"-{label}" if label else "")
            connection.execute(
                "INSERT INTO runs(experiment_id, run_id, ordinal, label, created_at_utc) "
                "VALUES(?, ?, ?, ?, ?)",
                (experiment_id, run_id, ordinal, label, created),
            )
            connection.execute("COMMIT")
        directory = run_directory(self.workspace_root, experiment_id, run_id)
        try:
            directory.mkdir(parents=True, exist_ok=False)
            save_run_record(
                self.workspace_root,
                RunRecord(
                    experiment_id=experiment_id,
                    run_id=run_id,
                    ordinal=ordinal,
                    label=label,
                    created_at_utc=created,
                ),
            )
        except OSError as exc:
            raise WorkspaceError(
                f"unable to create durable run record for {run_id}; ID remains consumed"
            ) from exc
        except WorkspaceError:
            raise
        return run_id

    def issue_operation_id(
        self,
        *,
        kind: str,
        experiment_id: str | None = None,
        now: datetime | None = None,
    ) -> str:
        current = (now or utc_now()).astimezone(timezone.utc)
        created = format_utc(current)
        record_template = OperationRecord(
            operation_id="op-000001",
            ordinal=1,
            kind=kind,
            experiment_id=experiment_id,
            created_at_utc=created,
        )
        if experiment_id is not None:
            validate_experiment_id(experiment_id)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if experiment_id is not None:
                exists = connection.execute(
                    "SELECT 1 FROM experiments WHERE experiment_id = ?", (experiment_id,)
                ).fetchone()
                if exists is None:
                    connection.execute("ROLLBACK")
                    raise WorkspaceError(f"experiment {experiment_id} is not indexed")
            ordinal = self._next_counter(connection, "operation")
            operation_id = f"op-{ordinal:06d}"
            connection.execute(
                "INSERT INTO operations(operation_id, ordinal, experiment_id, kind, created_at_utc) "
                "VALUES(?, ?, ?, ?, ?)",
                (operation_id, ordinal, experiment_id, record_template.kind, created),
            )
            connection.execute("COMMIT")
        directory = operation_directory(self.workspace_root, operation_id)
        try:
            directory.mkdir(parents=True, exist_ok=False)
            save_operation_record(
                self.workspace_root,
                OperationRecord(
                    operation_id=operation_id,
                    ordinal=ordinal,
                    kind=record_template.kind,
                    experiment_id=experiment_id,
                    created_at_utc=created,
                ),
            )
        except OSError as exc:
            raise WorkspaceError(
                f"unable to create durable operation record for {operation_id}; ID remains consumed"
            ) from exc
        except WorkspaceError:
            raise
        return operation_id

    def rebuild_from_experiment_folders(self) -> int:
        """Rebuild indexes and all non-reuse counters from durable workspace folders."""

        workspace = workspace_directory(self.workspace_root)
        experiment_root = workspace / "experiments"
        operation_root = workspace / "operations"
        experiment_root.mkdir(parents=True, exist_ok=True)
        operation_root.mkdir(parents=True, exist_ok=True)

        experiments: list[tuple[str, ExperimentRecord | None, str]] = []
        run_records: list[RunRecord] = []
        run_maxima: dict[str, int] = {}
        for directory in sorted(path for path in experiment_root.iterdir() if path.is_dir()):
            try:
                validate_experiment_id(directory.name)
            except WorkspaceError:
                continue
            record_path = directory / "experiment.toml"
            record = (
                load_experiment_record(self.workspace_root, directory.name)
                if record_path.is_file()
                else None
            )
            status = "failed"
            if record is not None:
                status = "configured"
                try:
                    status = load_experiment_status(
                        self.workspace_root, directory.name
                    ).state
                except WorkspaceError:
                    pass
            experiments.append((directory.name, record, status))

            runs_root = directory / "runs"
            if not runs_root.is_dir():
                continue
            maximum = 0
            for child in sorted(path for path in runs_root.iterdir() if path.is_dir()):
                try:
                    ordinal, _ = parse_run_id(child.name)
                except WorkspaceError:
                    continue
                maximum = max(maximum, ordinal)
                if (child / "run.json").is_file():
                    run_records.append(
                        load_run_record(self.workspace_root, directory.name, child.name)
                    )
            if maximum:
                run_maxima[directory.name] = maximum

        operation_records: list[OperationRecord] = []
        operation_maximum = 0
        for directory in sorted(path for path in operation_root.iterdir() if path.is_dir()):
            try:
                ordinal = parse_operation_id(directory.name)
            except WorkspaceError:
                continue
            operation_maximum = max(operation_maximum, ordinal)
            if (directory / "operation.json").is_file():
                operation_records.append(
                    load_operation_record(self.workspace_root, directory.name)
                )

        experiment_ids = {experiment_id for experiment_id, _, _ in experiments}
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("DELETE FROM runs")
            connection.execute("DELETE FROM operations")
            connection.execute("DELETE FROM experiments")
            connection.execute("DELETE FROM counters")

            for experiment_id, record, status in experiments:
                date_stamp, ordinal_text = experiment_id.split("-")[1:]
                ordinal = int(ordinal_text)
                self._set_counter_max(
                    connection, f"experiment:{date_stamp}", ordinal
                )
                created_at = (
                    record.created_at_utc
                    if record is not None
                    else f"{date_stamp[0:4]}-{date_stamp[4:6]}-{date_stamp[6:8]}T00:00:00Z"
                )
                connection.execute(
                    "INSERT INTO experiments(experiment_id, created_at_utc, status, path) "
                    "VALUES(?, ?, ?, ?)",
                    (
                        experiment_id,
                        created_at,
                        status,
                        f"experiments/{experiment_id}",
                    ),
                )

            for experiment_id, maximum in run_maxima.items():
                self._set_counter_max(connection, f"run:{experiment_id}", maximum)
            for record in run_records:
                if record.experiment_id not in experiment_ids:
                    connection.execute("ROLLBACK")
                    raise WorkspaceError(
                        f"run {record.run_id} references an unknown experiment"
                    )
                connection.execute(
                    "INSERT INTO runs(experiment_id, run_id, ordinal, label, created_at_utc) "
                    "VALUES(?, ?, ?, ?, ?)",
                    (
                        record.experiment_id,
                        record.run_id,
                        record.ordinal,
                        record.label,
                        record.created_at_utc,
                    ),
                )

            if operation_maximum:
                self._set_counter_max(connection, "operation", operation_maximum)
            for record in operation_records:
                if (
                    record.experiment_id is not None
                    and record.experiment_id not in experiment_ids
                ):
                    connection.execute("ROLLBACK")
                    raise WorkspaceError(
                        f"operation {record.operation_id} references an unknown experiment"
                    )
                connection.execute(
                    "INSERT INTO operations(operation_id, ordinal, experiment_id, kind, created_at_utc) "
                    "VALUES(?, ?, ?, ?, ?)",
                    (
                        record.operation_id,
                        record.ordinal,
                        record.experiment_id,
                        record.kind,
                        record.created_at_utc,
                    ),
                )
            connection.execute("COMMIT")
        return len(experiments)
