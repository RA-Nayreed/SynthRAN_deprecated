"""Interactive prompt-toolkit shell for the session-first SynthRAN terminal."""

from __future__ import annotations

from pathlib import Path
import sys
from typing import Callable, Iterable, TextIO

from prompt_toolkit import PromptSession
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.document import Document
from prompt_toolkit.history import InMemoryHistory

from synthran.app.controller import ApplicationController
from synthran.terminal.commands import COMMANDS, command_spec
from synthran.terminal.experiment_setup import ensure_active_experiment
from synthran.terminal.initialize import initialization_root, initialize_from_terminal
from synthran.terminal.router import DispatchResult, TerminalCommandRouter
from synthran.terminal.session import TerminalLine, TerminalSession
from synthran.workspace.model import WorkspaceError
from synthran.workspace.store import workspace_file


class SynthRANCompleter(Completer):
    """Complete only the explicit command registry and its fixed subcommands."""

    def get_completions(self, document: Document, _complete_event):  # type: ignore[override]
        text = document.text_before_cursor.lstrip()
        if not text.startswith("/"):
            return
        if " " not in text:
            for item in COMMANDS:
                if item.name.startswith(text):
                    yield Completion(item.name, start_position=-len(text))
            return

        command_name, remainder = text.split(" ", 1)
        try:
            spec = command_spec(command_name)
        except WorkspaceError:
            return
        if not spec.subcommands or " " in remainder.strip():
            return
        prefix = remainder.strip()
        for value in spec.subcommands:
            if value.startswith(prefix):
                yield Completion(value, start_position=-len(prefix))


def create_prompt_session() -> PromptSession[str]:
    """Create the production prompt session without owning application state."""

    return PromptSession[str](
        history=InMemoryHistory(),
        completer=SynthRANCompleter(),
        complete_while_typing=False,
    )


def _write_lines(lines: Iterable[TerminalLine], output: TextIO) -> None:
    for line in lines:
        print(line.text, file=output, flush=True)


def _toolbar(application: ApplicationController, session: TerminalSession) -> str:
    try:
        snapshot = application.snapshot()
    except WorkspaceError:
        return f" {session.mode.upper()} | workspace unavailable "
    experiment = snapshot.experiment_id or "—"
    return f" {session.mode.upper()} | {snapshot.lifecycle} | {experiment} "


def _prompt(session: TerminalSession) -> str:
    return f"synthran[{session.mode.upper()}]> "


def _open_application(
    *,
    start: Path | None,
    prompt: PromptSession[str],
    output: TextIO,
) -> ApplicationController:
    target = initialization_root(start)
    try:
        return ApplicationController(start=start or Path.cwd())
    except WorkspaceError:
        if workspace_file(target).is_file():
            raise

    initialize_from_terminal(root=target, prompt=prompt, output=output)
    return ApplicationController(start=target)


def run_terminal(
    *,
    start: Path | None = None,
    application: ApplicationController | None = None,
    router: TerminalCommandRouter | None = None,
    prompt_session: PromptSession[str] | None = None,
    output: TextIO | None = None,
    clear_screen: Callable[[], None] | None = None,
) -> int:
    """Run the inline terminal transcript until `/quit` or EOF."""

    stream = output or sys.stdout
    prompt = prompt_session or create_prompt_session()
    injected_application = application is not None
    try:
        app = application or _open_application(
            start=start,
            prompt=prompt,
            output=stream,
        )
        if not injected_application:
            ensure_active_experiment(
                application=app,
                prompt=prompt,
                output=stream,
            )
    except WorkspaceError as exc:
        print(f"error: {exc}", file=stream, flush=True)
        print(
            "Terminal setup did not complete; no provider resource mutation was attempted.",
            file=stream,
            flush=True,
        )
        return 2

    command_router = router or TerminalCommandRouter(app)
    terminal = TerminalSession(app)

    if clear_screen is None:
        from prompt_toolkit.shortcuts import clear

        clear_screen = clear

    print("SynthRAN interactive terminal", file=stream, flush=True)
    print("Mode: OBSERVE  |  /help for commands", file=stream, flush=True)

    while not terminal.closed:
        try:
            line = prompt.prompt(
                _prompt(terminal),
                bottom_toolbar=lambda: _toolbar(app, terminal),
            )
        except KeyboardInterrupt:
            print("^C", file=stream, flush=True)
            continue
        except EOFError:
            print("Session closed", file=stream, flush=True)
            return 0

        response = terminal.submit(line)
        if response.action == "clear":
            clear_screen()
            continue

        _write_lines(response.lines, stream)
        if response.action == "quit":
            return 0
        if response.action != "dispatch":
            continue

        assert response.request is not None
        result: DispatchResult = command_router.dispatch(response.request)
        routed_lines = terminal.record_dispatch_result(
            result.lines,
            error=result.error,
        )
        _write_lines(routed_lines, stream)

        if result.operation_id is not None:
            updates = terminal.operation_updates(result.operation_id)
            _write_lines(updates, stream)

    return 0
