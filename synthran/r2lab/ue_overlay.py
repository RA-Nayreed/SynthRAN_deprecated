"""Fail-closed convergence overlay for the pinned R2Lab UE connect role.

The modem mechanics remain owned by the locked ``fiveg_ansible`` dependency.
SynthRAN only corrects the orchestration around the upstream MBIM ``start.sh``:
stop once, then retry the existing start operation while the modem converges.
The overlay is applied to an isolated temporary role copy and never mutates the
locked dependency checkout.
"""

from __future__ import annotations

from pathlib import Path


class R2LabUeOverlayError(RuntimeError):
    """Raised when the pinned UE role no longer matches the reviewed source."""


CONNECT_TASKS = Path("r2lab/ue/connect/tasks/main.yml")

_UPSTREAM_MBIM_BLOCK = '''        - name: "MBIM: stop.sh + start.sh on {{ ue_item }} if wwan0 not reachable"
          shell: >
            ssh -o ConnectTimeout=5 -o BatchMode=yes -o StrictHostKeyChecking=no
            root@{{ ue_item }}
            'stop.sh; start.sh -F {{ current_dnn }}'
          when:
            - ue_mode == 'mbim'
            - current_dnn is defined
            - not wwan0_up
          ignore_errors: "{{ ignore_task_errors | default(true) }}"
'''

_STABLE_MBIM_BLOCK = '''        - name: "MBIM: stop {{ ue_item }} once before reconnect"
          shell: >
            ssh -o ConnectTimeout=5 -o BatchMode=yes -o StrictHostKeyChecking=no
            root@{{ ue_item }}
            'stop.sh'
          when:
            - ue_mode == 'mbim'
            - current_dnn is defined
            - not wwan0_up
          ignore_errors: "{{ ignore_task_errors | default(true) }}"

        - name: "MBIM: start {{ ue_item }} and wait for modem readiness"
          shell: >
            ssh -o ConnectTimeout=5 -o BatchMode=yes -o StrictHostKeyChecking=no
            root@{{ ue_item }}
            'start.sh -F {{ current_dnn }} -q'
          register: mbim_start
          until: mbim_start.rc == 0
          retries: 10
          delay: 3
          when:
            - ue_mode == 'mbim'
            - current_dnn is defined
            - not wwan0_up
          ignore_errors: "{{ ignore_task_errors | default(true) }}"
'''


def apply_ue_connect_overlay(roles_root: Path) -> Path:
    """Patch one isolated upstream connect role with bounded MBIM convergence."""

    path = roles_root.expanduser().resolve() / CONNECT_TASKS
    try:
        source = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise R2LabUeOverlayError("pinned UE connect role is unavailable") from exc

    count = source.count(_UPSTREAM_MBIM_BLOCK)
    if count != 1:
        raise R2LabUeOverlayError(
            "pinned UE connect role drifted from the reviewed MBIM bring-up contract"
        )

    rendered = source.replace(_UPSTREAM_MBIM_BLOCK, _STABLE_MBIM_BLOCK, 1)
    if "stop.sh; start.sh" in rendered or "start.sh -F {{ current_dnn }} -q" not in rendered:
        raise R2LabUeOverlayError("MBIM convergence overlay was not applied exactly")

    try:
        path.write_text(rendered, encoding="utf-8", newline="\n")
    except OSError as exc:
        raise R2LabUeOverlayError("isolated UE connect role could not be written") from exc
    return path
