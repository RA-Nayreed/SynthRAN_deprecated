"""Fail-closed transformations for the pinned fiveg_ansible checkout."""

from __future__ import annotations

from pathlib import Path
import re


class UpstreamOverlayError(RuntimeError):
    """Raised when pinned upstream source no longer matches an expected anchor."""


_SUBSCRIBER_RE = re.compile(r"^[a-z][a-z0-9]{1,31}$")


def _replace_once(root: Path, relative: str, old: str, new: str) -> None:
    path = root / relative
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise UpstreamOverlayError(
            f"unable to read pinned upstream source: {relative}"
        ) from exc
    count = text.count(old)
    if count != 1:
        raise UpstreamOverlayError(
            f"pinned upstream source drifted at {relative}: expected one exact anchor, found {count}"
        )
    try:
        path.write_text(text.replace(old, new, 1), encoding="utf-8", newline="\n")
    except OSError as exc:
        raise UpstreamOverlayError(
            f"unable to write pinned upstream source: {relative}"
        ) from exc


def apply_network_overlay(
    worktree: Path, *, subscriber_name: str = "uesim01"
) -> None:
    """Restrict the pinned upstream deployment to SynthRAN's accepted network path."""

    if not _SUBSCRIBER_RE.fullmatch(subscriber_name):
        raise UpstreamOverlayError("subscriber name is not safe for the pinned template")

    _replace_once(
        worktree,
        "roles/5g/open5gs/config/templates/amf-configmap.yaml.j2",
        "{% for slice in fiveg.slices %}",
        "{% for slice in fiveg.slices | selectattr('name', 'equalto', 'slice1') %}",
    )
    _replace_once(
        worktree,
        "roles/5g/open5gs/config/templates/generate-data-fiveg.py.j2",
        "    {% for s in profile.slices %}",
        "    {% for s in profile.slices | selectattr('name', 'equalto', 'slice1') %}",
    )
    _replace_once(
        worktree,
        "roles/5g/open5gs/config/templates/generate-data-fiveg.py.j2",
        "    {% for ue_name, ue in profile.ues.items() %}",
        "    {% for ue_name, ue in profile.ues.items() if ue_name == '"
        + subscriber_name
        + "' %}",
    )
    _replace_once(
        worktree,
        "roles/5g/open5gs/config/templates/nssf-configmap.yaml.j2",
        "{% for slice in fiveg.slices %}",
        "{% for slice in fiveg.slices | selectattr('name', 'equalto', 'slice1') %}",
    )

    open5gs = "roles/5g/open5gs/deploy/tasks/main.yml"
    guard = "  when: not synthran_golden_path_guard | default(false) | bool\n"
    _replace_once(
        worktree,
        open5gs,
        "  become: true\n  changed_when: false\n\n- name: Restart CoreDNS pods after kubelet restart",
        "  become: true\n  changed_when: false\n"
        + guard
        + "\n- name: Restart CoreDNS pods after kubelet restart",
    )
    _replace_once(
        worktree,
        open5gs,
        "  environment:\n    KUBECONFIG: /etc/kubernetes/admin.conf\n  changed_when: false\n\n- name: Wait for CoreDNS to be Ready",
        "  environment:\n    KUBECONFIG: /etc/kubernetes/admin.conf\n  changed_when: false\n"
        + guard
        + "\n- name: Wait for CoreDNS to be Ready",
    )
    _replace_once(
        worktree,
        open5gs,
        "  until: wait_coredns.rc == 0\n  changed_when: false\n\n- name: Test CoreDNS resolution from core node",
        "  until: wait_coredns.rc == 0\n  changed_when: false\n"
        + guard
        + "\n- name: Test CoreDNS resolution from core node",
    )
    _replace_once(
        worktree,
        open5gs,
        "    kubectl delete pod dns-test-{{ groups['core_node'][0] }} --ignore-not-found\n  changed_when: false\n\n- name: Ensure Open5GS namespace exists",
        "    kubectl delete pod dns-test-{{ groups['core_node'][0] }} --ignore-not-found\n  changed_when: false\n"
        + guard
        + "\n- name: Ensure Open5GS namespace exists",
    )
    _replace_once(
        worktree,
        open5gs,
        "- name: Install jq\n  ansible.builtin.apt:\n    name: jq\n    state: present\n    update_cache: yes\n  become: yes\n",
        "- name: Install jq\n  ansible.builtin.apt:\n    name: jq\n    state: present\n    update_cache: yes\n  become: yes\n"
        + guard,
    )

    for old, new in (
        (
            "- name: Install python3-venv, python3-pip, setuptools and wheel\n"
            "  ansible.builtin.apt:\n"
            "    name:\n"
            "      - python3-venv\n"
            "      - python3-pip\n"
            "      - python3-setuptools\n"
            "      - python3-wheel\n"
            "    state: present\n"
            "  become: yes\n",
            "- name: Install python3-venv, python3-pip, setuptools and wheel\n"
            "  ansible.builtin.apt:\n"
            "    name:\n"
            "      - python3-venv\n"
            "      - python3-pip\n"
            "      - python3-setuptools\n"
            "      - python3-wheel\n"
            "    state: present\n"
            "  become: yes\n"
            + guard,
        ),
        (
            "- name: Create Python virtual environment (idempotent)\n"
            "  ansible.builtin.command:\n"
            "    cmd: python3 -m venv \"{{ repo_dest }}/venv\"\n"
            "    creates: \"{{ repo_dest }}/venv/bin/activate\"\n"
            "  become: yes\n",
            "- name: Create Python virtual environment (idempotent)\n"
            "  ansible.builtin.command:\n"
            "    cmd: python3 -m venv \"{{ repo_dest }}/venv\"\n"
            "    creates: \"{{ repo_dest }}/venv/bin/activate\"\n"
            "  become: yes\n"
            + guard,
        ),
        (
            "- name: Bootstrap pip, setuptools and wheel in venv\n"
            "  ansible.builtin.command:\n"
            "    cmd: \"{{ repo_dest }}/venv/bin/python -m ensurepip --upgrade\"\n"
            "  become: yes\n",
            "- name: Bootstrap pip, setuptools and wheel in venv\n"
            "  ansible.builtin.command:\n"
            "    cmd: \"{{ repo_dest }}/venv/bin/python -m ensurepip --upgrade\"\n"
            "  become: yes\n"
            + guard,
        ),
        (
            "- name: Upgrade pip, setuptools and wheel explicitly in venv\n"
            "  ansible.builtin.command:\n"
            "    cmd: \"{{ repo_dest }}/venv/bin/pip install --upgrade pip setuptools wheel\"\n"
            "  become: yes\n",
            "- name: Upgrade pip, setuptools and wheel explicitly in venv\n"
            "  ansible.builtin.command:\n"
            "    cmd: \"{{ repo_dest }}/venv/bin/pip install --upgrade pip setuptools wheel\"\n"
            "  become: yes\n"
            + guard,
        ),
        (
            "- name: Install Python requirements in venv\n"
            "  ansible.builtin.command:\n"
            "    cmd: \"{{ repo_dest }}/venv/bin/pip install -r {{ repo_dest }}/requirements.txt\"\n"
            "  become: yes\n",
            "- name: Install Python requirements in venv\n"
            "  ansible.builtin.command:\n"
            "    cmd: \"{{ repo_dest }}/venv/bin/pip install -r {{ repo_dest }}/requirements.txt\"\n"
            "  become: yes\n"
            + guard,
        ),
    ):
        _replace_once(worktree, open5gs, old, new)

    _replace_once(
        worktree,
        open5gs,
        "- name: Deploy Open5GS Web UI\n"
        "  ansible.builtin.command: kubectl apply -k \"{{ repo_dest }}/open5gs-webui\" -n {{ open5gs_ns }}\n\n"
        "- name: Wait for WebUI pod Ready\n"
        "  kubernetes.core.k8s_info:\n"
        "    kind: Pod\n"
        "    namespace: \"{{ open5gs_ns }}\"\n"
        "    label_selectors:\n"
        "      - \"nf=webui\"\n"
        "  register: webui_pod\n"
        "  until: >\n"
        "    webui_pod.resources | length > 0 and\n"
        "    (webui_pod.resources[0].status.containerStatuses[0].ready | default(false))\n"
        "  retries: 30\n"
        "  delay: 5\n"
        "  failed_when: false\n"
        "  changed_when: false\n\n"
        "- name: Warn if WebUI not ready\n"
        "  ansible.builtin.debug:\n"
        "    msg: \"WebUI pod not Ready — continuing (optional component)\"\n"
        "  when: webui_pod is failed or\n"
        "        webui_pod.resources | length == 0 or\n"
        "        not (webui_pod.resources[0].status.containerStatuses[0].ready | default(false))\n\n",
        "",
    )
    _replace_once(
        worktree,
        open5gs,
        "  ansible.builtin.shell: |\n"
        "    {{ repo_dest }}/venv/bin/python {{ repo_dest }}/mongo-tools/generate-data-fiveg.py\n"
        "    {{ repo_dest }}/venv/bin/python {{ repo_dest }}/mongo-tools/add-subscribers.py\n"
        "  args:\n"
        "    chdir: \"{{ repo_dest }}\"\n\n"
        "- name: Run add-admin-account.py\n"
        "  ansible.builtin.command: >\n"
        "    {{ repo_dest }}/venv/bin/python mongo-tools/add-admin-account.py\n"
        "  args:\n"
        "    chdir: \"{{ repo_dest }}\"\n\n"
        "- name: Check Open5GS deployment\n"
        "  ansible.builtin.include_role:\n"
        "    name: 5g/open5gs/check_all",
        "  ansible.builtin.shell: |\n"
        "    /opt/synthran-venv/bin/python {{ repo_dest }}/mongo-tools/generate-data-fiveg.py\n"
        "    /opt/synthran-venv/bin/python {{ repo_dest }}/mongo-tools/add-subscribers.py\n"
        "  args:\n"
        "    chdir: \"{{ repo_dest }}\"\n"
        "  no_log: true",
    )

    profile = "roles/5g/srsRAN/config/tasks/apply_5g_profile.yaml"
    _replace_once(
        worktree,
        profile,
        "- name: Download yq binary using curl or wget\n"
        "  ansible.builtin.shell: |\n"
        "    set -e\n"
        "    if command -v curl >/dev/null 2>&1; then\n"
        "      curl -L --fail \"{{ yq_url }}\" -o \"{{ yq_bin_path }}\"\n"
        "    elif command -v wget >/dev/null 2>&1; then\n"
        "      wget -O \"{{ yq_bin_path }}\" \"{{ yq_url }}\"\n"
        "    else\n"
        "      echo \"Neither curl nor wget found\" >&2\n"
        "      exit 1\n"
        "    fi\n"
        "  args:\n"
        "    creates: \"{{ yq_bin_path }}\"\n"
        "  when: not yq_bin.stat.exists\n"
        "  become: true",
        "- name: Refuse an unprepared yq dependency\n"
        "  ansible.builtin.fail:\n"
        "    msg: \"the digest-locked yq binary required by SynthRAN is unavailable\"\n"
        "  when: not yq_bin.stat.exists",
    )
    _replace_once(
        worktree,
        profile,
        '  loop: "{{ fiveg.slices }}"',
        '  loop: "{{ fiveg.slices | selectattr(\'name\', \'equalto\', \'slice1\') | list }}"',
    )

    srs_config = "roles/5g/srsRAN/config/tasks/main.yml"
    _replace_once(
        worktree,
        srs_config,
        "- name: Ensure kubernetes python client is installed\n"
        "  ansible.builtin.package:\n"
        "    name: python3-kubernetes\n"
        "    state: present\n"
        "  become: yes",
        "- name: Ensure kubernetes python client is installed\n"
        "  ansible.builtin.command:\n"
        "    argv:\n"
        "      - /opt/synthran-venv/bin/python\n"
        "      - -c\n"
        "      - import kubernetes\n"
        "  changed_when: false",
    )
    _replace_once(
        worktree,
        srs_config,
        "    name: \"{{ ran_ns }}\"\n"
        "    state: present\n"
        "  vars:\n"
        "    ansible_python_interpreter: \"{{ ansible_playbook_python }}\"",
        "    name: \"{{ ran_ns }}\"\n    state: present",
    )

    _replace_once(
        worktree,
        "roles/5g/srsRAN/deploy/tasks/deploy_gnb.yml",
        "- name: Install Helm if missing\n"
        "  ansible.builtin.shell: |\n"
        "    set -e\n"
        "    curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash\n"
        "  when: helm_check.rc != 0\n"
        "  args:\n"
        "    executable: /bin/bash",
        "- name: Refuse an unprepared Helm dependency\n"
        "  ansible.builtin.fail:\n"
        "    msg: \"Helm v3 must pass SynthRAN live preflight before deployment\"\n"
        "  when: helm_check.rc != 0",
    )


def apply_preparation_overlay(worktree: Path) -> None:
    """Keep provider ownership in SynthRAN and stop upstream before 5G deployment."""

    _replace_once(
        worktree,
        "roles/pos/tasks/main.yml",
        "- name: (POS) Free any existing allocation for {{ node }}\n"
        "  shell: pos allocations free -k \"{{ node }}\"\n\n"
        "- name: (POS) Allocate {{ node }}\n"
        "  shell: pos allocations allocate \"{{ node }}\"\n\n",
        "",
    )
    deploy = "playbooks/deploy.yml"
    _replace_once(
        worktree,
        deploy,
        "    - role: setup/k8s/k8s_env\n    - role: setup/optimization/cpu",
        "    - role: setup/k8s/k8s_env\n"
        "      when: not (synthran_prepare_only | default(false) | bool)\n"
        "    - role: setup/optimization/cpu",
    )
    _replace_once(
        worktree,
        deploy,
        "    - role: 5g/open5gs/config\n"
        "      when: core == 'open5gs'\n"
        "    - role: 5g/open5gs/deploy\n"
        "      when: core == 'open5gs'",
        "    - role: 5g/open5gs/config\n"
        "      when: core == 'open5gs' and not (synthran_prepare_only | default(false) | bool)\n"
        "    - role: 5g/open5gs/deploy\n"
        "      when: core == 'open5gs' and not (synthran_prepare_only | default(false) | bool)",
    )
    _replace_once(
        worktree,
        deploy,
        "    - role: 5g/srsRAN/config\n"
        "      when: ran == 'srsRAN'\n"
        "    - role: 5g/srsRAN/deploy\n"
        "      when: ran == 'srsRAN'\n"
        "    - role: 5g/srsRAN/csi\n"
        "      when: \n"
        "        - ran == 'srsRAN'\n"
        "        - csi_logger_enabled | default(false)",
        "    - role: 5g/srsRAN/config\n"
        "      when: ran == 'srsRAN' and not (synthran_prepare_only | default(false) | bool)\n"
        "    - role: 5g/srsRAN/deploy\n"
        "      when: ran == 'srsRAN' and not (synthran_prepare_only | default(false) | bool)\n"
        "    - role: 5g/srsRAN/csi\n"
        "      when: \n"
        "        - ran == 'srsRAN'\n"
        "        - csi_logger_enabled | default(false)\n"
        "        - not (synthran_prepare_only | default(false) | bool)",
    )
