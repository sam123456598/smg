"""Who used CPU on the measurement cores around a trial (guardrail 5's foreign-load check).

Two snapshots of every thread's CPU ticks and last CPU, `seconds` apart, give each process's CPU
share on the given cores. A process above the threshold that is neither part of the trial nor on
the allow list makes the trial foreign-loaded; allow-listed processes are recorded as background.
"""

from __future__ import annotations

import os
import re
import time

CLK_TCK = os.sysconf("SC_CLK_TCK")


def parse_cpu_list(spec: str) -> set[int]:
    """ "0-3,8" -> {0, 1, 2, 3, 8}."""
    cpus: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            lo, hi = part.split("-", 1)
            cpus.update(range(int(lo), int(hi) + 1))
        else:
            cpus.add(int(part))
    return cpus


def _task_ticks(pid: str, tid: str) -> tuple[int, int] | None:
    """(utime + stime in ticks, last CPU) of one thread, or None if it is gone."""
    try:
        with open(f"/proc/{pid}/task/{tid}/stat", "rb") as handle:
            raw = handle.read()
    except OSError:
        return None
    # The command name may contain spaces and parentheses; fields follow the last ')'.
    rest = raw[raw.rfind(b")") + 2 :].split()
    if len(rest) < 37:
        return None
    return int(rest[11]) + int(rest[12]), int(rest[36])


def _snapshot() -> dict[str, dict[str, tuple[int, int]]]:
    out: dict[str, dict[str, tuple[int, int]]] = {}
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            tids = os.listdir(f"/proc/{pid}/task")
        except OSError:
            continue
        threads = {}
        for tid in tids:
            ticks = _task_ticks(pid, tid)
            if ticks is not None:
                threads[tid] = ticks
        if threads:
            out[pid] = threads
    return out


def _describe(pid: str) -> tuple[str, str, bool]:
    """(comm, cmdline, is_kernel_thread)."""
    try:
        with open(f"/proc/{pid}/comm") as handle:
            comm = handle.read().strip()
    except OSError:
        comm = "?"
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as handle:
            cmdline = handle.read().replace(b"\0", b" ").decode(errors="replace").strip()
    except OSError:
        cmdline = ""
    return comm, cmdline[:200], cmdline == ""


def _proc_ticks(pid: str) -> tuple[int, int] | None:
    """(utime + stime in ticks, last CPU) of a whole process, or None if it is gone."""
    try:
        with open(f"/proc/{pid}/stat", "rb") as handle:
            raw = handle.read()
    except OSError:
        return None
    rest = raw[raw.rfind(b")") + 2 :].split()
    if len(rest) < 37:
        return None
    return int(rest[11]) + int(rest[12]), int(rest[36])


def _proc_snapshot() -> dict[str, tuple[int, int]]:
    out: dict[str, tuple[int, int]] = {}
    for pid in os.listdir("/proc"):
        if pid.isdigit():
            ticks = _proc_ticks(pid)
            if ticks is not None:
                out[pid] = ticks
    return out


def _thread_cpus(pid: str, cores: set[int]) -> set[int]:
    """The last CPU of every thread of `pid` (what `ps -o psr` shows), within `cores`."""
    out: set[int] = set()
    try:
        tids = os.listdir(f"/proc/{pid}/task")
    except OSError:
        return out
    for tid in tids:
        ticks = _task_ticks(pid, tid)
        if ticks is not None and ticks[1] in cores:
            out.add(ticks[1])
    return out


def _all_cpus() -> set[int]:
    try:
        return set(range(os.cpu_count() or 0))
    except Exception:
        return set()


def sample(cores: set[int], seconds: float, ignore_pids: set[int]) -> list[dict]:
    """Processes that used CPU on `cores` during the sampling interval, busiest first."""
    if cores >= _all_cpus() and cores:
        # Every core is sampled, so whole-process CPU time is exact and the walk reads one stat
        # per process instead of one per thread (a host with tens of thousands of threads makes
        # the per-thread walk take seconds per sample).
        started = time.monotonic()
        first = _proc_snapshot()
        time.sleep(seconds)
        resumed = time.monotonic()
        second = _proc_snapshot()
        elapsed = max(resumed - started, 1e-3)
        rows = []
        for pid, (ticks, cpu) in second.items():
            if int(pid) in ignore_pids or pid not in first:
                continue
            delta = ticks - first[pid][0]
            if delta <= 0:
                continue
            comm, cmdline, kernel = _describe(pid)
            pct = delta / CLK_TCK / elapsed * 100.0
            cpus = {c for c in (cpu, first[pid][1]) if c in cores}
            if pct >= 5.0:
                # `ps -o psr` for every thread of a process worth recording: the cores it favours.
                cpus |= _thread_cpus(pid, cores)
            rows.append(
                {
                    "pid": int(pid),
                    "comm": comm,
                    "cmdline": cmdline,
                    "cpu_pct": pct,
                    "kernel_thread": kernel,
                    "cpus": sorted(cpus),
                }
            )
        rows.sort(key=lambda row: -row["cpu_pct"])
        return rows
    started = time.monotonic()
    first = _snapshot()
    time.sleep(seconds)
    resumed = time.monotonic()
    second = _snapshot()
    # A walk over /proc takes a noticeable fraction of a second on a busy host; the share is
    # taken over the measured interval between the two walks, not the nominal sleep.
    elapsed = max(resumed - started, 1e-3)
    rows = []
    for pid, threads in second.items():
        if int(pid) in ignore_pids or pid not in first:
            continue
        delta = 0
        busy_cpus: set[int] = set()
        for tid, (ticks, cpu) in threads.items():
            before = first[pid].get(tid)
            if before is None:
                continue
            if cpu in cores or before[1] in cores:
                used = ticks - before[0]
                delta += used
                if used > 0:
                    busy_cpus.update(c for c in (cpu, before[1]) if c in cores)
        if delta <= 0:
            continue
        comm, cmdline, kernel = _describe(pid)
        rows.append(
            {
                "pid": int(pid),
                "comm": comm,
                "cmdline": cmdline,
                "cpu_pct": delta / CLK_TCK / elapsed * 100.0,
                "kernel_thread": kernel,
                # The sampled cores this process's busy threads were last seen on (which lanes or
                # issuers it sat on), so a flagged trial names the cores, not only the process.
                "cpus": sorted(busy_cpus),
            }
        )
    rows.sort(key=lambda row: -row["cpu_pct"])
    return rows


def classify(
    rows: list[dict], threshold_pct: float, allow: re.Pattern | None, discard_kernel: bool
) -> tuple[list[dict], list[dict]]:
    """Split the busy processes into (foreign, background) by the allow list and the threshold."""
    foreign, background = [], []
    for row in rows:
        if row["cpu_pct"] < threshold_pct:
            continue
        text = f"{row['comm']} {row['cmdline']}"
        if allow is not None and allow.search(text):
            background.append(row)
        elif row["kernel_thread"] and not discard_kernel:
            background.append(row)
        else:
            foreign.append(row)
    return foreign, background
