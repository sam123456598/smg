#!/usr/bin/env python3
"""Guardrail 5 runner: N fresh-process trials with an interleaved same-binary control pair.

For one named binary and configuration this runs `--trials` subject trials, each followed by a
control trial of `--control-command` (by default the very same command, so the pair measures the
noise floor an A/A comparison would show). Every trial holds the lock file, samples the
measurement cores for foreign load before and after, and is discarded (kept in the output, marked)
when a process above the threshold that is neither the trial nor allow-listed shows up.

Reported: medians with bootstrap 95% confidence intervals (percentile method) of achieved block
ops/s and lookup p50/p99, per series, plus the subject-minus-control difference of medians with
its own bootstrap interval. Finished trials are skipped on re-run, so an interrupted run resumes.

Thresholds: every process above `--record-pct` (5%) of a core on the measured cores is recorded
with its peak; a trial is discarded when a process above `--discard-pct` (50%) is neither the
trial nor allow-listed. The rows are stored raw, so re-running on the same output directory
re-summarises finished trials under other thresholds without re-measuring.

Command placeholders: `{json}` (result path) and `{trial}`.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import random
import re
import shlex
import statistics
import subprocess
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import hostload  # noqa: E402
import measurelock  # noqa: E402

METRICS = (
    ("achieved_m", "Achieved (M block ops/s)", 1.0),
    ("p50_us", "Lookup p50 (us)", 1.0),
    ("p99_us", "Lookup p99 (us)", 1.0),
)


def run_trial(
    args: argparse.Namespace,
    role: str,
    index: int,
    command_template: str,
    lock: measurelock.MeasureLock,
) -> dict:
    out = pathlib.Path(args.out)
    record_path = out / "trials" / f"{role}-{index}.json"
    if record_path.exists():
        return json.loads(record_path.read_text())
    result = out / f"{role}-{index}.json"
    command = command_template.format(json=result, trial=index)
    cores = hostload.parse_cpu_list(args.cores)
    record = {"role": role, "index": index, "command": command, "started_at": time.time()}
    if args.lock_scope == "trial":
        lock.acquire(args.trial_minutes)
    before = hostload.sample(cores, args.sample_seconds, {os.getpid()})
    record["socket_freq_mhz_before"] = hostload.socket_freq_mhz()
    started = time.time()
    with open(out / f"{role}-{index}.log", "w") as log:
        child = subprocess.Popen(shlex.split(command), stdout=log, stderr=subprocess.STDOUT)
        (out / "child.pid").write_text(f"{child.pid}\n")  # the only pid a stop may target
        returncode = child.wait()
    (out / "child.pid").write_text("")
    record["wall_s"] = time.time() - started
    after = hostload.sample(cores, args.sample_seconds, {os.getpid()})
    record["socket_freq_mhz_after"] = hostload.socket_freq_mhz()
    if args.lock_scope == "trial":
        lock.release()
    # Everything above the record threshold is kept; the discard decision is made at summary time
    # from these rows, so a finished run can be re-summarised under other thresholds.
    record["load_rows"] = [row for row in before + after if row["cpu_pct"] >= args.record_pct]
    record["exit_code"] = returncode
    if returncode != 0 or not result.exists():
        record["discarded"] = f"trial failed (exit {returncode})"
    else:
        data = json.loads(result.read_text())
        offered = data["offered_block_ops_per_sec"]
        achieved = data["achieved_block_ops_per_sec"]
        record.update(
            offered_m=offered / 1e6,
            achieved_m=achieved / 1e6,
            ratio=achieved / offered if offered else 0.0,
            generator_valid=bool(data["generator_valid"]),
            kept_up=bool(data["generator_valid"]) and achieved >= 0.99 * offered,
            p50_us=data["query_service"]["p50_ns"] / 1e3,
            p99_us=data["query_service"]["p99_ns"] / 1e3,
            p999_us=data["query_service"]["p999_ns"] / 1e3,
            e2e_p99_us=data["query_scheduled_to_finished"]["p99_ns"] / 1e3,
            drain_ms=data["drain_ns"] / 1e6,
            failure_reasons=data.get("failure_reasons", []),
        )
        if not data["generator_valid"]:
            record["discarded"] = (
                "generator invalid: " + ", ".join(data.get("failure_reasons", []))[:200]
            )
    record_path.parent.mkdir(parents=True, exist_ok=True)
    record_path.write_text(json.dumps(record, indent=1))
    return record


def bootstrap_median(
    values: list[float], rng: random.Random, rounds: int
) -> tuple[float, float, float]:
    """(median, low, high) with a percentile-bootstrap 95% interval."""
    if not values:
        return float("nan"), float("nan"), float("nan")
    medians = sorted(statistics.median(rng.choices(values, k=len(values))) for _ in range(rounds))
    return (
        statistics.median(values),
        medians[int(0.025 * rounds)],
        medians[min(rounds - 1, int(0.975 * rounds))],
    )


def bootstrap_difference(
    a: list[float], b: list[float], rng: random.Random, rounds: int
) -> tuple[float, float, float]:
    if not a or not b:
        return float("nan"), float("nan"), float("nan")
    diffs = sorted(
        statistics.median(rng.choices(a, k=len(a))) - statistics.median(rng.choices(b, k=len(b)))
        for _ in range(rounds)
    )
    return (
        statistics.median(a) - statistics.median(b),
        diffs[int(0.025 * rounds)],
        diffs[min(rounds - 1, int(0.975 * rounds))],
    )


def load_rows(record: dict) -> list[dict]:
    """The sampled processes of a trial (older records kept them pre-classified)."""
    return record.get(
        "load_rows", record.get("foreign_load", []) + record.get("background_load", [])
    )


def verdict(
    record: dict, args: argparse.Namespace, allow: re.Pattern | None
) -> tuple[str | None, list]:
    """(reason the trial is discarded or None, background rows) under the current thresholds."""
    hard = record.get("discarded")
    if hard and not hard.startswith("foreign load"):
        return hard, []
    rows = load_rows(record)
    foreign, background = hostload.classify(
        rows, args.discard_pct, allow, args.discard_kernel_threads
    )
    background += [row for row in rows if row["cpu_pct"] < args.discard_pct]
    if foreign:
        busy = ", ".join(
            f"{row['comm']}[{row['pid']}] {row['cpu_pct']:.0f}%" for row in foreign[:4]
        )
        return f"foreign load: {busy}", background
    return None, background


def background_summary(
    records: list[dict], args: argparse.Namespace, allow: re.Pattern | None
) -> list[str]:
    """Background per process name (allow-listed, kernel, or below the discard threshold)."""
    peak: dict[str, tuple[int, float]] = {}
    for record in records:
        seen: dict[str, float] = {}
        for row in verdict(record, args, allow)[1]:
            name = "kernel threads" if row["kernel_thread"] else row["comm"]
            seen[name] = max(seen.get(name, 0.0), row["cpu_pct"])
        for name, pct in seen.items():
            count, top = peak.get(name, (0, 0.0))
            peak[name] = (count + 1, max(top, pct))
    return [
        f"{name}: in {count} of {len(records)} trials, peak {top:.0f}%"
        for name, (count, top) in sorted(peak.items(), key=lambda item: -item[1][1])
    ]


def summarize(args: argparse.Namespace, series: dict[str, list[dict]]) -> tuple[dict, str]:
    rng = random.Random(args.seed)
    allow = re.compile(args.allow) if args.allow else None
    summary: dict = {"trials_requested": args.trials, "series": {}}
    attempts = max(len(records) for records in series.values())
    lines = [
        f"{args.name}: {args.trials} usable trials wanted per series ({attempts} attempted), fresh "
        f"process each, lock scope {args.lock_scope}; cores {args.cores} sampled before and after "
        f"each trial, processes above {args.record_pct:.0f}% recorded, a trial discarded when one "
        f"above {args.discard_pct:.0f}% is neither the trial nor allow-listed.",
        "",
        "| Series | Used / discarded | Kept up | Achieved median [95% CI] (M block ops/s) "
        "| Lookup p50 [CI] (us) | Lookup p99 [CI] (us) | Drain median (ms) |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    kept: dict[str, dict[str, list[float]]] = {}
    for role, records in series.items():
        verdicts = {id(r): verdict(r, args, allow)[0] for r in records}
        used = [r for r in records if verdicts[id(r)] is None]
        discarded = [r for r in records if verdicts[id(r)] is not None]
        kept[role] = {key: [r[key] for r in used] for key, _, _ in METRICS}
        stats = {}
        cells = []
        for key, _, _ in METRICS:
            med, lo, hi = bootstrap_median(kept[role][key], rng, args.bootstrap)
            stats[key] = {"median": med, "ci95": [lo, hi], "n": len(used)}
            cells.append(f"{med:.1f} [{lo:.1f}, {hi:.1f}]")
        drain = statistics.median([r["drain_ms"] for r in used]) if used else float("nan")
        summary["series"][role] = {
            "used": len(used),
            "discarded": [{"index": r["index"], "why": verdicts[id(r)]} for r in discarded],
            "kept_up": sum(1 for r in used if r.get("kept_up")),
            "stats": stats,
            "drain_ms_median": drain,
            "background_load": background_summary(records, args, allow),
        }
        lines.append(
            f"| {role} | {len(used)} / {len(discarded)} | {summary['series'][role]['kept_up']} of {len(used)} | "
            + " | ".join(cells)
            + f" | {drain:.0f} |"
        )
    roles = list(series)
    if len(roles) == 2:
        diffs = {}
        parts = []
        for key, label, _ in METRICS:
            d, lo, hi = bootstrap_difference(
                kept[roles[0]][key], kept[roles[1]][key], rng, args.bootstrap
            )
            diffs[key] = {"difference": d, "ci95": [lo, hi]}
            parts.append(f"{label}: {d:+.1f} [{lo:+.1f}, {hi:+.1f}]")
        summary["subject_minus_control"] = diffs
        lines += [
            "",
            f"Subject minus control (difference of medians, bootstrap 95% CI): {'; '.join(parts)}.",
        ]
    discarded_lines = [
        f"- {role} trial {d['index']}: {d['why']}"
        for role in roles
        for d in summary["series"][role]["discarded"]
    ]
    lines += ["", "Discarded trials:" + (" none" if not discarded_lines else "")] + discarded_lines
    background = background_summary([r for role in roles for r in series[role]], args, allow)
    lines += ["", "Background load recorded (allow-listed daemons and kernel threads):"]
    lines += [f"- {entry}" for entry in background] or ["- none"]
    return summary, "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--name", required=True, help="label of the subject")
    parser.add_argument("--command", required=True, help="subject trial command template")
    parser.add_argument(
        "--control-command",
        default="",
        help="control template; default: the subject's; 'none' disables",
    )
    parser.add_argument("--control-name", default="control (same binary)")
    parser.add_argument("--trials", type=int, default=20, help="usable trials wanted per series")
    parser.add_argument(
        "--max-trials",
        type=int,
        default=0,
        help="stop after this many attempts per series even if fewer are usable (default 2 x trials)",
    )
    parser.add_argument(
        "--lock-scope",
        choices=("trial", "run"),
        default="trial",
        help="hold the lock per trial (fair to other takers) or for the whole run (one queue wait)",
    )
    parser.add_argument("--lock", required=True)
    parser.add_argument(
        "--owner-file", default="", help="owner note for waiters (default: <lock>.owner)"
    )
    parser.add_argument("--workstream", default="bench", help="first word of the owner note")
    parser.add_argument(
        "--series", default="", help="second word of the owner note (default: out dir name)"
    )
    parser.add_argument(
        "--max-hold-minutes", type=float, default=45.0, help="release and re-queue past this"
    )
    parser.add_argument(
        "--trial-minutes", type=float, default=2.0, help="expected length of one trial"
    )
    parser.add_argument(
        "--leave-owner-note",
        action="store_true",
        help="keep the owner note after the final release (a multi-step driver truncates it itself)",
    )
    parser.add_argument("--cores", default="0-63")
    parser.add_argument(
        "--record-pct", type=float, default=5.0, help="record processes above this CPU share"
    )
    parser.add_argument(
        "--discard-pct",
        type=float,
        default=50.0,
        help="discard a trial when a process above this share is neither the trial nor allow-listed",
    )
    parser.add_argument("--sample-seconds", type=float, default=1.0)
    parser.add_argument(
        "--allow", default="", help="regex of background processes to record, not flag"
    )
    parser.add_argument("--discard-kernel-threads", action="store_true")
    parser.add_argument("--bootstrap", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "runner.pid").write_text(f"{os.getpid()}\n")
    lock = measurelock.MeasureLock(
        args.lock,
        args.owner_file or re.sub(r"\.lock$", "", args.lock) + ".owner",
        args.workstream,
        args.series or out.name,
        args.max_hold_minutes,
        leave_note=args.leave_owner_note,
    )
    if args.lock_scope == "run":
        lock.acquire(min(args.max_hold_minutes, args.trials * 2 * args.trial_minutes))
    control = args.control_command or args.command
    series: dict[str, list[dict]] = {args.name: []}
    if control != "none":
        series[args.control_name] = []
    max_trials = args.max_trials or 2 * args.trials
    index = 0
    # Discarded trials are replaced until every series has `--trials` usable ones (or the cap).
    allow = re.compile(args.allow) if args.allow else None
    while index < max_trials and any(
        sum(1 for r in records if verdict(r, args, allow)[0] is None) < args.trials
        for records in series.values()
    ):
        record = run_trial(args, "subject", index, args.command, lock)
        series[args.name].append(record)
        print(f"subject {index}: " + describe(record, verdict(record, args, allow)[0]), flush=True)
        if control != "none":
            record = run_trial(args, "control", index, control, lock)
            series[args.control_name].append(record)
            print(
                f"control {index}: " + describe(record, verdict(record, args, allow)[0]), flush=True
            )
        summary, text = summarize(args, series)
        (out / "summary.json").write_text(json.dumps(summary, indent=1))
        (out / "summary.md").write_text(text)
        index += 1
        if args.lock_scope == "run":
            done = [r for records in series.values() for r in records if "wall_s" in r]
            per_trial = sum(r["wall_s"] for r in done) / max(len(done), 1) / 60.0 + 0.1
            usable = min(
                sum(1 for r in records if verdict(r, args, allow)[0] is None)
                for records in series.values()
            )
            expected = max(0, args.trials - usable) * len(series) * per_trial
            if lock.over_cap() and expected > 0:
                print(
                    f"lock held {lock.held_s() / 60:.0f} min: releasing and queueing again",
                    flush=True,
                )
                lock.rotate(min(args.max_hold_minutes, expected))
            else:
                lock.note(min(args.max_hold_minutes, expected))
    lock.release()
    print(text)
    return 0


def describe(record: dict, discarded: str | None) -> str:
    if "achieved_m" not in record:
        return discarded or "failed"
    text = (
        f"{record['achieved_m']:.1f}M ({record['ratio'] * 100:.1f}% of offered), "
        f"p50 {record['p50_us']:.1f} us, p99 {record['p99_us']:.0f} us"
    )
    return text + (f" DISCARDED: {discarded}" if discarded else "")


if __name__ == "__main__":
    sys.exit(main())
