#!/usr/bin/env python3
"""Bracket an indexer's sustained throughput: the highest offered rate at which trials keep up.

A trial keeps up when its generator was valid and it achieved at least `--keep-up-ratio` (0.99)
of the offered rate. A point passes when every one of its `--trials` fresh-process trials keeps
up. Starting from a rate known to pass (`--lo`) and one known to fail (`--hi`), the search moves
the geometric midpoint until hi / lo <= 1 + tolerance.

The command template runs one trial. Placeholders: `{rate}` (block ops per second, for a harness
with `--offered-block-ops-per-sec`), `{window_ms}` (for a harness driven by the window; needs
`--total-block-ops`), `{json}` (result path), `{point}` and `{trial}` (indices). Each trial runs
under the lock file (flock), with a foreign-load sample before and after it recorded in the
output. A trial whose generator could not issue on schedule counts as not kept up and is marked
`G` in the log and "generator invalid" in the table, so a bracket that ends on the generator's
ceiling rather than the indexer's is visible as such.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import pathlib
import re
import shlex
import subprocess
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import hostload  # noqa: E402
import measurelock  # noqa: E402


def run_trial(
    args: argparse.Namespace,
    point: int,
    trial: int,
    rate: float,
    out: pathlib.Path,
    lock: measurelock.MeasureLock,
) -> dict:
    window_ms = (
        max(1, round(args.total_block_ops / rate * 1000.0)) if args.total_block_ops else None
    )
    result = out / f"point{point}-trial{trial}.json"
    command = args.command.format(
        rate=f"{rate:.0f}", window_ms=window_ms, json=result, point=point, trial=trial
    )
    cores = hostload.parse_cpu_list(args.cores)
    allow = re.compile(args.allow) if args.allow else None
    record = {
        "point": point,
        "trial": trial,
        "rate_requested": rate,
        "window_ms": window_ms,
        "command": command,
    }
    if args.lock_scope == "trial":
        lock.acquire(args.trial_minutes)
    before = hostload.sample(cores, args.sample_seconds, {os.getpid()})
    started = time.time()
    with open(out / f"point{point}-trial{trial}.log", "w") as log:
        child = subprocess.Popen(shlex.split(command), stdout=log, stderr=subprocess.STDOUT)
        (out / "child.pid").write_text(f"{child.pid}\n")  # the only pid a stop may target
        returncode = child.wait()
    (out / "child.pid").write_text("")
    record["wall_s"] = time.time() - started
    after = hostload.sample(cores, args.sample_seconds, {os.getpid()})
    if args.lock_scope == "trial":
        lock.release()
    elif lock.over_cap():
        print(f"lock held {lock.held_s() / 60:.0f} min: releasing and queueing again", flush=True)
        lock.rotate(args.point_minutes)
    else:
        lock.note(args.point_minutes)
    record["exit_code"] = returncode
    foreign_b, background_b = hostload.classify(before, args.threshold_pct, allow, False)
    foreign_a, background_a = hostload.classify(after, args.threshold_pct, allow, False)
    record["foreign_load"] = foreign_b + foreign_a
    record["background_load"] = background_b + background_a
    if returncode != 0 or not result.exists():
        record.update(kept_up=False, error="trial failed")
        return record
    data = json.loads(result.read_text())
    offered = data["offered_block_ops_per_sec"]
    achieved = data["achieved_block_ops_per_sec"]
    record.update(
        offered=offered,
        achieved=achieved,
        ratio=achieved / offered if offered else 0.0,
        generator_valid=bool(data["generator_valid"]),
        kept_up=bool(data["generator_valid"]) and achieved >= args.keep_up_ratio * offered,
        lookup_p50_us=data["query_service"]["p50_ns"] / 1e3,
        lookup_p99_us=data["query_service"]["p99_ns"] / 1e3,
        drain_ms=data["drain_ns"] / 1e6,
        total_block_ops=data.get("total_block_ops"),
        failure_reasons=data.get("failure_reasons", []),
    )
    return record


def fmt_rate(rate: float) -> str:
    return f"{rate / 1e6:.1f}M"


def span(values: list[float], digits: int) -> str:
    lo, hi = min(values), max(values)
    return f"{lo:.{digits}f}" if lo == hi else f"{lo:.{digits}f}-{hi:.{digits}f}"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--command", required=True, help="trial command template (see module doc)")
    parser.add_argument(
        "--lo", type=float, required=True, help="rate known to keep up (block ops/s)"
    )
    parser.add_argument("--hi", type=float, required=True, help="rate known to fail (block ops/s)")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument(
        "--tolerance", type=float, default=0.10, help="stop when hi/lo <= 1 + tolerance"
    )
    parser.add_argument("--max-points", type=int, default=8)
    parser.add_argument("--keep-up-ratio", type=float, default=0.99)
    parser.add_argument(
        "--total-block-ops", type=int, default=0, help="needed when the template uses {window_ms}"
    )
    parser.add_argument("--verify-ends", action="store_true", help="run the endpoints first")
    parser.add_argument(
        "--lock-scope",
        choices=("trial", "run"),
        default="trial",
        help="hold the lock per trial (fair to other takers) or for the whole run (one queue wait)",
    )
    parser.add_argument(
        "--lock", required=True, help="lock file held for the duration of each trial"
    )
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
    parser.add_argument(
        "--point-minutes", type=float, default=6.0, help="expected length of one point"
    )
    parser.add_argument("--cores", default="0-63", help="cores to check for foreign load")
    parser.add_argument(
        "--threshold-pct", type=float, default=5.0, help="record foreign processes above this share"
    )
    parser.add_argument(
        "--discard-pct",
        type=float,
        default=50.0,
        help="a trial that fell short under a foreign process above this share is replaced",
    )
    parser.add_argument("--max-replacements", type=int, default=3, help="per point")
    parser.add_argument("--sample-seconds", type=float, default=1.0)
    parser.add_argument(
        "--allow", default="", help="regex of background processes to record, not flag"
    )
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "runner.pid").write_text(f"{os.getpid()}\n")
    lock = measurelock.MeasureLock(
        args.lock,
        args.owner_file or re.sub(r"\.lock$", "", args.lock) + ".owner",
        args.workstream,
        args.series or pathlib.Path(args.out).name,
        args.max_hold_minutes,
        leave_note=args.leave_owner_note,
    )
    if args.lock_scope == "run":
        lock.acquire(min(args.max_hold_minutes, args.max_points * args.point_minutes))
    if "{window_ms}" in args.command and not args.total_block_ops:
        parser.error("--total-block-ops is required with a {window_ms} template")

    lo, hi = args.lo, args.hi
    points: list[dict] = []

    def disturbed(trial: dict) -> bool:
        """A trial that fell short while a foreign process sat above the discard threshold does
        not count against the point; it is replaced, as the protocol runner replaces such trials."""
        if trial["kept_up"]:
            return False
        return any(row["cpu_pct"] >= args.discard_pct for row in trial["foreign_load"])

    def measure(rate: float) -> bool:
        index = len(points)
        trials: list[dict] = []
        replaced: list[dict] = []
        attempt = 0
        while len(trials) < args.trials and attempt < args.trials + args.max_replacements:
            trial = run_trial(args, index, attempt, rate, out, lock)
            attempt += 1
            (replaced if disturbed(trial) else trials).append(trial)
        passed = len(trials) == args.trials and all(t["kept_up"] for t in trials)
        points.append({"rate": rate, "passed": passed, "trials": trials, "replaced": replaced})
        line = ", ".join(
            f"{t.get('ratio', 0) * 100:.1f}%{'' if t['kept_up'] else '!'}"
            + ("" if t.get("generator_valid", True) else " G")
            + (" FL" if t["foreign_load"] else "")
            for t in trials
        )
        note = f", {len(replaced)} replaced for foreign load" if replaced else ""
        print(
            f"point {index}: {fmt_rate(rate)} offered -> {'pass' if passed else 'fail'} [{line}]{note}",
            flush=True,
        )
        (out / "bracket.json").write_text(
            json.dumps({"lo": lo, "hi": hi, "points": points}, indent=1)
        )
        return passed

    if args.verify_ends:
        if not measure(lo):
            print(f"--lo {fmt_rate(lo)} does not keep up; lower it", file=sys.stderr)
            return 2
        if measure(hi):
            print(f"--hi {fmt_rate(hi)} keeps up; raise it", file=sys.stderr)
            return 2
    while hi / lo > 1.0 + args.tolerance and len(points) < args.max_points:
        mid = math.sqrt(lo * hi)
        if measure(mid):
            lo = mid
        else:
            hi = mid
    summary = {
        "lo_keeps_up": lo,
        "hi_fails": hi,
        "bracket_ratio": hi / lo,
        "within_tolerance": hi / lo <= 1.0 + args.tolerance,
        "trials_per_point": args.trials,
        "keep_up_ratio": args.keep_up_ratio,
        "points": points,
    }
    (out / "bracket.json").write_text(json.dumps(summary, indent=1))
    header = (
        "| Point | Offered | Verdict | Achieved / offered per trial | Lookup p50 (us) "
        "| Lookup p99 (us) | Foreign load |"
    )
    lines = [
        f"Sustained bracket: keeps up at {fmt_rate(lo)}, fails at {fmt_rate(hi)} "
        f"(ratio {hi / lo:.3f}, {args.trials} trials per point, keep-up ratio {args.keep_up_ratio}).",
        "",
        header,
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for i, p in enumerate(points):
        ok = [t for t in p["trials"] if "ratio" in t]
        ratios = (
            ", ".join(
                f"{t['ratio'] * 100:.1f}%"
                + ("" if t.get("generator_valid", True) else " (generator invalid)")
                for t in ok
            )
            or "failed"
        )
        p50 = span([t["lookup_p50_us"] for t in ok], 1) if ok else "-"
        p99 = span([t["lookup_p99_us"] for t in ok], 0) if ok else "-"
        flagged = sum(1 for t in p["trials"] if t["foreign_load"])
        verdict = "pass" if p["passed"] else "fail"
        lines.append(
            f"| {i} | {fmt_rate(p['rate'])} | {verdict} | {ratios} | {p50} | {p99} | {flagged} |"
        )
    (out / "bracket.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    lock.release()
    return 0 if summary["within_tolerance"] else 1


if __name__ == "__main__":
    sys.exit(main())
