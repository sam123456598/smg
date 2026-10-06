"""The shared measurement lock with an owner note, so waiters can see who holds it and until when.

One hold per series, capped at `max_hold_minutes`; on acquisition the owner file gets
`<workstream> <series> <expected end HH:MM>`, refreshed as the estimate improves and truncated on
release.
"""

from __future__ import annotations

import fcntl
import time


class MeasureLock:
    def __init__(
        self,
        path: str,
        owner_file: str,
        workstream: str,
        series: str,
        max_hold_minutes: float,
        leave_note: bool = False,
    ):
        self.path = path
        # A multi-step series leaves its note in place between holds (with the next expected
        # end), so waiters see one continuing series; the driver truncates it at the very end.
        self.leave_note = leave_note
        self.owner_file = owner_file
        self.workstream = workstream
        self.series = series
        self.max_hold_s = max_hold_minutes * 60.0
        self.handle = None
        self.acquired_at = 0.0

    def acquire(self, expected_minutes: float) -> None:
        self.handle = open(self.path, "w")  # noqa: SIM115 - held until release()
        fcntl.flock(self.handle, fcntl.LOCK_EX)
        self.acquired_at = time.monotonic()
        self.note(expected_minutes)

    def note(self, expected_minutes: float) -> None:
        """Refresh the owner note with the current estimate of when the hold ends."""
        if self.handle is None:
            return
        end = time.strftime("%H:%M", time.localtime(time.time() + expected_minutes * 60.0))
        try:
            with open(self.owner_file, "w") as owner:
                owner.write(f"{self.workstream} {self.series} {end}\n")
        except OSError:
            pass

    def held_s(self) -> float:
        return time.monotonic() - self.acquired_at if self.handle else 0.0

    def over_cap(self) -> bool:
        return self.handle is not None and self.held_s() > self.max_hold_s

    def release(self, expected_minutes: float | None = None) -> None:
        if self.handle is None:
            return
        if self.leave_note and expected_minutes is not None:
            self.note(expected_minutes)
        elif not self.leave_note:
            try:
                with open(self.owner_file, "w"):
                    pass
            except OSError:
                pass
        fcntl.flock(self.handle, fcntl.LOCK_UN)
        self.handle.close()
        self.handle = None

    def rotate(self, expected_minutes: float) -> None:
        """Give the lock back and queue for it again (between trials, when a hold hits the cap);
        the note keeps announcing the series with its next expected end meanwhile."""
        keep = self.leave_note
        self.leave_note = True
        self.release(expected_minutes)
        self.leave_note = keep
        self.acquire(expected_minutes)
