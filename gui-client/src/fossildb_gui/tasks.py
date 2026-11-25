"""Running blocking work (gRPC calls, decoding) in background threads, and handing results to the UI thread."""

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

import shiboken6
from PySide6.QtCore import QObject, Signal


class _Relay(QObject):
    # Emitted from worker threads. The relay lives in the UI thread, so the slot runs there (queued connection).
    finished = Signal(object)


class TaskRunner(QObject):
    """Runs functions in a thread pool and calls on_result/on_error in the UI thread.

    If a context object is given, the callbacks are skipped when it has been deleted in the meantime.
    """

    def __init__(self, max_workers: int = 8, parent: QObject | None = None):
        super().__init__(parent)
        self._pool = ThreadPoolExecutor(max_workers, thread_name_prefix="fossildb-gui")
        self._relay = _Relay(self)
        self._relay.finished.connect(self._deliver)
        self._shut_down = False

    def submit(
        self,
        fn: Callable[[], Any],
        on_result: Callable[[Any], None] | None = None,
        on_error: Callable[[Exception], None] | None = None,
        context: QObject | None = None,
    ) -> None:
        def run():
            try:
                outcome = (True, fn())
            except Exception as e:  # handed to on_error
                outcome = (False, e)
            self._relay.finished.emit((outcome, on_result, on_error, context))

        if self._shut_down:
            return  # the app is quitting
        self._pool.submit(run)

    def _deliver(self, payload) -> None:
        (ok, value), on_result, on_error, context = payload
        if context is not None and not shiboken6.isValid(context):
            return
        if ok:
            if on_result is not None:
                on_result(value)
        elif on_error is not None:
            on_error(value)
        else:
            raise value

    def shutdown(self) -> None:
        # Running gRPC calls can't be interrupted, so don't wait for them
        self._shut_down = True
        self._pool.shutdown(wait=False, cancel_futures=True)


class Latest:
    """Drops results of requests that were superseded by a newer one, e.g. after selecting another key."""

    def __init__(self):
        self._generation = 0

    def next(self) -> int:
        self._generation += 1
        return self._generation

    def current(self) -> int:
        """The latest generation, e.g. for follow-up requests that belong to it."""
        return self._generation

    def is_current(self, generation: int) -> bool:
        return generation == self._generation

    def invalidate(self) -> None:
        self._generation += 1

    def wrap(self, generation: int, callback: Callable[[Any], None]) -> Callable[[Any], None]:
        def wrapped(value):
            if generation == self._generation:
                callback(value)

        return wrapped
