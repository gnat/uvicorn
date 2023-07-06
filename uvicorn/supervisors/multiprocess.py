import logging
import os
import signal
import threading
import time
from multiprocessing.context import SpawnProcess
from socket import socket
from types import FrameType
from typing import Callable, List, Optional

import click

from uvicorn._subprocess import get_subprocess
from uvicorn.config import Config

HANDLED_SIGNALS = (
    signal.SIGINT,  # Unix signal 2. Sent by Ctrl+C.
    signal.SIGTERM,  # Unix signal 15. Sent by `kill <pid>`.
)

logger = logging.getLogger("uvicorn.error")


class Multiprocess:
    def __init__(
        self,
        config: Config,
        target: Callable[[Optional[List[socket]]], None],
        sockets: List[socket],
    ) -> None:
        self.config = config
        self.target = target
        self.sockets = sockets
        self.processes: List[SpawnProcess] = []
        self.should_exit = threading.Event()
        self.pid = os.getpid()
        self.mtimes: Dict[Path, float] = {}
        self.timeout_max = 10 # Worker timeout in seconds.

    def signal_handler(self, sig: int, frame: Optional[FrameType]) -> None:
        """
        A signal handler that is registered with the parent process.
        """
        self.should_exit.set()

    def run(self) -> None:
        self.startup()
        self.monitor()
        self.shutdown()

    def startup(self) -> None:
        message = "Started parent process [{}]".format(str(self.pid))
        color_message = "Started parent process [{}]".format(
            click.style(str(self.pid), fg="cyan", bold=True)
        )
        logger.info(message, extra={"color_message": color_message})
        logger.info(f"Reload_dirs:{self.config.reload_dirs}")

        for sig in HANDLED_SIGNALS:
            signal.signal(sig, self.signal_handler)

        for _idx in range(self.config.workers):
            process = get_subprocess(
                config=self.config, target=self.target, sockets=self.sockets
            )
            process.start()
            process.updated = False
            self.processes.append(process)

    def shutdown(self) -> None:
        for process in self.processes:
            process.terminate()
            process.join(self.timeout_max)

        message = "Stopping parent process [{}]".format(str(self.pid))
        color_message = "Stopping parent process [{}]".format(
            click.style(str(self.pid), fg="cyan", bold=True)
        )
        logger.info(message, extra={"color_message": color_message})

    def monitor(self) -> None:
        while not self.should_exit.is_set():
            time.sleep(0.5)
            if self.should_exit.is_set():
                break
            # Files updated?
            if self.updated():
                self.mtimes = {}
                for process in self.processes:
                    process.updated = True
                    #logger.info(f"process.updated: {process.updated}")
                    #process.name = f"{process.name}_restarting"
            # Restart expired workers.
            for process in self.processes:
                if process.updated: # Thundering herd protection: Migrate workers one by one.
                    logger.info(f"📦  Upgrading worker pid: {process.pid} ({process.name})")
                    process.terminate()
                    process.join(self.timeout_max)
                if not process.is_alive():
                    logger.info(f"♻️  Dead worker pid: {process.pid}")
                    self.processes.remove(process)
                    process_new = get_subprocess(
                        config=self.config, target=self.target, sockets=self.sockets
                    )
                    process_new.start()
                    process_new.updated = False
                    self.processes.append(process_new)
                    logger.info(f"⭐ New worker pid: {process_new.pid}")
                    time.sleep(0.1) # Thundering herd protection: Stagger new worker creation.

    def updated(self) -> bool:
        for reload_dir in self.config.reload_dirs:
            for path in list(reload_dir.rglob("*.py")):
                file = path.resolve()
                try:
                    mtime = file.stat().st_mtime
                except OSError:  # pragma: nocover
                    continue
                old_time = self.mtimes.get(file)
                if old_time is None:
                    self.mtimes[file] = mtime
                    continue
                elif mtime > old_time:
                    logger.info("Files updated. Reloading workers. [{}]".format(str(self.pid)))
                    return True # Files were updated.
