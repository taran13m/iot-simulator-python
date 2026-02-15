"""Simulator - top-level orchestrator that wires the data generator to
one or more sinks, each with independent throughput control.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

from iot_simulator.generator import DataGenerator
from iot_simulator.models import SensorRecord
from iot_simulator.sensor_models import SensorConfig
from iot_simulator.sinks.base import Sink, SinkRunner
from iot_simulator.sinks.callback import CallbackSink

__all__ = ["Simulator"]

logger = logging.getLogger("iot_simulator")


class Simulator:
    """High-level API for generating IoT sensor data and pushing it to sinks.

    Example::

        from iot_simulator import Simulator
        from iot_simulator.sinks import ConsoleSink

        sim = Simulator(industries=["mining"], update_rate_hz=2.0)
        sim.add_sink(ConsoleSink(rate_hz=0.5))
        sim.run(duration_s=30)

    Parameters:
        industries:
            Names of built-in industry sensor sets to activate.
        custom_sensors:
            User-defined :class:`SensorConfig` list.
        custom_industry:
            Label applied to *custom_sensors*.
        update_rate_hz:
            How many times per second the generator produces a new batch
            of values.  Each sink may independently consume at a different
            (possibly slower) rate.
    """

    def __init__(
        self,
        *,
        industries: list[str] | None = None,
        custom_sensors: list[SensorConfig] | None = None,
        custom_industry: str = "custom",
        update_rate_hz: float = 2.0,
    ) -> None:
        self._generator = DataGenerator(
            industries=industries,
            custom_sensors=custom_sensors,
            custom_industry=custom_industry,
            update_rate_hz=update_rate_hz,
        )
        self._update_rate_hz = update_rate_hz
        self._runners: list[SinkRunner] = []
        self._running = False

    # ------------------------------------------------------------------
    # Sensor management
    # ------------------------------------------------------------------

    def add_sensors(self, sensors: list[SensorConfig], industry: str = "custom") -> None:
        """Add more sensors to the generator at any time (before ``run``)."""
        self._generator.add_sensors(sensors, industry=industry)

    @property
    def sensor_count(self) -> int:
        return self._generator.sensor_count

    # ------------------------------------------------------------------
    # Sink management
    # ------------------------------------------------------------------

    def add_sink(
        self,
        sink: Sink | Callable[[list[SensorRecord]], Any],
        *,
        rate_hz: float | None = None,
        batch_size: int | None = None,
    ) -> None:
        """Register a sink (or callable) to receive generated data.

        Parameters:
            sink:
                A :class:`Sink` instance **or** any callable that accepts
                ``list[SensorRecord]``.
            rate_hz:
                Override the sink's ``rate_hz`` (useful when the sink was
                created without throughput params, e.g. a lambda).
            batch_size:
                Override the sink's ``batch_size``.
        """
        if not isinstance(sink, Sink):
            # Wrap bare callable in a CallbackSink
            sink = CallbackSink(sink, rate_hz=rate_hz, batch_size=batch_size or 100)
        else:
            # Optionally override throughput settings
            if rate_hz is not None:
                sink.sink_config.rate_hz = rate_hz
            if batch_size is not None:
                sink.sink_config.batch_size = batch_size

        runner = SinkRunner(sink)
        self._runners.append(runner)

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------

    def run(self, duration_s: float | None = None) -> None:
        """Blocking entry point - starts the event loop.

        Works transparently inside environments that already have a running
        event loop (Databricks notebooks, Jupyter, IPython) by spawning a
        dedicated background thread with its own loop.

        Parameters:
            duration_s: If provided, stop automatically after this many
                        seconds.  ``None`` means run until Ctrl-C.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None and loop.is_running():
            # Inside an existing event loop (Databricks notebook, Jupyter, IPython).
            # Spawn a dedicated thread so asyncio.run() gets its own loop.
            exc: list[BaseException | None] = [None]

            def _target() -> None:
                try:
                    asyncio.run(self.run_async(duration_s=duration_s))
                except KeyboardInterrupt:
                    logger.info("Interrupted by user")
                except BaseException as e:
                    exc[0] = e

            t = threading.Thread(target=_target, daemon=True)
            t.start()
            t.join()
            if exc[0] is not None:
                raise exc[0]
        else:
            try:
                asyncio.run(self.run_async(duration_s=duration_s))
            except KeyboardInterrupt:
                logger.info("Interrupted by user")

    async def run_async(self, duration_s: float | None = None) -> None:
        """Async entry point - runs inside an existing event loop."""
        if not self._runners:
            logger.warning("No sinks registered - nothing to do. Call add_sink() first.")
            return

        logger.info(
            "Starting simulator: %d sensors, %d sinks, %.1f Hz",
            self._generator.sensor_count,
            len(self._runners),
            self._update_rate_hz,
        )

        # Start all sink runners (connects + background drain)
        for runner in self._runners:
            await runner.start()

        self._running = True
        interval = 1.0 / self._update_rate_hz
        start_time = asyncio.get_event_loop().time()

        # Install signal handlers for graceful shutdown.
        # NotImplementedError: raised on Windows where signal handlers are unsupported.
        # RuntimeError: raised when running in a non-main thread (e.g. notebook env).
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError, RuntimeError):
                loop.add_signal_handler(sig, stop_event.set)

        try:
            tick_count = 0
            while self._running:
                # Check duration
                if duration_s is not None:
                    elapsed = asyncio.get_event_loop().time() - start_time
                    if elapsed >= duration_s:
                        logger.info("Duration reached (%.1fs) - stopping", duration_s)
                        break

                # Check stop signal
                if stop_event.is_set():
                    logger.info("Stop signal received - shutting down")
                    break

                # Generate a batch
                tick_start = asyncio.get_event_loop().time()
                records = self._generator.tick()

                # Fan out to all sink runners
                for runner in self._runners:
                    await runner.enqueue(records)

                tick_count += 1
                if tick_count % 100 == 0:
                    logger.debug("Tick %d - generated %d records", tick_count, len(records))

                # Sleep for remaining interval
                tick_elapsed = asyncio.get_event_loop().time() - tick_start
                sleep_time = max(0.0, interval - tick_elapsed)
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)

        except asyncio.CancelledError:
            logger.info("Simulator cancelled")
        finally:
            self._running = False
            # Graceful shutdown: drain + flush + close all sinks
            logger.info("Stopping %d sink runners...", len(self._runners))
            for runner in self._runners:
                await runner.stop()
            logger.info("All sinks stopped.")

    # ------------------------------------------------------------------
    # Alternative constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_csv(
        cls,
        path: str | Path,
        industry: str = "custom",
        update_rate_hz: float = 2.0,
    ) -> Simulator:
        """Create a Simulator loading custom sensor definitions from CSV.

        See :meth:`DataGenerator.from_csv` for the expected file format.
        """
        gen = DataGenerator.from_csv(path, industry=industry, update_rate_hz=update_rate_hz)
        sim = cls(update_rate_hz=update_rate_hz)
        sim._generator = gen
        return sim
