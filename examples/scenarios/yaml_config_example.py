#!/usr/bin/env python3
"""YAML config-driven example -- load simulator configuration from a YAML file
and run with the sink factory.

This demonstrates the declarative approach: all settings (industries, custom
sensors, sinks, throughput) are defined in ``simulator_config.yaml`` and the
Python code is minimal.

Directly runnable (uses Console + File sinks only).

Usage::

    python examples/scenarios/yaml_config_example.py

Equivalent CLI::

    iot-simulator run --config examples/configs/simulator_config.yaml --duration 15
"""

from __future__ import annotations

import logging
from pathlib import Path


def main() -> None:
    print("=== YAML Config-Driven Example ===\n")

    # Resolve the config file relative to this script
    config_path = Path(__file__).parent.parent / "configs" / "simulator_config.yaml"

    if not config_path.exists():
        print(f"  Config file not found: {config_path}")
        return

    print(f"  Config file: {config_path}\n")

    # --- Load the YAML configuration ---
    from iot_simulator.config import load_yaml_config
    cfg = load_yaml_config(config_path)

    # Set up logging
    logging.basicConfig(
        level=getattr(logging, cfg.log_level, logging.INFO),
        format="%(asctime)s %(name)-30s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    print(f"  Industries:      {cfg.industries}")
    print(f"  Custom sensors:  {len(cfg.custom_sensors)} ({cfg.custom_industry})")
    print(f"  Update rate:     {cfg.update_rate_hz} Hz")
    print(f"  Sinks:           {len(cfg.sink_configs)}")
    for i, sc in enumerate(cfg.sink_configs, 1):
        print(f"    Sink {i}: type={sc.get('type')}, rate_hz={sc.get('rate_hz')}, batch_size={sc.get('batch_size')}")
    print()

    # --- Build the Simulator ---
    from iot_simulator.simulator import Simulator
    sim = Simulator(
        industries=cfg.industries or None,
        custom_sensors=cfg.custom_sensors or None,
        custom_industry=cfg.custom_industry,
        update_rate_hz=cfg.update_rate_hz,
    )

    # --- Create sinks from config via the factory ---
    from iot_simulator.sinks.factory import create_sink
    if not cfg.sink_configs:
        # Fallback: console sink if no sinks defined
        from iot_simulator.sinks.console import ConsoleSink
        sim.add_sink(ConsoleSink(rate_hz=1.0))
    else:
        for sink_dict in cfg.sink_configs:
            sink = create_sink(sink_dict)
            sim.add_sink(sink)

    print(f"  Total sensors: {sim.sensor_count}\n")

    # --- Run ---
    duration = cfg.duration_s
    sim.run(duration_s=duration)


if __name__ == "__main__":
    main()
