"""CLI entry point for the IoT Data Simulator.

Usage::

    iot-simulator run --industries mining --duration 10
    iot-simulator run -i mining -s console -s file -o ./data
    iot-simulator run --config simulator.yaml
    iot-simulator list-industries
    iot-simulator list-sensors mining
    iot-simulator list-sinks
    iot-simulator init-config --output simulator.yaml
"""

from __future__ import annotations

import argparse
import logging
import sys
import textwrap

# ---------------------------------------------------------------------------
# Extras mapping for list-sinks display
# ---------------------------------------------------------------------------
_SINK_EXTRAS: dict[str, str | None] = {
    "console": None,
    "callback": None,
    "kafka": "kafka",
    "file": "file",
    "database": "database",
    "webhook": "webhook",
    "delta": "delta",
    "azure_iot": "cloud",
    "aws_iot": "cloud",
    "zerobus": "zerobus",
}

# ---------------------------------------------------------------------------
# Sample YAML config template for init-config
# ---------------------------------------------------------------------------
_SAMPLE_CONFIG = """\
# IoT Data Simulator configuration
# Docs: see iot_simulator/README.md

simulator:
  industries: [mining, utilities]     # built-in industry names (run 'iot-simulator list-industries')
  update_rate_hz: 2.0                 # how many value batches per second
  # duration_s: 60                    # optional: auto-stop after N seconds
  # log_level: INFO                   # DEBUG, INFO, WARNING, ERROR

# Optional: define your own sensors
# custom_sensors:
#   industry: smart_office            # grouping label
#   sensors:
#     - name: room_temperature
#       sensor_type: temperature
#       unit: "°C"
#       min_value: 15.0
#       max_value: 35.0
#       nominal_value: 22.0
#       noise_std: 0.3
#       cyclic: true
#       cycle_period_seconds: 3600

# Sinks define where data is sent. Each has independent throughput control.
sinks:
  - type: console
    fmt: text                         # text or json
    rate_hz: 1.0                      # flush once per second

  - type: file
    path: ./output
    format: csv                       # csv, json, or parquet
    # rotation: 1h                    # rotate files: 30s, 5m, 1h, 1d
    rate_hz: 1.0
    batch_size: 500

  # - type: kafka
  #   bootstrap_servers: localhost:9092
  #   topic: iot-data
  #   compression: snappy
  #   rate_hz: 2.0
  #   batch_size: 50

  # - type: database
  #   connection_string: sqlite+aiosqlite:///sensor_data.db
  #   table: sensor_data
  #   rate_hz: 1.0
  #   batch_size: 200

  # - type: webhook
  #   url: https://example.com/ingest
  #   rate_hz: 0.5
  #   batch_size: 100
  #   headers:
  #     Authorization: Bearer my-token

  # - type: delta
  #   table_path: /tmp/delta/sensors
  #   rate_hz: 0.2
  #   batch_size: 1000

  # - type: zerobus
  #   server_endpoint: "1234567890123456.zerobus.us-west-2.cloud.databricks.com"
  #   table_name: catalog.schema.sensor_data
  #   databricks_profile: DEFAULT      # reads ~/.databrickscfg (host, client_id, client_secret)
  #   # workspace_url: ...             # optional: override profile host
  #   # client_id: ...                 # optional: override profile client_id
  #   # client_secret: ...             # optional: override profile client_secret
  #   record_type: json                # json (default) or proto
  #   rate_hz: 1.0
  #   batch_size: 100
"""


# ======================================================================
# Main entry point
# ======================================================================


def main(argv: list[str] | None = None) -> None:
    epilog = textwrap.dedent("""\
        examples:
          iot-simulator run --industries mining --duration 10
          iot-simulator run -i mining -s console -s file -o ./data --output-format csv
          iot-simulator run -i mining --sink file --output-format parquet --rotation 1h
          iot-simulator run --config simulator.yaml
          iot-simulator list-industries
          iot-simulator list-sensors mining
          iot-simulator list-sinks
          iot-simulator init-config --output simulator.yaml
    """)

    parser = argparse.ArgumentParser(
        prog="iot-simulator",
        description="Generate realistic IoT sensor data and push to pluggable sinks.",
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", title="commands")

    # -- run ---------------------------------------------------------------
    run_parser = subparsers.add_parser(
        "run",
        help="Run the simulator (generate data and send to sinks).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            examples:
              iot-simulator run --industries mining --duration 10
              iot-simulator run -i mining -s console -s file -o ./data
              iot-simulator run --config simulator.yaml --duration 120
        """),
    )
    run_parser.add_argument(
        "--config",
        "-c",
        type=str,
        default=None,
        help="Path to YAML config file. When set, --sink/--output-* flags are ignored.",
    )
    run_parser.add_argument(
        "--industries",
        "-i",
        nargs="*",
        default=None,
        help="Built-in industry names (e.g. mining utilities). Default: mining.",
    )
    run_parser.add_argument(
        "--rate",
        type=float,
        default=2.0,
        help="Generator tick rate in Hz (default: 2.0).",
    )
    run_parser.add_argument(
        "--duration",
        "-d",
        type=float,
        default=None,
        help="Run duration in seconds (default: indefinite, Ctrl-C to stop).",
    )
    run_parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )
    # Sink selection (without --config)
    run_parser.add_argument(
        "--sink",
        "-s",
        action="append",
        dest="sinks",
        choices=["console", "file"],
        help="Sink(s) to enable (repeatable). Default: console. Example: -s console -s file",
    )
    run_parser.add_argument(
        "--format",
        type=str,
        default="text",
        choices=["text", "json"],
        help="Console sink output format (default: text).",
    )
    run_parser.add_argument(
        "--output-dir",
        "-o",
        type=str,
        default="./output",
        help="Output directory for the file sink (default: ./output).",
    )
    run_parser.add_argument(
        "--output-format",
        type=str,
        default="csv",
        choices=["csv", "json", "parquet"],
        help="File sink format (default: csv).",
    )
    run_parser.add_argument(
        "--rotation",
        type=str,
        default=None,
        help="File rotation interval, e.g. 1h, 30m, 60s (default: none).",
    )

    # -- list-industries ---------------------------------------------------
    subparsers.add_parser(
        "list-industries",
        help="List all built-in industries and their sensor counts.",
    )

    # -- list-sinks --------------------------------------------------------
    subparsers.add_parser(
        "list-sinks",
        help="List all available sink types and install instructions.",
    )

    # -- list-sensors ------------------------------------------------------
    sensors_parser = subparsers.add_parser(
        "list-sensors",
        help="List all sensors for a given industry.",
    )
    sensors_parser.add_argument(
        "industry",
        type=str,
        help="Industry name (run 'list-industries' to see available names).",
    )

    # -- init-config -------------------------------------------------------
    init_parser = subparsers.add_parser(
        "init-config",
        help="Generate a sample YAML configuration file.",
    )
    init_parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=None,
        help="Write config to this file instead of stdout.",
    )

    # -- Pre-check for backward compatibility --------------------------------
    # If the first arg is NOT a known subcommand but looks like a flag
    # (e.g. --industries, --config, -i, -d), inject "run" as the subcommand
    # so that `iot-simulator --industries mining` keeps working.
    _known_commands = {"run", "list-industries", "list-sinks", "list-sensors", "init-config"}
    raw_args = argv if argv is not None else sys.argv[1:]
    if raw_args and raw_args[0] not in _known_commands and raw_args[0] not in ("-h", "--help"):
        raw_args = ["run", *list(raw_args)]

    args = parser.parse_args(raw_args)

    if args.command is None:
        parser.print_help()
        return

    # -- Dispatch ----------------------------------------------------------
    if args.command == "run":
        _cmd_run(args)
    elif args.command == "list-industries":
        _cmd_list_industries()
    elif args.command == "list-sinks":
        _cmd_list_sinks()
    elif args.command == "list-sensors":
        _cmd_list_sensors(args.industry)
    elif args.command == "init-config":
        _cmd_init_config(args.output)
    else:
        parser.print_help()


# ======================================================================
# Command implementations
# ======================================================================


def _cmd_run(args: argparse.Namespace) -> None:
    """Execute the simulator."""
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(name)-30s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.config:
        _run_from_config(args.config, args.duration)
    else:
        sinks = args.sinks or ["console"]
        _run_quick(
            industries=args.industries or ["mining"],
            rate=args.rate,
            duration=args.duration,
            enabled_sinks=sinks,
            console_fmt=args.format,
            output_dir=args.output_dir,
            output_format=args.output_format,
            rotation=args.rotation,
        )


def _run_from_config(config_path: str, duration_override: float | None) -> None:
    """Load YAML config and run the simulator."""
    from iot_simulator.config import load_yaml_config
    from iot_simulator.simulator import Simulator
    from iot_simulator.sinks.factory import create_sink

    cfg = load_yaml_config(config_path)
    logging.getLogger().setLevel(getattr(logging, cfg.log_level, logging.INFO))

    sim = Simulator(
        industries=cfg.industries or None,
        custom_sensors=cfg.custom_sensors or None,
        custom_industry=cfg.custom_industry,
        update_rate_hz=cfg.update_rate_hz,
    )

    if not cfg.sink_configs:
        from iot_simulator.sinks.console import ConsoleSink

        sim.add_sink(ConsoleSink(rate_hz=1.0))
    else:
        for sink_dict in cfg.sink_configs:
            sink = create_sink(sink_dict)
            sim.add_sink(sink)

    duration = duration_override if duration_override is not None else cfg.duration_s
    sim.run(duration_s=duration)


def _run_quick(
    industries: list[str],
    rate: float,
    duration: float | None,
    enabled_sinks: list[str],
    console_fmt: str,
    output_dir: str,
    output_format: str,
    rotation: str | None,
) -> None:
    """Run with CLI-specified sinks (console and/or file)."""
    from iot_simulator.simulator import Simulator

    sim = Simulator(industries=industries, update_rate_hz=rate)

    if "console" in enabled_sinks:
        from iot_simulator.sinks.console import ConsoleSink

        sim.add_sink(ConsoleSink(fmt=console_fmt, rate_hz=1.0))

    if "file" in enabled_sinks:
        from iot_simulator.sinks.file import FileSink

        sim.add_sink(
            FileSink(
                path=output_dir,
                format=output_format,
                rotation=rotation,
                rate_hz=1.0,
                batch_size=500,
            )
        )

    if not sim._runners:
        # Fallback if somehow no sinks matched
        from iot_simulator.sinks.console import ConsoleSink

        sim.add_sink(ConsoleSink(fmt=console_fmt, rate_hz=1.0))

    sim.run(duration_s=duration)


# -- list-industries -------------------------------------------------------


def _cmd_list_industries() -> None:
    from iot_simulator.sensor_models import IndustryType, get_industry_sensors

    print(f"\n{'Industry':<25} {'Sensors':>7}")
    print("-" * 34)
    total = 0
    for industry in IndustryType:
        sims = get_industry_sensors(industry)
        count = len(sims)
        total += count
        print(f"{industry.value:<25} {count:>7}")
    print("-" * 34)
    print(f"{'TOTAL':<25} {total:>7}")
    print()


# -- list-sinks ------------------------------------------------------------


def _cmd_list_sinks() -> None:
    from iot_simulator.sinks.factory import _SINK_REGISTRY

    print(f"\n{'Sink Type':<14} {'Class':<20} {'Install Extra'}")
    print("-" * 62)
    for name, (_module_path, class_name) in _SINK_REGISTRY.items():
        extra = _SINK_EXTRAS.get(name)
        extra_str = "(built-in)" if extra is None else f"pip install iot-data-simulator[{extra}]"
        print(f"{name:<14} {class_name:<20} {extra_str}")
    print()


# -- list-sensors -----------------------------------------------------------


def _cmd_list_sensors(industry_name: str) -> None:
    from iot_simulator.sensor_models import IndustryType, get_industry_sensors

    try:
        industry = IndustryType(industry_name)
    except ValueError:
        valid = ", ".join(i.value for i in IndustryType)
        print(f"Error: unknown industry '{industry_name}'.")
        print(f"Available industries: {valid}")
        sys.exit(1)

    sims = get_industry_sensors(industry)
    print(f"\nSensors for '{industry_name}' ({len(sims)} sensors):\n")
    print(f"{'Name':<36} {'Type':<14} {'Unit':<8} {'Min':>10} {'Max':>10} {'Nominal':>10}")
    print("-" * 92)
    for sim in sims:
        cfg = sim.config
        stype = cfg.sensor_type.value if hasattr(cfg.sensor_type, "value") else str(cfg.sensor_type)
        print(
            f"{cfg.name:<36} {stype:<14} {cfg.unit:<8} "
            f"{cfg.min_value:>10.2f} {cfg.max_value:>10.2f} {cfg.nominal_value:>10.2f}"
        )
    print()


# -- init-config ------------------------------------------------------------


def _cmd_init_config(output_path: str | None) -> None:
    if output_path:
        from pathlib import Path

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_text(_SAMPLE_CONFIG)
        print(f"Sample config written to {output_path}")
    else:
        print(_SAMPLE_CONFIG)


# ======================================================================
if __name__ == "__main__":
    main()
