"""Tests for iot_simulator.__main__ - CLI entry point."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from iot_simulator.__main__ import (
    _SAMPLE_CONFIG,
    _cmd_init_config,
    _cmd_list_industries,
    _cmd_list_sensors,
    _cmd_list_sinks,
    main,
)

# -----------------------------------------------------------------------
# main() dispatch
# -----------------------------------------------------------------------


class TestMainDispatch:
    """CLI argument parsing and sub-command dispatch."""

    def test_no_args_prints_help(self, capsys: pytest.CaptureFixture[str]) -> None:
        main([])
        out = capsys.readouterr().out
        assert "usage" in out.lower() or "commands" in out.lower()

    def test_help_flag(self) -> None:
        with pytest.raises(SystemExit) as exc_info:
            main(["--help"])
        assert exc_info.value.code == 0

    def test_list_industries(self, capsys: pytest.CaptureFixture[str]) -> None:
        main(["list-industries"])
        out = capsys.readouterr().out
        assert "Industry" in out
        assert "TOTAL" in out

    def test_list_sinks(self, capsys: pytest.CaptureFixture[str]) -> None:
        main(["list-sinks"])
        out = capsys.readouterr().out
        assert "console" in out
        assert "kafka" in out

    def test_list_sensors_valid(self, capsys: pytest.CaptureFixture[str]) -> None:
        main(["list-sensors", "mining"])
        out = capsys.readouterr().out
        assert "mining" in out
        assert "Name" in out

    def test_list_sensors_invalid(self, capsys: pytest.CaptureFixture[str]) -> None:
        with pytest.raises(SystemExit) as exc_info:
            main(["list-sensors", "nonexistent_industry"])
        assert exc_info.value.code == 1

    def test_init_config_stdout(self, capsys: pytest.CaptureFixture[str]) -> None:
        main(["init-config"])
        out = capsys.readouterr().out
        assert "simulator:" in out
        assert "sinks:" in out

    def test_init_config_to_file(self, tmp_path: Path) -> None:
        outfile = tmp_path / "test_config.yaml"
        main(["init-config", "--output", str(outfile)])
        assert outfile.exists()
        content = outfile.read_text()
        assert "simulator:" in content

    def test_backward_compat_injects_run(self) -> None:
        """When first arg is a flag (not a subcommand), 'run' is injected."""
        with patch("iot_simulator.__main__._cmd_run") as mock_run:
            main(["--industries", "mining", "--duration", "0.1"])
            mock_run.assert_called_once()

    def test_run_subcommand_dispatches(self) -> None:
        with patch("iot_simulator.__main__._cmd_run") as mock_run:
            main(["run", "--industries", "mining", "--duration", "0.1"])
            mock_run.assert_called_once()


# -----------------------------------------------------------------------
# list-industries
# -----------------------------------------------------------------------


class TestListIndustries:
    """_cmd_list_industries output."""

    def test_output_format(self, capsys: pytest.CaptureFixture[str]) -> None:
        _cmd_list_industries()
        out = capsys.readouterr().out
        assert "Industry" in out
        assert "Sensors" in out
        assert "TOTAL" in out
        # Should list at least mining
        assert "mining" in out


# -----------------------------------------------------------------------
# list-sinks
# -----------------------------------------------------------------------


class TestListSinks:
    """_cmd_list_sinks output."""

    def test_output_includes_all_sink_types(self, capsys: pytest.CaptureFixture[str]) -> None:
        _cmd_list_sinks()
        out = capsys.readouterr().out
        assert "console" in out
        assert "(built-in)" in out
        assert "kafka" in out


# -----------------------------------------------------------------------
# list-sensors
# -----------------------------------------------------------------------


class TestListSensors:
    """_cmd_list_sensors output."""

    def test_valid_industry(self, capsys: pytest.CaptureFixture[str]) -> None:
        _cmd_list_sensors("mining")
        out = capsys.readouterr().out
        assert "mining" in out

    def test_invalid_industry_exits(self) -> None:
        with pytest.raises(SystemExit) as exc_info:
            _cmd_list_sensors("bogus_industry_name")
        assert exc_info.value.code == 1


# -----------------------------------------------------------------------
# init-config
# -----------------------------------------------------------------------


class TestInitConfig:
    """_cmd_init_config output."""

    def test_to_stdout(self, capsys: pytest.CaptureFixture[str]) -> None:
        _cmd_init_config(None)
        out = capsys.readouterr().out
        assert out.strip() == _SAMPLE_CONFIG.strip()

    def test_to_file(self, tmp_path: Path) -> None:
        outfile = tmp_path / "sub" / "config.yaml"
        _cmd_init_config(str(outfile))
        assert outfile.exists()
        assert "simulator:" in outfile.read_text()


# -----------------------------------------------------------------------
# run command
# -----------------------------------------------------------------------


class TestRunCommand:
    """_cmd_run with quick mode and config mode."""

    def test_run_quick_with_console(self) -> None:
        """run --industries mining -s console --duration 0.1 should complete."""
        main(["run", "--industries", "mining", "-s", "console", "--duration", "0.1"])

    def test_run_quick_with_file_sink(self, tmp_path: Path) -> None:
        main(
            [
                "run",
                "--industries",
                "mining",
                "-s",
                "file",
                "--output-dir",
                str(tmp_path),
                "--output-format",
                "json",
                "--duration",
                "0.1",
            ]
        )
        # Some output files should exist
        files = list(tmp_path.glob("*"))
        assert len(files) >= 1

    def test_run_quick_console_and_file(self, tmp_path: Path) -> None:
        main(
            [
                "run",
                "-s",
                "console",
                "-s",
                "file",
                "--output-dir",
                str(tmp_path),
                "--duration",
                "0.1",
            ]
        )

    def test_run_from_config(self, tmp_path: Path) -> None:
        config_content = """\
simulator:
  industries: [mining]
  update_rate_hz: 5.0
  duration_s: 0.1

sinks:
  - type: console
    fmt: text
    rate_hz: 1.0
"""
        config_file = tmp_path / "sim.yaml"
        config_file.write_text(config_content)
        main(["run", "--config", str(config_file)])

    def test_run_from_config_duration_override(self, tmp_path: Path) -> None:
        config_content = """\
simulator:
  industries: [mining]
  update_rate_hz: 5.0
  duration_s: 60

sinks:
  - type: console
    rate_hz: 1.0
"""
        config_file = tmp_path / "sim.yaml"
        config_file.write_text(config_content)
        # --duration overrides config's duration_s
        main(["run", "--config", str(config_file), "--duration", "0.1"])

    def test_run_from_config_no_sinks_uses_console(self, tmp_path: Path) -> None:
        config_content = """\
simulator:
  industries: [mining]
  update_rate_hz: 5.0
  duration_s: 0.1
"""
        config_file = tmp_path / "sim.yaml"
        config_file.write_text(config_content)
        main(["run", "--config", str(config_file)])

    def test_run_with_rotation(self, tmp_path: Path) -> None:
        main(
            [
                "run",
                "-s",
                "file",
                "--output-dir",
                str(tmp_path),
                "--rotation",
                "1s",
                "--duration",
                "0.1",
            ]
        )

    def test_run_default_log_level(self) -> None:
        """Default log level is INFO."""
        with patch("iot_simulator.__main__._cmd_run") as mock_run:
            main(["run", "--duration", "0.1"])
            args = mock_run.call_args[0][0]
            assert args.log_level == "INFO"

    def test_run_debug_log_level(self) -> None:
        with patch("iot_simulator.__main__._cmd_run") as mock_run:
            main(["run", "--log-level", "DEBUG", "--duration", "0.1"])
            args = mock_run.call_args[0][0]
            assert args.log_level == "DEBUG"
