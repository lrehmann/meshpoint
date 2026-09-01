"""Tests for ``TcpApiConfig``: defaults, YAML merge, and section registration."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.config import AppConfig, TcpApiConfig, _apply_yaml


class TestTcpApiConfigDefaults(unittest.TestCase):
    """Off by default and backward-compatible."""

    def test_disabled_by_default(self) -> None:
        self.assertFalse(TcpApiConfig().enabled)

    def test_default_port_is_4403(self) -> None:
        self.assertEqual(TcpApiConfig().port, 4403)

    def test_default_host_binds_all_interfaces(self) -> None:
        self.assertEqual(TcpApiConfig().host, "0.0.0.0")

    def test_mdns_and_forward_encrypted_on_by_default(self) -> None:
        cfg = TcpApiConfig()
        self.assertTrue(cfg.mdns)
        self.assertTrue(cfg.forward_encrypted)

    def test_appconfig_has_tcp_api_section(self) -> None:
        self.assertIsInstance(AppConfig().tcp_api, TcpApiConfig)


class TestTcpApiConfigYamlMerge(unittest.TestCase):
    """The section is wired into _apply_yaml's section_map."""

    def _merge(self, body: str) -> AppConfig:
        cfg = AppConfig()
        with tempfile.NamedTemporaryFile(
            "w", suffix=".yaml", delete=False
        ) as fh:
            fh.write(body)
            path = Path(fh.name)
        try:
            _apply_yaml(cfg, path)
        finally:
            path.unlink()
        return cfg

    def test_yaml_overrides_apply(self) -> None:
        cfg = self._merge(
            "tcp_api:\n  enabled: true\n  port: 4500\n  mdns: false\n"
        )
        self.assertTrue(cfg.tcp_api.enabled)
        self.assertEqual(cfg.tcp_api.port, 4500)
        self.assertFalse(cfg.tcp_api.mdns)

    def test_absent_section_keeps_defaults(self) -> None:
        cfg = self._merge("radio:\n  region: EU_868\n")
        self.assertFalse(cfg.tcp_api.enabled)
        self.assertEqual(cfg.tcp_api.port, 4403)


if __name__ == "__main__":
    unittest.main()
