import json
import unittest
from pathlib import Path


class TestInputFilesExist(unittest.TestCase):
    def setUp(self) -> None:
        self.project_root = Path(__file__).resolve().parents[1]
        self.config_dir = self.project_root / "config"

    def _assert_source_file_exists(self, config_filename: str) -> None:
        config_path = self.config_dir / config_filename
        self.assertTrue(config_path.exists(), f"Config file missing: {config_path}")

        with config_path.open("r", encoding="utf-8") as handle:
            config = json.load(handle)

        source_path = config.get("source", {}).get("path")
        self.assertIsNotNone(source_path, f"Missing source.path in {config_path}")

        input_path = (self.project_root / source_path).resolve()
        self.assertTrue(
            input_path.exists(),
            f"Input file missing for {config_filename}: {input_path}",
        )

    def test_call_input_exists(self) -> None:
        self._assert_source_file_exists("call.json")

    def test_edetail_input_exists(self) -> None:
        self._assert_source_file_exists("edetail.json")

    def test_events_input_exists(self) -> None:
        self._assert_source_file_exists("events.json")

    def test_vae_input_exists(self) -> None:
        self._assert_source_file_exists("vae.json")
