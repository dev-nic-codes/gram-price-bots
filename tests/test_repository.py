from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BOT_FOLDERS = ("utya", "redo", "scat", "yoda", "cherry", "mtonga", "groyp", "gramming", "grm")
REQUIRED_FILES = {
    "main.py",
    "test_price_alert_reliability.py",
    "README.md",
    ".env.example",
    "requirements.txt",
    "start.ps1",
    "start.sh",
}
TOKEN_PATTERN = re.compile(r"\b\d{8,10}:[A-Za-z0-9_-]{30,}\b")


class RepositoryLayoutTests(unittest.TestCase):
    def test_every_bot_folder_is_self_contained(self) -> None:
        for folder in BOT_FOLDERS:
            with self.subTest(folder=folder):
                names = {path.name for path in (ROOT / folder).iterdir()}
                expected = REQUIRED_FILES | {f"{folder}-price-bot.service"}
                self.assertTrue(expected <= names, expected - names)

    def test_environment_examples_require_private_configuration(self) -> None:
        for folder in BOT_FOLDERS:
            with self.subTest(folder=folder):
                sample = (ROOT / folder / ".env.example").read_text(encoding="utf-8")
                self.assertIn("BOT_TOKEN=REPLACE_WITH_BOTFATHER_TOKEN", sample)
                self.assertIn("ALLOWED_CONTROL_USER_IDS=", sample)
                self.assertFalse(TOKEN_PATTERN.search(sample))

    def test_source_files_do_not_contain_bot_tokens(self) -> None:
        for folder in BOT_FOLDERS:
            for path in (ROOT / folder).glob("*.py"):
                with self.subTest(path=path.relative_to(ROOT)):
                    source = path.read_text(encoding="utf-8")
                    self.assertFalse(TOKEN_PATTERN.search(source))


if __name__ == "__main__":
    unittest.main()
