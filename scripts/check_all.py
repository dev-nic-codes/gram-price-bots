from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BOT_FOLDERS = ("utya", "redo", "scat", "yoda", "cherry", "mtonga", "groyp", "gramming", "grm")


def run(command: list[str], *, cwd: Path) -> None:
    print(f"\n[{cwd.name}] {' '.join(command[1:])}", flush=True)
    subprocess.run(command, cwd=cwd, check=True)


def main() -> int:
    for folder in BOT_FOLDERS:
        bot_dir = ROOT / folder
        run([sys.executable, "-m", "unittest", "-v", "test_price_alert_reliability.py"], cwd=bot_dir)
        run([sys.executable, "-m", "py_compile", "main.py", "test_price_alert_reliability.py"], cwd=bot_dir)

    print(f"\nValidated {len(BOT_FOLDERS)} price bots.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
