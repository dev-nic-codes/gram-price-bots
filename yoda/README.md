# YODA Price Bot

Standalone Telegram price publisher and trade-alert bot for YODA. The public default channel is `@yodaprices`; trade alerts are read from the configured DeDust pool.

## Run

1. Copy `.env.example` to `.env` and enter your BotFather token and administrator IDs.
2. Start with `./start.ps1` on Windows or `./start.sh` on Linux.
3. Run the reliability tests with `python -m unittest -v test_price_alert_reliability.py`.

Runtime state, logs, caches, and credentials remain local and are ignored by Git.
