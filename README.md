# GRAM Price Bots

Nine independent Telegram price bots for GRAM meme coin communities. Each bot
publishes scheduled prices, detects buy and sell activity from its configured
STON.fi or DeDust pool, and provides a private inline control menu.

| Folder | Token | Default channel | Alert source |
| --- | --- | --- | --- |
| [`utya`](utya/) | UTYA | [@utyaprices](https://t.me/utyaprices) | STON.fi |
| [`redo`](redo/) | REDO | [@redopricess](https://t.me/redopricess) | STON.fi |
| [`scat`](scat/) | SCAT | [@ScaredCatsPrice](https://t.me/ScaredCatsPrice) | STON.fi |
| [`yoda`](yoda/) | YODA | [@yodaprices](https://t.me/yodaprices) | DeDust |
| [`cherry`](cherry/) | CHERRY | [@cherryprices](https://t.me/cherryprices) | STON.fi |
| [`mtonga`](mtonga/) | MTONGA | [@mtongaprices](https://t.me/mtongaprices) | DeDust |
| [`groyp`](groyp/) | GROYP | [@groypprices](https://t.me/groypprices) | STON.fi |
| [`gramming`](gramming/) | GRAMMING | [@grammingprices](https://t.me/grammingprices) | DeDust |
| [`grm`](grm/) | GRM | [@GRM_prices](https://t.me/GRM_prices) | STON.fi |

## Repository layout

Every token folder is a self-contained deployment with:

- `main.py` - the complete bot implementation;
- `.env.example` - a credential-free configuration template;
- `test_price_alert_reliability.py` - token-specific reliability tests;
- `requirements.txt` - runtime dependency declaration;
- `start.ps1` and `start.sh` - local launchers;
- `<token>-price-bot.service` - an example systemd unit.

The services intentionally remain independent. A provider failure, restart, or
configuration change for one token does not stop the other eight bots.

## Quick start

Choose one token folder, create its private `.env`, and run it with Python 3.10
or newer. Python 3.12 is the production baseline.

```bash
git clone https://github.com/dev-nic-codes/gram-price-bots.git
cd gram-price-bots/utya
cp .env.example .env
# Edit .env before starting.
python3 main.py
```

On Windows:

```powershell
git clone https://github.com/dev-nic-codes/gram-price-bots.git
cd gram-price-bots\utya
Copy-Item .env.example .env
# Edit .env before starting.
.\start.ps1
```

`BOT_TOKEN`, `CONTROL_USER_ID`, and `ALLOWED_CONTROL_USER_IDS` must be set to
your own values. Never commit `.env`, state, logs, caches, or Telegram tokens.

## Controls and reliability

The private menu can change posting status, destination, interval, decimal
precision, displayed percentage fields, buy/sell thresholds, alert
destinations, and alert templates. Allowed users are validated on every update
and callback, not only by hiding the menu buttons.

Alert ingestion uses durable STON.fi block or DeDust logical-time cursors.
Telegram delivery uses an outbox so a temporary send failure does not advance
past an undelivered event. State is written atomically, events are deduplicated,
and HTTP 429 responses trigger backoff instead of tight retry loops.

## Validation

Run the complete repository suite:

```bash
python scripts/check_all.py
```

Or test one bot directly:

```bash
cd redo
python -m unittest -v test_price_alert_reliability.py
python -m py_compile main.py test_price_alert_reliability.py
```

GitHub Actions runs every token suite independently on Linux and Windows.

## Deployment

Each token folder contains an example systemd unit. Adjust its user and path,
copy it to `/etc/systemd/system`, then enable it. Keep each bot in its own
working directory so `.env`, `state.json`, logs, and caches stay isolated.

## External services

Market and transaction data comes from CoinGecko, DexScreener, STON.fi,
DeDust, TON Center, and optionally TONAPI. Availability and rate limits remain
outside the application. The bots validate responses, retain the last usable
state, and retry transient failures, but cannot guarantee an external provider
will always be reachable or correct.

## License

Copyright (c) 2026 Nic. All rights reserved. This repository is
source-available, not open source. See [LICENSE](LICENSE).
