from __future__ import annotations

import html as html_lib
import http.client
import json
import os
import re
import signal
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


ROOT = Path(__file__).resolve().parent
ENV_PATH = ROOT / ".env"
STATE_PATH = ROOT / "state.json"
LOG_PATH = ROOT / "yoda-price-bot.log"
PRICE_CACHE_PATH = ROOT / "price_cache.json"
WALLET_DNS_CACHE_PATH = ROOT / "wallet_dns_cache.json"
SHARED_MEME_PRICE_CACHE_PATH = Path(
    os.getenv(
        "MEME_PRICE_CACHE_PATH",
        str(ROOT.parent / "meme-price-bot" / "output" / "price_cache.json"),
    )
)

TOKEN_TICKER = "YODA"
COINGECKO_ID = ""
COINGECKO_MARKET_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
    f"?vs_currency=usd&ids={COINGECKO_ID},the-open-network"
    "&price_change_percentage=1h,24h,7d&precision=full"
)
DEXSCREENER_PAIR_URL = "https://api.dexscreener.com/latest/dex/pairs/ton/{pool_address}"
TONAPI_POOL_EVENTS_URL = "https://tonapi.io/v2/accounts/{pool_address}/events?limit={limit}"
TONAPI_ACCOUNT_DNS_BACKRESOLVE_URL = "https://tonapi.io/v2/accounts/{account_id}/dns/backresolve"
STON_LATEST_BLOCK_URL = "https://api.ston.fi/export/dexscreener/v1/latest-block"
STON_EVENTS_URL = (
    "https://api.ston.fi/export/dexscreener/v1/events"
    "?fromBlock={from_block}&toBlock={to_block}"
)
STON_PAIR_URL = "https://api.ston.fi/export/dexscreener/v1/pair/{pool_address}"
DEDUST_TRADES_URL = "https://api.dedust.io/v2/pools/{pool_address}/trades?page_size={page_size}&after_lt={after_lt}"
TONCENTER_LATEST_TRANSACTION_URL = "https://toncenter.com/api/v3/transactions?account={pool_address}&limit=1&sort=desc"
DEFAULT_ALERT_POOL_ADDRESS = "EQBjBklMBO8hh8cFSeyVNB0GEimYOO9IZ6WDLXjuL45Dsbxu"
DEFAULT_ALERT_POOL_LABEL = "DeDust YODA/GRAM"
DEFAULT_ALERT_DEX = "dedust"
DEFAULT_ALERT_THRESHOLD_USD = 1000.0
DEFAULT_ALERT_INTERVAL_SECONDS = 20
DEFAULT_ALERT_FETCH_LIMIT = 25
YODA_MASTER_ADDRESS = "0:bbbee28460b742ef6621516b77014540f8e8bae90d43c531d71fbafaa57695e7"
YODA_FRIENDLY_MASTER_ADDRESS = "EQC7vuKEYLdC72YhUWt3AUVA-Oi66Q1DxTHXH7r6pXaV50j7"
TOKEN_DECIMALS = 9
STON_NATIVE_ASSET_ADDRESS = "EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c"
STON_ALERT_BLOCK_CHUNK = 1_000
STON_ALERT_MAX_CHUNKS_PER_CHECK = 5
DEDUST_ALERT_PAGE_SIZE = 100
DEDUST_ALERT_MAX_PAGES_PER_CHECK = 5

DEFAULT_INTERVAL_SECONDS = 60
DEFAULT_CHANNEL = "@yodaprices"
DEFAULT_USER_AGENT = "yoda-price-bot/2.0"
DEFAULT_TIMEOUT_SECONDS = 20
DEFAULT_RETRY_SECONDS = 15
DEFAULT_COMMAND_TIMEOUT_SECONDS = 25
DEFAULT_LOG_LINES = 15
DEFAULT_DECIMAL_PLACES = 6
CONTROL_POLL_TIMEOUT_SECONDS = 2
EXTERNAL_DATA_TIMEOUT_SECONDS = 5
WALLET_DNS_POSITIVE_TTL_SECONDS = 24 * 60 * 60
WALLET_DNS_NEGATIVE_TTL_SECONDS = 60 * 60
SHARED_GRAM_PRICE_MAX_AGE_SECONDS = 15 * 60

ALERT_TEMPLATE_PLACEHOLDERS = {
    "[SIDE]",
    "[PRICE]",
    "[USD_AMOUNT]",
    "[GRAM_AMOUNT]",
    "[GRAM_USD]",
    "[GRAM_SIZE]",
    "[YODA_AMOUNT]",
    "[TOKEN_AMOUNT]",
    "[WALLET]",
    "[SHORT_WALLET]",
    "[SHORT_ADDRESS]",
    "[WALLET_SHORT]",
    "[WALLET_LINK]",
    "[WALLET_SHORT_LINK]",
    "[WALLET_URL]",
    "[TX_HASH]",
    "[TX_URL]",
    "[POOL]",
    "[POOL_ADDRESS]",
    "[DATE]",
    "[TIME]",
    "[UTC_TIME]",
    "[DATETIME]",
}
ALERT_TEMPLATE_PLACEHOLDER_ALIASES = {
    "[wallet_link]": "[WALLET_LINK]",
    "[short_wallet]": "[SHORT_WALLET]",
    "[wallet_short]": "[SHORT_WALLET]",
    "[gram_amount]": "[GRAM_AMOUNT]",
    "[gram_usd]": "[GRAM_USD]",
    "[gram_size]": "[GRAM_SIZE]",
    "[token_amount]": "[TOKEN_AMOUNT]",
    "[short_address]": "[SHORT_ADDRESS]",
    "[utc_time]": "[UTC_TIME]",
}
ALERT_TEMPLATE_TOKEN_RE = re.compile(r"\[[A-Za-z0-9_]+\]")
ALERT_TEMPLATE_DYNAMIC_LINK_RE = re.compile(
    r'<a\s+href=(["\'])([^"\']*\[[A-Za-z0-9_]+\][^"\']*)\1>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
ALERT_TEMPLATE_ESCAPED_WALLET_LINK_RE = re.compile(
    r'&lt;a\s+href=(["\'])\[WALLET_URL\]\1&gt;(.*?)&lt;/a&gt;',
    re.IGNORECASE | re.DOTALL,
)
MAX_ALERT_TEMPLATE_LENGTH = 3500

_STOP = False
_WALLET_DNS_CACHE: Optional[dict[str, dict[str, Any]]] = None
_STON_PAIR_LAYOUT_CACHE: dict[str, tuple[int, bool]] = {}


def create_ipv4_connection(
    address: tuple[str, int],
    timeout: object = socket._GLOBAL_DEFAULT_TIMEOUT,
    source_address: Optional[tuple[str, int]] = None,
) -> socket.socket:
    host, port = address
    addresses = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
    if not addresses:
        raise OSError(f"No IPv4 address found for {host}")
    ipv4_address = addresses[0][4]
    return socket.create_connection(ipv4_address, timeout, source_address)


class IPv4HTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self._create_connection = create_ipv4_connection


class IPv4HTTPSHandler(urllib.request.HTTPSHandler):
    def https_open(self, request: urllib.request.Request):
        context = getattr(self, "_context", None)
        if context is not None:
            return self.do_open(IPv4HTTPSConnection, request, context=context)
        return self.do_open(IPv4HTTPSConnection, request)


TELEGRAM_IPV4_OPENER = urllib.request.build_opener(IPv4HTTPSHandler())


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


@dataclass(frozen=True)
class EnvConfig:
    bot_token: str
    default_channel: str
    default_interval_seconds: int
    user_agent: str
    timeout_seconds: int
    retry_seconds: int
    command_timeout_seconds: int


@dataclass
class RuntimeConfig:
    bot_token: str
    channel: str
    interval_seconds: int
    decimal_places: int
    posting_enabled: bool
    user_agent: str
    timeout_seconds: int
    retry_seconds: int
    command_timeout_seconds: int


@dataclass(frozen=True)
class MarketSnapshot:
    price_usd: float
    change_1h_percent: Optional[float]
    change_24h_percent: Optional[float]
    change_7d_percent: Optional[float]
    gram_price_usd: Optional[float] = None


@dataclass(frozen=True)
class BuyAlertEvent:
    event_id: str
    wallet_address: str
    utya_amount: float
    usd_amount: float
    pool_label: str
    pool_address: str
    tx_hash: str
    timestamp: int
    happened_at: str
    gram_amount: float = 0.0
    wallet_dns: str = ""


@dataclass(frozen=True)
class SellAlertEvent:
    event_id: str
    wallet_address: str
    utya_amount: float
    usd_amount: float
    pool_label: str
    pool_address: str
    tx_hash: str
    timestamp: int
    happened_at: str
    gram_amount: float = 0.0
    wallet_dns: str = ""


def read_required_env(name: str) -> str:
    value = str(os.getenv(name, "")).strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def clamp_interval(seconds: int) -> int:
    return max(30, min(24 * 60 * 60, int(seconds)))


def clamp_decimal_places(value: int) -> int:
    return max(0, min(12, int(value)))


def clamp_alert_interval(seconds: int) -> int:
    return max(10, min(24 * 60 * 60, int(seconds)))


def load_env_config() -> EnvConfig:
    load_dotenv(ENV_PATH)
    return EnvConfig(
        bot_token=read_required_env("BOT_TOKEN"),
        default_channel=str(os.getenv("CHANNEL_USERNAME", DEFAULT_CHANNEL)).strip() or DEFAULT_CHANNEL,
        default_interval_seconds=clamp_interval(int(str(os.getenv("POST_INTERVAL_SECONDS", DEFAULT_INTERVAL_SECONDS)).strip() or DEFAULT_INTERVAL_SECONDS)),
        user_agent=str(os.getenv("USER_AGENT", DEFAULT_USER_AGENT)).strip() or DEFAULT_USER_AGENT,
        timeout_seconds=max(5, int(str(os.getenv("HTTP_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS)).strip() or DEFAULT_TIMEOUT_SECONDS)),
        retry_seconds=max(5, int(str(os.getenv("RETRY_DELAY_SECONDS", DEFAULT_RETRY_SECONDS)).strip() or DEFAULT_RETRY_SECONDS)),
        command_timeout_seconds=max(5, int(str(os.getenv("COMMAND_TIMEOUT_SECONDS", DEFAULT_COMMAND_TIMEOUT_SECONDS)).strip() or DEFAULT_COMMAND_TIMEOUT_SECONDS)),
    )


def parse_allowed_user_ids(raw: object) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for part in text.replace(";", ",").split(","):
        token = str(part or "").strip()
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def dedupe_text_values(values: object, *, limit: int) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in list(values or []):
        token = str(item or "").strip()
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out[-limit:]


def is_http_too_many_requests(exc: BaseException) -> bool:
    return isinstance(exc, urllib.error.HTTPError) and int(getattr(exc, "code", 0) or 0) == 429


def rate_limit_post_backoff_seconds(runtime: RuntimeConfig) -> int:
    return max(int(runtime.interval_seconds), 3 * 60)


def rate_limit_alert_backoff_seconds() -> int:
    return 10 * 60


def default_state(env: EnvConfig) -> dict[str, Any]:
    return {
        "last_update_id": 0,
        "control_chat_id": str(os.getenv("CONTROL_CHAT_ID", "")).strip(),
        "control_thread_id": str(os.getenv("CONTROL_THREAD_ID", "")).strip(),
        "control_user_id": str(os.getenv("CONTROL_USER_ID", "")).strip(),
        "allowed_user_ids": parse_allowed_user_ids(os.getenv("ALLOWED_CONTROL_USER_IDS", "")),
        "ui_sessions": {},
        "menu_message_id": 0,
        "menu_page": "home",
        "ui_notice": "",
        "ui_notice_at": "",
        "posting_enabled": True,
        "channel": env.default_channel,
        "interval_seconds": env.default_interval_seconds,
        "decimal_places": clamp_decimal_places(int(str(os.getenv("DECIMAL_PLACES", DEFAULT_DECIMAL_PLACES)).strip() or DEFAULT_DECIMAL_PLACES)),
        "show_change_1h": True,
        "show_change_24h": True,
        "show_change_7d": True,
        "buy_alerts_enabled": True,
        "buy_alert_channel": "",
        "buy_alert_threshold_usd": DEFAULT_ALERT_THRESHOLD_USD,
        "buy_alert_interval_seconds": DEFAULT_ALERT_INTERVAL_SECONDS,
        "buy_alert_pool_address": DEFAULT_ALERT_POOL_ADDRESS,
        "buy_alert_pool_label": DEFAULT_ALERT_POOL_LABEL,
        "show_buy_alert_wallet": True,
        "show_buy_alert_utya": True,
        "show_buy_alert_usd": True,
        "show_buy_alert_pool": True,
        "show_buy_alert_link": True,
        "buy_alert_template": "",
        "buy_alert_seen_event_ids": [],
        "buy_alert_seen_keys": [],
        "buy_alert_bootstrapped": False,
        "last_buy_alert_at": "",
        "last_buy_alert_text": "",
        "last_buy_alert_wallet": "",
        "sell_alerts_enabled": True,
        "sell_alert_channel": "",
        "sell_alert_threshold_usd": DEFAULT_ALERT_THRESHOLD_USD,
        "sell_alert_interval_seconds": DEFAULT_ALERT_INTERVAL_SECONDS,
        "sell_alert_pool_address": DEFAULT_ALERT_POOL_ADDRESS,
        "sell_alert_pool_label": DEFAULT_ALERT_POOL_LABEL,
        "show_sell_alert_wallet": True,
        "show_sell_alert_utya": True,
        "show_sell_alert_usd": True,
        "show_sell_alert_pool": True,
        "show_sell_alert_link": True,
        "sell_alert_template": "",
        "sell_alert_seen_event_ids": [],
        "sell_alert_seen_keys": [],
        "sell_alert_bootstrapped": False,
        "last_sell_alert_at": "",
        "last_sell_alert_text": "",
        "last_sell_alert_wallet": "",
        "ston_alert_cursor_block": 0,
        "ston_alert_outbox": [],
        "ston_alert_last_sync_at": "",
        "ston_alert_last_error": "",
        "dedust_alert_cursor_lt": 0,
        "dedust_alert_last_sync_at": "",
        "dedust_alert_last_error": "",
        "activity_log": [],
        "pending_inputs": {},
        "last_post_at": "",
        "last_post_text": "",
        "last_price": "",
        "last_change_1h": "",
        "last_change_24h": "",
        "last_change_7d": "",
        "last_error": "",
        "last_error_at": "",
        "last_command_at": "",
    }


def load_state(env: EnvConfig) -> dict[str, Any]:
    state = default_state(env)
    if STATE_PATH.exists():
        try:
            raw = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            raw = {}
        if isinstance(raw, dict):
            state.update(raw)
    state.pop("allowed_usernames", None)
    state["control_chat_id"] = str(state.get("control_chat_id", "") or "").strip()
    state["control_thread_id"] = str(state.get("control_thread_id", "") or "").strip()
    state["control_user_id"] = str(state.get("control_user_id", "") or "").strip()
    allowed_ids = []
    for item in list(state.get("allowed_user_ids") or []):
        token = str(item or "").strip()
        if token and token not in allowed_ids:
            allowed_ids.append(token)
    for item in parse_allowed_user_ids(os.getenv("ALLOWED_CONTROL_USER_IDS", "")):
        if item not in allowed_ids:
            allowed_ids.append(item)
    owner_id = str(state.get("control_user_id") or "").strip()
    if owner_id and owner_id not in allowed_ids:
        allowed_ids.insert(0, owner_id)
    state["allowed_user_ids"] = allowed_ids
    sessions = state.get("ui_sessions")
    state["ui_sessions"] = sessions if isinstance(sessions, dict) else {}
    state["menu_message_id"] = int(state.get("menu_message_id", 0) or 0)
    state["menu_page"] = str(state.get("menu_page", "home") or "home").strip() or "home"
    state["ui_notice"] = str(state.get("ui_notice", "") or "").strip()
    state["ui_notice_at"] = str(state.get("ui_notice_at", "") or "").strip()
    state["posting_enabled"] = bool(state.get("posting_enabled", True))
    state["channel"] = str(state.get("channel", env.default_channel) or env.default_channel).strip() or env.default_channel
    state["interval_seconds"] = clamp_interval(int(state.get("interval_seconds", env.default_interval_seconds) or env.default_interval_seconds))
    state["decimal_places"] = clamp_decimal_places(int(state.get("decimal_places", DEFAULT_DECIMAL_PLACES) or DEFAULT_DECIMAL_PLACES))
    state["show_change_1h"] = bool(state.get("show_change_1h", True))
    state["show_change_24h"] = bool(state.get("show_change_24h", True))
    state["show_change_7d"] = bool(state.get("show_change_7d", True))
    state["buy_alerts_enabled"] = bool(state.get("buy_alerts_enabled", True))
    state["buy_alert_channel"] = str(state.get("buy_alert_channel", "") or "").strip()
    try:
        state["buy_alert_threshold_usd"] = max(0.0, float(state.get("buy_alert_threshold_usd", DEFAULT_ALERT_THRESHOLD_USD) or DEFAULT_ALERT_THRESHOLD_USD))
    except Exception:
        state["buy_alert_threshold_usd"] = DEFAULT_ALERT_THRESHOLD_USD
    state["buy_alert_interval_seconds"] = clamp_alert_interval(int(state.get("buy_alert_interval_seconds", DEFAULT_ALERT_INTERVAL_SECONDS) or DEFAULT_ALERT_INTERVAL_SECONDS))
    state["buy_alert_pool_address"] = str(state.get("buy_alert_pool_address", DEFAULT_ALERT_POOL_ADDRESS) or DEFAULT_ALERT_POOL_ADDRESS).strip() or DEFAULT_ALERT_POOL_ADDRESS
    state["buy_alert_pool_label"] = str(state.get("buy_alert_pool_label", DEFAULT_ALERT_POOL_LABEL) or DEFAULT_ALERT_POOL_LABEL).strip() or DEFAULT_ALERT_POOL_LABEL
    state["show_buy_alert_wallet"] = bool(state.get("show_buy_alert_wallet", True))
    state["show_buy_alert_utya"] = bool(state.get("show_buy_alert_utya", True))
    state["show_buy_alert_usd"] = bool(state.get("show_buy_alert_usd", True))
    state["show_buy_alert_pool"] = bool(state.get("show_buy_alert_pool", True))
    state["show_buy_alert_link"] = bool(state.get("show_buy_alert_link", True))
    state["buy_alert_seen_event_ids"] = dedupe_text_values(state.get("buy_alert_seen_event_ids") or [], limit=300)
    state["buy_alert_seen_keys"] = dedupe_text_values(state.get("buy_alert_seen_keys") or [], limit=300)
    state["buy_alert_bootstrapped"] = bool(state.get("buy_alert_bootstrapped", False))
    if state["buy_alert_seen_event_ids"] and not state["buy_alert_seen_keys"]:
        # Migration path: force one clean sync before sending alerts so older deployments
        # don't re-emit the same logical buy under a new TonAPI event id.
        state["buy_alert_bootstrapped"] = False
    state["last_buy_alert_at"] = str(state.get("last_buy_alert_at", "") or "").strip()
    state["last_buy_alert_text"] = str(state.get("last_buy_alert_text", "") or "").strip()
    state["last_buy_alert_wallet"] = str(state.get("last_buy_alert_wallet", "") or "").strip()
    state["sell_alerts_enabled"] = bool(state.get("sell_alerts_enabled", True))
    state["sell_alert_channel"] = str(state.get("sell_alert_channel", "") or "").strip()
    try:
        state["sell_alert_threshold_usd"] = max(0.0, float(state.get("sell_alert_threshold_usd", DEFAULT_ALERT_THRESHOLD_USD) or DEFAULT_ALERT_THRESHOLD_USD))
    except Exception:
        state["sell_alert_threshold_usd"] = DEFAULT_ALERT_THRESHOLD_USD
    state["sell_alert_interval_seconds"] = clamp_alert_interval(int(state.get("sell_alert_interval_seconds", DEFAULT_ALERT_INTERVAL_SECONDS) or DEFAULT_ALERT_INTERVAL_SECONDS))
    state["sell_alert_pool_address"] = str(state.get("sell_alert_pool_address", DEFAULT_ALERT_POOL_ADDRESS) or DEFAULT_ALERT_POOL_ADDRESS).strip() or DEFAULT_ALERT_POOL_ADDRESS
    state["sell_alert_pool_label"] = str(state.get("sell_alert_pool_label", DEFAULT_ALERT_POOL_LABEL) or DEFAULT_ALERT_POOL_LABEL).strip() or DEFAULT_ALERT_POOL_LABEL
    state["show_sell_alert_wallet"] = bool(state.get("show_sell_alert_wallet", True))
    state["show_sell_alert_utya"] = bool(state.get("show_sell_alert_utya", True))
    state["show_sell_alert_usd"] = bool(state.get("show_sell_alert_usd", True))
    state["show_sell_alert_pool"] = bool(state.get("show_sell_alert_pool", True))
    state["show_sell_alert_link"] = bool(state.get("show_sell_alert_link", True))
    state["sell_alert_seen_event_ids"] = dedupe_text_values(state.get("sell_alert_seen_event_ids") or [], limit=300)
    state["sell_alert_seen_keys"] = dedupe_text_values(state.get("sell_alert_seen_keys") or [], limit=300)
    state["sell_alert_bootstrapped"] = bool(state.get("sell_alert_bootstrapped", False))
    if state["sell_alert_seen_event_ids"] and not state["sell_alert_seen_keys"]:
        state["sell_alert_bootstrapped"] = False
    state["last_sell_alert_at"] = str(state.get("last_sell_alert_at", "") or "").strip()
    state["last_sell_alert_text"] = str(state.get("last_sell_alert_text", "") or "").strip()
    state["last_sell_alert_wallet"] = str(state.get("last_sell_alert_wallet", "") or "").strip()
    try:
        state["ston_alert_cursor_block"] = max(0, int(state.get("ston_alert_cursor_block", 0) or 0))
    except (TypeError, ValueError):
        state["ston_alert_cursor_block"] = 0
    state["ston_alert_outbox"] = [
        item for item in list(state.get("ston_alert_outbox") or []) if isinstance(item, dict)
    ]
    state["ston_alert_last_sync_at"] = str(state.get("ston_alert_last_sync_at", "") or "").strip()
    state["ston_alert_last_error"] = str(state.get("ston_alert_last_error", "") or "").strip()
    try:
        state["dedust_alert_cursor_lt"] = max(0, int(state.get("dedust_alert_cursor_lt", 0) or 0))
    except (TypeError, ValueError):
        state["dedust_alert_cursor_lt"] = 0
    state["dedust_alert_last_sync_at"] = str(state.get("dedust_alert_last_sync_at", "") or "").strip()
    state["dedust_alert_last_error"] = str(state.get("dedust_alert_last_error", "") or "").strip()
    activity_log = state.get("activity_log")
    state["activity_log"] = activity_log if isinstance(activity_log, list) else []
    state["last_change_1h"] = str(state.get("last_change_1h", "") or "").strip()
    state["last_change_24h"] = str(state.get("last_change_24h", "") or "").strip()
    state["last_change_7d"] = str(state.get("last_change_7d", "") or "").strip()
    pending = state.get("pending_inputs")
    state["pending_inputs"] = pending if isinstance(pending, dict) else {}
    return state


def save_state(state: dict[str, Any]) -> None:
    temporary_path = STATE_PATH.with_suffix(".json.tmp")
    payload = json.dumps(state, indent=2, ensure_ascii=False) + "\n"
    with temporary_path.open("w", encoding="utf-8") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    temporary_path.replace(STATE_PATH)


def runtime_from_state(env: EnvConfig, state: dict[str, Any]) -> RuntimeConfig:
    return RuntimeConfig(
        bot_token=env.bot_token,
        channel=str(state.get("channel") or env.default_channel).strip() or env.default_channel,
        interval_seconds=clamp_interval(int(state.get("interval_seconds", env.default_interval_seconds) or env.default_interval_seconds)),
        decimal_places=clamp_decimal_places(int(state.get("decimal_places", DEFAULT_DECIMAL_PLACES) or DEFAULT_DECIMAL_PLACES)),
        posting_enabled=bool(state.get("posting_enabled", True)),
        user_agent=env.user_agent,
        timeout_seconds=env.timeout_seconds,
        retry_seconds=env.retry_seconds,
        command_timeout_seconds=env.command_timeout_seconds,
    )


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_text() -> str:
    return utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")


def format_local_text(raw_iso: str) -> str:
    value = str(raw_iso or "").strip()
    if not value:
        return "-"
    try:
        dt = datetime.fromisoformat(value)
    except Exception:
        return value
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")


def log(message: str) -> None:
    line = f"[{utc_now_text()}] {message}"
    print(line, flush=True)
    try:
        with LOG_PATH.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
    except Exception:
        pass


def tail_log_lines(limit: int = DEFAULT_LOG_LINES) -> list[str]:
    if not LOG_PATH.exists():
        return []
    try:
        lines = LOG_PATH.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return []
    return lines[-max(1, limit):]


def build_request(url: str, *, user_agent: str, method: str = "GET", data: bytes | None = None) -> urllib.request.Request:
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("User-Agent", user_agent)
    return request


def api_request(runtime: RuntimeConfig, method: str, payload: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    url = f"https://api.telegram.org/bot{runtime.bot_token}/{method}"
    data = None
    request: urllib.request.Request
    if payload is None:
        request = build_request(url, user_agent=runtime.user_agent)
    else:
        encoded = {}
        for key, value in payload.items():
            if value is None:
                continue
            if isinstance(value, (dict, list)):
                encoded[key] = json.dumps(value, ensure_ascii=False)
            else:
                encoded[key] = str(value)
        data = urllib.parse.urlencode(encoded).encode("utf-8")
        request = build_request(url, user_agent=runtime.user_agent, method="POST", data=data)
        request.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with TELEGRAM_IPV4_OPENER.open(request, timeout=runtime.timeout_seconds + 10) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            payload_json = json.loads(raw)
        except Exception as parse_exc:
            raise RuntimeError(f"Telegram {method} failed: HTTP {exc.code}: {raw or exc.reason}") from parse_exc
        description = str(payload_json.get("description") or payload_json)
        raise RuntimeError(f"Telegram {method} failed: {description}") from exc
    payload_json = json.loads(raw)
    if not payload_json.get("ok"):
        raise RuntimeError(f"Telegram {method} failed: {payload_json}")
    return payload_json


def save_market_snapshot_cache(snapshot: MarketSnapshot) -> None:
    try:
        PRICE_CACHE_PATH.write_text(json.dumps({
            "price_usd": snapshot.price_usd,
            "change_1h_percent": snapshot.change_1h_percent,
            "change_24h_percent": snapshot.change_24h_percent,
            "change_7d_percent": snapshot.change_7d_percent,
            "gram_price_usd": snapshot.gram_price_usd,
            "saved_at": utc_now().isoformat(),
        }, indent=2), encoding="utf-8")
    except Exception as exc:
        log(f"Price cache save failed: {type(exc).__name__}: {exc}")


def load_market_snapshot_cache() -> MarketSnapshot:
    payload = json.loads(PRICE_CACHE_PATH.read_text(encoding="utf-8-sig"))
    return MarketSnapshot(
        price_usd=float(payload["price_usd"]),
        change_1h_percent=float(payload["change_1h_percent"]) if payload.get("change_1h_percent") is not None else None,
        change_24h_percent=float(payload["change_24h_percent"]) if payload.get("change_24h_percent") is not None else None,
        change_7d_percent=float(payload["change_7d_percent"]) if payload.get("change_7d_percent") is not None else None,
        gram_price_usd=load_cached_gram_price(payload),
    )


def load_cached_gram_price(payload: dict[str, Any]) -> Optional[float]:
    try:
        saved_at = datetime.fromisoformat(str(payload.get("saved_at") or ""))
        if saved_at.tzinfo is None:
            saved_at = saved_at.replace(tzinfo=timezone.utc)
        age_seconds = max(0.0, (utc_now() - saved_at.astimezone(timezone.utc)).total_seconds())
        price = float(payload.get("gram_price_usd"))
        if price > 0 and age_seconds <= SHARED_GRAM_PRICE_MAX_AGE_SECONDS:
            return price
    except Exception:
        pass
    return load_shared_gram_price_cache()


def load_shared_gram_price_cache() -> Optional[float]:
    try:
        age_seconds = max(0.0, time.time() - SHARED_MEME_PRICE_CACHE_PATH.stat().st_mtime)
        if age_seconds > SHARED_GRAM_PRICE_MAX_AGE_SECONDS:
            return None
        payload = json.loads(SHARED_MEME_PRICE_CACHE_PATH.read_text(encoding="utf-8-sig"))
        for item in payload if isinstance(payload, list) else []:
            if not isinstance(item, dict) or str(item.get("ticker") or "").strip().upper() != "GRAM":
                continue
            price = float(item.get("price"))
            return price if price > 0 else None
    except Exception:
        return None
    return None


def load_alert_market_snapshot() -> MarketSnapshot:
    cached: Optional[MarketSnapshot]
    try:
        cached = load_market_snapshot_cache()
    except Exception:
        cached = None

    shared_prices: dict[str, float] = {}
    try:
        age_seconds = max(0.0, time.time() - SHARED_MEME_PRICE_CACHE_PATH.stat().st_mtime)
        if age_seconds <= SHARED_GRAM_PRICE_MAX_AGE_SECONDS:
            payload = json.loads(SHARED_MEME_PRICE_CACHE_PATH.read_text(encoding="utf-8-sig"))
            for item in payload if isinstance(payload, list) else []:
                if not isinstance(item, dict):
                    continue
                ticker = str(item.get("ticker") or "").strip().upper()
                price = float(item.get("price") or 0)
                if ticker and price > 0:
                    shared_prices[ticker] = price
    except Exception:
        shared_prices = {}

    utya_price = shared_prices.get(TOKEN_TICKER) or (cached.price_usd if cached else None)
    gram_price = shared_prices.get("GRAM") or (cached.gram_price_usd if cached else None)
    if utya_price is None or utya_price <= 0:
        raise RuntimeError(f"A current {TOKEN_TICKER}/USD price is unavailable; alert processing was deferred")
    if gram_price is None or gram_price <= 0:
        raise RuntimeError("A current GRAM/USD price is unavailable; alert processing was deferred")
    return MarketSnapshot(
        price_usd=utya_price,
        change_1h_percent=cached.change_1h_percent if cached else None,
        change_24h_percent=cached.change_24h_percent if cached else None,
        change_7d_percent=cached.change_7d_percent if cached else None,
        gram_price_usd=gram_price,
    )


def fetch_dexscreener_market_snapshot_live(runtime: RuntimeConfig) -> MarketSnapshot:
    url = DEXSCREENER_PAIR_URL.format(
        pool_address=urllib.parse.quote(DEFAULT_ALERT_POOL_ADDRESS, safe=""),
    )
    request = build_request(url, user_agent=runtime.user_agent)
    with urllib.request.urlopen(
        request,
        timeout=min(runtime.timeout_seconds, EXTERNAL_DATA_TIMEOUT_SECONDS),
    ) as response:
        payload = json.loads(response.read().decode("utf-8"))
    pairs = payload.get("pairs") if isinstance(payload, dict) else None
    pair = next(
        (
            item
            for item in pairs or []
            if isinstance(item, dict)
            and str(item.get("pairAddress") or "").strip() == DEFAULT_ALERT_POOL_ADDRESS
        ),
        None,
    )
    if not isinstance(pair, dict):
        raise RuntimeError("DexScreener did not return the configured primary pool")
    base = pair.get("baseToken") if isinstance(pair.get("baseToken"), dict) else {}
    if str(base.get("address") or "").strip() != YODA_FRIENDLY_MASTER_ADDRESS:
        raise RuntimeError(f"DexScreener primary pool does not use {TOKEN_TICKER} as its base asset")
    try:
        price = float(pair["priceUsd"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("DexScreener returned an invalid USD price") from exc
    if price <= 0:
        raise RuntimeError("DexScreener returned a non-positive USD price")
    changes = pair.get("priceChange") if isinstance(pair.get("priceChange"), dict) else {}
    try:
        cached = load_market_snapshot_cache()
    except Exception:
        cached = None
    gram_price = load_shared_gram_price_cache()
    try:
        native_price = float(pair.get("priceNative") or 0)
        if native_price > 0:
            gram_price = price / native_price
    except (TypeError, ValueError):
        pass
    return MarketSnapshot(
        price_usd=price,
        change_1h_percent=float(changes["h1"]) if changes.get("h1") is not None else None,
        change_24h_percent=float(changes["h24"]) if changes.get("h24") is not None else None,
        change_7d_percent=cached.change_7d_percent if cached else None,
        gram_price_usd=gram_price,
    )


def fetch_utya_market_snapshot_live(runtime: RuntimeConfig) -> MarketSnapshot:
    if not COINGECKO_ID:
        raise RuntimeError(f"CoinGecko does not list {TOKEN_TICKER}")
    request = build_request(COINGECKO_MARKET_URL, user_agent=runtime.user_agent)
    with urllib.request.urlopen(request, timeout=min(runtime.timeout_seconds, EXTERNAL_DATA_TIMEOUT_SECONDS)) as response:
        payload = json.loads(response.read().decode("utf-8"))
    try:
        coins = {
            str(item.get("id") or "").strip(): item
            for item in payload
            if isinstance(item, dict)
        }
        coin = coins[COINGECKO_ID]
        gram_coin = coins.get("the-open-network") or {}
        price = float(coin["current_price"])
        change_1h = coin.get("price_change_percentage_1h_in_currency")
        change_24h = coin.get("price_change_percentage_24h_in_currency")
        change_7d = coin.get("price_change_percentage_7d_in_currency")
    except (IndexError, KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(f"Unexpected price payload: {payload!r}") from exc
    return MarketSnapshot(
        price_usd=price,
        change_1h_percent=float(change_1h) if change_1h is not None else None,
        change_24h_percent=float(change_24h) if change_24h is not None else None,
        change_7d_percent=float(change_7d) if change_7d is not None else None,
        gram_price_usd=float(gram_coin["current_price"]) if gram_coin.get("current_price") is not None else load_shared_gram_price_cache(),
    )


def fetch_utya_market_snapshot(runtime: RuntimeConfig) -> MarketSnapshot:
    errors: list[str] = []
    sources = [("DexScreener primary pool", fetch_dexscreener_market_snapshot_live)]
    if COINGECKO_ID:
        sources.append(("CoinGecko", fetch_utya_market_snapshot_live))
    for source_name, fetcher in sources:
        try:
            snapshot = fetcher(runtime)
            if snapshot.gram_price_usd is None:
                try:
                    cached_gram_price = load_market_snapshot_cache().gram_price_usd
                except Exception:
                    cached_gram_price = None
                if cached_gram_price is not None:
                    snapshot = replace(snapshot, gram_price_usd=cached_gram_price)
            save_market_snapshot_cache(snapshot)
            return snapshot
        except Exception as exc:
            errors.append(f"{source_name}: {type(exc).__name__}: {exc}")
    log(f"Live {TOKEN_TICKER} price sources failed: {'; '.join(errors)}")
    try:
        snapshot = load_market_snapshot_cache()
        log(f"Using cached {TOKEN_TICKER} price snapshot after live source failure")
        return snapshot
    except Exception as cache_exc:
        raise RuntimeError(
            f"{TOKEN_TICKER} price fetch failed and cache unavailable: "
            f"{type(cache_exc).__name__}: {cache_exc}"
        ) from cache_exc


def fetch_pool_events(runtime: RuntimeConfig, state: dict[str, Any], *, limit: int = DEFAULT_ALERT_FETCH_LIMIT) -> list[dict[str, Any]]:
    pool_address = str(state.get("buy_alert_pool_address") or DEFAULT_ALERT_POOL_ADDRESS).strip() or DEFAULT_ALERT_POOL_ADDRESS
    url = TONAPI_POOL_EVENTS_URL.format(
        pool_address=urllib.parse.quote(pool_address, safe=""),
        limit=max(1, min(100, int(limit))),
    )
    request = build_request(url, user_agent=runtime.user_agent)
    with urllib.request.urlopen(request, timeout=min(runtime.timeout_seconds, EXTERNAL_DATA_TIMEOUT_SECONDS)) as response:
        payload = json.loads(response.read().decode("utf-8"))
    events = payload.get("events")
    return events if isinstance(events, list) else []


def fetch_ston_json(runtime: RuntimeConfig, url: str) -> dict[str, Any]:
    request = build_request(url, user_agent=runtime.user_agent)
    with urllib.request.urlopen(
        request,
        timeout=min(runtime.timeout_seconds, EXTERNAL_DATA_TIMEOUT_SECONDS),
    ) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("STON.fi returned an invalid response")
    return payload


def fetch_ston_latest_block(runtime: RuntimeConfig) -> int:
    payload = fetch_ston_json(runtime, STON_LATEST_BLOCK_URL)
    block = payload.get("block") if isinstance(payload.get("block"), dict) else {}
    block_number = int(block.get("blockNumber") or 0)
    if block_number <= 0:
        raise RuntimeError("STON.fi did not return a valid latest block")
    return block_number


def fetch_ston_events(runtime: RuntimeConfig, from_block: int, to_block: int) -> list[dict[str, Any]]:
    if from_block <= 0 or to_block < from_block:
        return []
    payload = fetch_ston_json(
        runtime,
        STON_EVENTS_URL.format(from_block=int(from_block), to_block=int(to_block)),
    )
    events = payload.get("events")
    return [item for item in list(events or []) if isinstance(item, dict)]


def fetch_ston_pair_layout(runtime: RuntimeConfig, pool_address: str) -> tuple[int, bool]:
    pool_key = str(pool_address or "").strip()
    cached = _STON_PAIR_LAYOUT_CACHE.get(pool_key)
    if cached is not None:
        return cached
    payload = fetch_ston_json(
        runtime,
        STON_PAIR_URL.format(pool_address=urllib.parse.quote(pool_key, safe="")),
    )
    pool = payload.get("pool") if isinstance(payload.get("pool"), dict) else {}
    asset0 = str(pool.get("asset0Id") or "").strip()
    asset1 = str(pool.get("asset1Id") or "").strip()
    if asset0 == YODA_FRIENDLY_MASTER_ADDRESS:
        layout = (0, asset1 == STON_NATIVE_ASSET_ADDRESS)
    elif asset1 == YODA_FRIENDLY_MASTER_ADDRESS:
        layout = (1, asset0 == STON_NATIVE_ASSET_ADDRESS)
    else:
        raise RuntimeError("The configured alert pool does not contain the YODA master contract")
    _STON_PAIR_LAYOUT_CACHE[pool_key] = layout
    return layout


def parse_positive_amount(value: Any) -> Optional[float]:
    try:
        amount = float(str(value or "").strip())
    except (TypeError, ValueError):
        return None
    return amount if amount > 0 else None


def parse_ston_alert_events(
    state: dict[str, Any],
    events: list[dict[str, Any]],
    snapshot: MarketSnapshot,
    pool_layouts: dict[str, tuple[int, bool]],
) -> list[tuple[str, BuyAlertEvent | SellAlertEvent]]:
    gram_price = snapshot.gram_price_usd or load_shared_gram_price_cache()
    if gram_price is None or gram_price <= 0:
        raise RuntimeError("Live GRAM/USD price is unavailable; alert processing was deferred")

    configured = {
        "BUY": (
            bool(state.get("buy_alerts_enabled", True)),
            str(state.get("buy_alert_pool_address") or DEFAULT_ALERT_POOL_ADDRESS).strip(),
            str(state.get("buy_alert_pool_label") or DEFAULT_ALERT_POOL_LABEL).strip(),
            float(state.get("buy_alert_threshold_usd", DEFAULT_ALERT_THRESHOLD_USD) or DEFAULT_ALERT_THRESHOLD_USD),
        ),
        "SELL": (
            bool(state.get("sell_alerts_enabled", True)),
            str(state.get("sell_alert_pool_address") or DEFAULT_ALERT_POOL_ADDRESS).strip(),
            str(state.get("sell_alert_pool_label") or DEFAULT_ALERT_POOL_LABEL).strip(),
            float(state.get("sell_alert_threshold_usd", DEFAULT_ALERT_THRESHOLD_USD) or DEFAULT_ALERT_THRESHOLD_USD),
        ),
    }
    parsed: list[tuple[str, BuyAlertEvent | SellAlertEvent]] = []
    ordered_events = sorted(
        events,
        key=lambda item: (
            int((item.get("block") or {}).get("blockNumber") or 0) if isinstance(item.get("block"), dict) else 0,
            int(item.get("txnIndex") or 0),
            int(item.get("eventIndex") or 0),
        ),
    )
    for item in ordered_events:
        if str(item.get("eventType") or "").strip().lower() != "swap":
            continue
        pair_id = str(item.get("pairId") or "").strip()
        transaction_hash = str(item.get("txnId") or "").strip()
        event_index = int(item.get("eventIndex") or 0)
        if not pair_id or not transaction_hash:
            continue
        block = item.get("block") if isinstance(item.get("block"), dict) else {}
        timestamp = int(block.get("blockTimestamp") or 0)
        happened_at = (
            format_local_text(datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat())
            if timestamp
            else "-"
        )
        wallet_address = str(item.get("maker") or "").strip() or "-"

        for side, (enabled, pool_address, pool_label, threshold_usd) in configured.items():
            if not enabled or pair_id != pool_address:
                continue
            utya_index, other_is_gram = pool_layouts[pool_address]
            other_index = 1 - utya_index
            if side == "BUY":
                utya_amount = parse_positive_amount(item.get(f"amount{utya_index}Out"))
                counter_amount = parse_positive_amount(item.get(f"amount{other_index}In"))
            else:
                utya_amount = parse_positive_amount(item.get(f"amount{utya_index}In"))
                counter_amount = parse_positive_amount(item.get(f"amount{other_index}Out"))
            if utya_amount is None or counter_amount is None:
                continue
            if other_is_gram:
                gram_amount = counter_amount
                usd_amount = gram_amount * gram_price
            else:
                usd_amount = utya_amount * snapshot.price_usd
                gram_amount = usd_amount / gram_price
            if usd_amount < threshold_usd:
                continue
            common = {
                "event_id": f"ston:{transaction_hash}:{event_index}",
                "wallet_address": wallet_address,
                "utya_amount": utya_amount,
                "usd_amount": usd_amount,
                "pool_label": pool_label or DEFAULT_ALERT_POOL_LABEL,
                "pool_address": pool_address,
                "tx_hash": transaction_hash,
                "timestamp": timestamp,
                "happened_at": happened_at,
                "gram_amount": gram_amount,
                "wallet_dns": "",
            }
            event = BuyAlertEvent(**common) if side == "BUY" else SellAlertEvent(**common)
            parsed.append((side, event))
    return parsed


def serialize_alert_outbox_item(side: str, event: BuyAlertEvent | SellAlertEvent) -> dict[str, Any]:
    return {"side": side.upper(), "event": asdict(event), "queued_at": utc_now().isoformat()}


def deserialize_alert_outbox_item(item: dict[str, Any]) -> tuple[str, BuyAlertEvent | SellAlertEvent]:
    side = str(item.get("side") or "").strip().upper()
    payload = item.get("event") if isinstance(item.get("event"), dict) else {}
    if side == "BUY":
        return side, BuyAlertEvent(**payload)
    if side == "SELL":
        return side, SellAlertEvent(**payload)
    raise ValueError("Alert outbox item has an invalid side")


def enqueue_alert_events(
    state: dict[str, Any],
    alerts: list[tuple[str, BuyAlertEvent | SellAlertEvent]],
) -> int:
    outbox = [item for item in list(state.get("ston_alert_outbox") or []) if isinstance(item, dict)]
    pending_ids = {
        (str(item.get("side") or "").upper(), str((item.get("event") or {}).get("event_id") or ""))
        for item in outbox
        if isinstance(item.get("event"), dict)
    }
    added = 0
    for side, event in alerts:
        seen_key = "buy_alert_seen_event_ids" if side == "BUY" else "sell_alert_seen_event_ids"
        seen_ids = set(dedupe_text_values(state.get(seen_key) or [], limit=300))
        identity = (side, event.event_id)
        if event.event_id in seen_ids or identity in pending_ids:
            continue
        outbox.append(serialize_alert_outbox_item(side, event))
        pending_ids.add(identity)
        added += 1
    state["ston_alert_outbox"] = outbox
    return added


def deliver_alert_outbox(runtime: RuntimeConfig, state: dict[str, Any]) -> int:
    delivered = 0
    while state.get("ston_alert_outbox"):
        item = state["ston_alert_outbox"][0]
        side, event = deserialize_alert_outbox_item(item)
        event = enrich_alert_wallet_dns(runtime, event)
        if side == "BUY":
            target_chat = get_buy_alert_channel(state, runtime)
            message = build_buy_alert_text(state, event)
            state["last_buy_alert_at"] = utc_now().isoformat()
            state["last_buy_alert_text"] = format_usd_value(event.usd_amount)
            state["last_buy_alert_wallet"] = event.wallet_address
            seen_id_key = "buy_alert_seen_event_ids"
            seen_key_key = "buy_alert_seen_keys"
            dedupe_key = build_buy_alert_dedupe_key(event)
        else:
            target_chat = get_sell_alert_channel(state, runtime)
            message = build_sell_alert_text(state, event)
            state["last_sell_alert_at"] = utc_now().isoformat()
            state["last_sell_alert_text"] = format_usd_value(event.usd_amount)
            state["last_sell_alert_wallet"] = event.wallet_address
            seen_id_key = "sell_alert_seen_event_ids"
            seen_key_key = "sell_alert_seen_keys"
            dedupe_key = build_sell_alert_dedupe_key(event)
        send_message(runtime, target_chat, message, parse_mode="HTML")
        state[seen_id_key] = dedupe_text_values(
            list(state.get(seen_id_key) or []) + [event.event_id],
            limit=300,
        )
        state[seen_key_key] = dedupe_text_values(
            list(state.get(seen_key_key) or []) + [dedupe_key],
            limit=300,
        )
        state["ston_alert_outbox"] = list(state.get("ston_alert_outbox") or [])[1:]
        save_state(state)
        delivered += 1
        log(
            f"Posted {side.lower()} alert {format_usd_value(event.usd_amount)} "
            f"for {shorten_wallet(event.wallet_address)} from {DEFAULT_ALERT_DEX} durable cursor"
        )
    return delivered


def sync_ston_alerts(runtime: RuntimeConfig, state: dict[str, Any]) -> tuple[int, int]:
    delivered = deliver_alert_outbox(runtime, state)
    latest_block = fetch_ston_latest_block(runtime)
    cursor = max(0, int(state.get("ston_alert_cursor_block", 0) or 0))
    if cursor <= 0:
        state["ston_alert_cursor_block"] = latest_block
        state["ston_alert_last_sync_at"] = utc_now().isoformat()
        state["ston_alert_last_error"] = ""
        save_state(state)
        log(f"Initialized reliable STON.fi alert cursor at block {latest_block}")
        return 0, delivered
    if latest_block <= cursor:
        state["ston_alert_last_sync_at"] = utc_now().isoformat()
        state["ston_alert_last_error"] = ""
        save_state(state)
        return 0, delivered

    pool_addresses: set[str] = set()
    if bool(state.get("buy_alerts_enabled", True)):
        pool_addresses.add(
            str(state.get("buy_alert_pool_address") or DEFAULT_ALERT_POOL_ADDRESS).strip()
        )
    if bool(state.get("sell_alerts_enabled", True)):
        pool_addresses.add(
            str(state.get("sell_alert_pool_address") or DEFAULT_ALERT_POOL_ADDRESS).strip()
        )
    pool_layouts = {address: fetch_ston_pair_layout(runtime, address) for address in pool_addresses}
    snapshot = load_alert_market_snapshot()
    queued = 0
    chunks = 0
    while cursor < latest_block and chunks < STON_ALERT_MAX_CHUNKS_PER_CHECK:
        to_block = min(latest_block, cursor + STON_ALERT_BLOCK_CHUNK)
        raw_events = fetch_ston_events(runtime, cursor + 1, to_block)
        alerts = parse_ston_alert_events(state, raw_events, snapshot, pool_layouts)
        queued += enqueue_alert_events(state, alerts)
        cursor = to_block
        state["ston_alert_cursor_block"] = cursor
        state["ston_alert_last_sync_at"] = utc_now().isoformat()
        state["ston_alert_last_error"] = ""
        save_state(state)
        delivered += deliver_alert_outbox(runtime, state)
        chunks += 1
    if cursor < latest_block:
        log(f"STON.fi alert cursor catch-up pending: block {cursor} of {latest_block}")
    return queued, delivered


def fetch_toncenter_latest_transaction_lt(runtime: RuntimeConfig, pool_address: str) -> int:
    url = TONCENTER_LATEST_TRANSACTION_URL.format(
        pool_address=urllib.parse.quote(pool_address, safe=""),
    )
    request = build_request(url, user_agent=runtime.user_agent)
    with urllib.request.urlopen(
        request,
        timeout=min(runtime.timeout_seconds, EXTERNAL_DATA_TIMEOUT_SECONDS),
    ) as response:
        payload = json.loads(response.read().decode("utf-8"))
    transactions = payload.get("transactions") if isinstance(payload, dict) else None
    if not isinstance(transactions, list) or not transactions:
        raise RuntimeError("TON Center did not return the pool's latest transaction")
    try:
        return max(int(item.get("lt") or 0) for item in transactions if isinstance(item, dict))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("TON Center returned an invalid pool transaction cursor") from exc


def fetch_dedust_trades(
    runtime: RuntimeConfig,
    pool_address: str,
    after_lt: int,
) -> list[dict[str, Any]]:
    url = DEDUST_TRADES_URL.format(
        pool_address=urllib.parse.quote(pool_address, safe=""),
        page_size=DEDUST_ALERT_PAGE_SIZE,
        after_lt=max(0, int(after_lt)),
    )
    request = build_request(url, user_agent=runtime.user_agent)
    with urllib.request.urlopen(
        request,
        timeout=min(runtime.timeout_seconds, EXTERNAL_DATA_TIMEOUT_SECONDS),
    ) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, list):
        raise RuntimeError("DeDust returned an invalid trades response")
    return [item for item in payload if isinstance(item, dict)]


def dedust_asset_is_native(asset: Any) -> bool:
    return isinstance(asset, dict) and str(asset.get("type") or "").strip().lower() == "native"


def dedust_asset_is_token(asset: Any) -> bool:
    return (
        isinstance(asset, dict)
        and str(asset.get("type") or "").strip().lower() == "jetton"
        and str(asset.get("address") or "").strip() == YODA_FRIENDLY_MASTER_ADDRESS
    )


def parse_dedust_raw_amount(value: Any, decimals: int) -> Optional[float]:
    try:
        amount = int(str(value or "").strip()) / (10 ** max(0, int(decimals)))
    except (TypeError, ValueError):
        return None
    return amount if amount > 0 else None


def parse_dedust_alert_events(
    state: dict[str, Any],
    trades: list[dict[str, Any]],
    snapshot: MarketSnapshot,
    pool_address: str,
) -> list[tuple[str, BuyAlertEvent | SellAlertEvent]]:
    gram_price = snapshot.gram_price_usd or load_shared_gram_price_cache()
    if gram_price is None or gram_price <= 0:
        raise RuntimeError("Live GRAM/USD price is unavailable; alert processing was deferred")
    configured = {
        "BUY": (
            bool(state.get("buy_alerts_enabled", True)),
            str(state.get("buy_alert_pool_address") or DEFAULT_ALERT_POOL_ADDRESS).strip(),
            str(state.get("buy_alert_pool_label") or DEFAULT_ALERT_POOL_LABEL).strip(),
            float(state.get("buy_alert_threshold_usd", DEFAULT_ALERT_THRESHOLD_USD) or DEFAULT_ALERT_THRESHOLD_USD),
        ),
        "SELL": (
            bool(state.get("sell_alerts_enabled", True)),
            str(state.get("sell_alert_pool_address") or DEFAULT_ALERT_POOL_ADDRESS).strip(),
            str(state.get("sell_alert_pool_label") or DEFAULT_ALERT_POOL_LABEL).strip(),
            float(state.get("sell_alert_threshold_usd", DEFAULT_ALERT_THRESHOLD_USD) or DEFAULT_ALERT_THRESHOLD_USD),
        ),
    }
    parsed: list[tuple[str, BuyAlertEvent | SellAlertEvent]] = []
    for item in sorted(trades, key=lambda trade: int(trade.get("lt") or 0)):
        asset_in = item.get("assetIn")
        asset_out = item.get("assetOut")
        if dedust_asset_is_native(asset_in) and dedust_asset_is_token(asset_out):
            side = "BUY"
            gram_amount = parse_dedust_raw_amount(item.get("amountIn"), 9)
            utya_amount = parse_dedust_raw_amount(item.get("amountOut"), TOKEN_DECIMALS)
        elif dedust_asset_is_token(asset_in) and dedust_asset_is_native(asset_out):
            side = "SELL"
            utya_amount = parse_dedust_raw_amount(item.get("amountIn"), TOKEN_DECIMALS)
            gram_amount = parse_dedust_raw_amount(item.get("amountOut"), 9)
        else:
            continue
        enabled, configured_pool, pool_label, threshold_usd = configured[side]
        if not enabled or configured_pool != pool_address or gram_amount is None or utya_amount is None:
            continue
        usd_amount = gram_amount * gram_price
        if usd_amount < threshold_usd:
            continue
        try:
            trade_lt = int(item.get("lt") or 0)
        except (TypeError, ValueError):
            continue
        created_at = str(item.get("createdAt") or "").strip()
        try:
            parsed_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            timestamp = int(parsed_at.timestamp())
            happened_at = format_local_text(parsed_at.isoformat())
        except (TypeError, ValueError):
            timestamp = 0
            happened_at = "-"
        common = {
            "event_id": f"dedust:{pool_address}:{trade_lt}",
            "wallet_address": str(item.get("sender") or "").strip() or "-",
            "utya_amount": utya_amount,
            "usd_amount": usd_amount,
            "pool_label": pool_label or DEFAULT_ALERT_POOL_LABEL,
            "pool_address": pool_address,
            "tx_hash": "",
            "timestamp": timestamp,
            "happened_at": happened_at,
            "gram_amount": gram_amount,
            "wallet_dns": "",
        }
        event = BuyAlertEvent(**common) if side == "BUY" else SellAlertEvent(**common)
        parsed.append((side, event))
    return parsed


def sync_dedust_alerts(runtime: RuntimeConfig, state: dict[str, Any]) -> tuple[int, int]:
    delivered = deliver_alert_outbox(runtime, state)
    pool_addresses = {
        str(state.get(key) or DEFAULT_ALERT_POOL_ADDRESS).strip()
        for enabled_key, key in (
            ("buy_alerts_enabled", "buy_alert_pool_address"),
            ("sell_alerts_enabled", "sell_alert_pool_address"),
        )
        if bool(state.get(enabled_key, True))
    }
    if not pool_addresses:
        return 0, delivered
    if len(pool_addresses) != 1:
        raise RuntimeError("DeDust buy and sell alerts must use the same primary pool")
    pool_address = next(iter(pool_addresses))
    cursor = max(0, int(state.get("dedust_alert_cursor_lt", 0) or 0))
    if cursor <= 0:
        cursor = fetch_toncenter_latest_transaction_lt(runtime, pool_address)
        state["dedust_alert_cursor_lt"] = cursor
        state["dedust_alert_last_sync_at"] = utc_now().isoformat()
        state["dedust_alert_last_error"] = ""
        save_state(state)
        log(f"Initialized reliable DeDust alert cursor at LT {cursor}")
        return 0, delivered

    snapshot = load_alert_market_snapshot()
    queued = 0
    for _page in range(DEDUST_ALERT_MAX_PAGES_PER_CHECK):
        trades = fetch_dedust_trades(runtime, pool_address, cursor)
        state["dedust_alert_last_sync_at"] = utc_now().isoformat()
        state["dedust_alert_last_error"] = ""
        if not trades:
            save_state(state)
            break
        alerts = parse_dedust_alert_events(state, trades, snapshot, pool_address)
        queued += enqueue_alert_events(state, alerts)
        next_cursor = max(int(item.get("lt") or 0) for item in trades)
        if next_cursor <= cursor:
            raise RuntimeError("DeDust returned a non-advancing trade cursor")
        cursor = next_cursor
        state["dedust_alert_cursor_lt"] = cursor
        state["dedust_alert_last_sync_at"] = utc_now().isoformat()
        state["dedust_alert_last_error"] = ""
        save_state(state)
        delivered += deliver_alert_outbox(runtime, state)
        if len(trades) < DEDUST_ALERT_PAGE_SIZE:
            break
    # An empty page is a successful poll, not a stale or failed scanner.
    state["dedust_alert_last_sync_at"] = utc_now().isoformat()
    state["dedust_alert_last_error"] = ""
    save_state(state)
    return queued, delivered


def sync_reliable_alerts(runtime: RuntimeConfig, state: dict[str, Any]) -> tuple[int, int]:
    if DEFAULT_ALERT_DEX == "stonfi":
        return sync_ston_alerts(runtime, state)
    if DEFAULT_ALERT_DEX == "dedust":
        return sync_dedust_alerts(runtime, state)
    raise RuntimeError(f"Unsupported alert DEX: {DEFAULT_ALERT_DEX}")


def build_buy_alert_dedupe_key(event: BuyAlertEvent) -> str:
    tx_hash = str(event.tx_hash or "").strip().lower()
    if tx_hash:
        return f"tx:{tx_hash}"
    wallet = str(event.wallet_address or "").strip().lower() or "-"
    timestamp = int(event.timestamp or 0)
    # Fallback key groups the same wallet/amount/timestamp into one alert when TonAPI
    # re-emits a logical buy under a different event id.
    amount_cents = int(round(float(event.utya_amount) * 100))
    return f"swap:{wallet}:{amount_cents}:{timestamp}"


def build_sell_alert_dedupe_key(event: SellAlertEvent) -> str:
    tx_hash = str(event.tx_hash or "").strip().lower()
    if tx_hash:
        return f"tx:{tx_hash}"
    wallet = str(event.wallet_address or "").strip().lower() or "-"
    timestamp = int(event.timestamp or 0)
    amount_cents = int(round(float(event.utya_amount) * 100))
    return f"swap:{wallet}:{amount_cents}:{timestamp}"


def parse_nano_gram_amount(value: Any) -> Optional[float]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        amount = int(raw) / 1_000_000_000
    except (TypeError, ValueError):
        return None
    return amount if amount > 0 else None


def extract_wallet_dns(account: dict[str, Any]) -> str:
    for key in ("name", "domain", "dns"):
        value = str(account.get(key) or "").strip().lower().rstrip(".")
        if value.endswith(".ton") and re.fullmatch(r"[a-z0-9_-]+(?:\.[a-z0-9_-]+)*\.ton", value):
            return value
    return ""


def gram_values_for_swap(
    *,
    gram_raw: Any,
    fallback_usd_amount: float,
    snapshot: MarketSnapshot,
) -> tuple[float, float]:
    gram_price = snapshot.gram_price_usd or load_shared_gram_price_cache()
    if gram_price is None or gram_price <= 0:
        raise RuntimeError("Live GRAM/USD price is unavailable; alert conversion was deferred")
    gram_amount = parse_nano_gram_amount(gram_raw)
    if gram_amount is None:
        gram_amount = fallback_usd_amount / gram_price
    return gram_amount, gram_amount * gram_price


def parse_buy_alert_events(state: dict[str, Any], events: list[dict[str, Any]], snapshot: MarketSnapshot) -> list[BuyAlertEvent]:
    pool_label = str(state.get("buy_alert_pool_label") or DEFAULT_ALERT_POOL_LABEL).strip() or DEFAULT_ALERT_POOL_LABEL
    pool_address = str(state.get("buy_alert_pool_address") or DEFAULT_ALERT_POOL_ADDRESS).strip() or DEFAULT_ALERT_POOL_ADDRESS
    threshold_usd = float(state.get("buy_alert_threshold_usd", DEFAULT_ALERT_THRESHOLD_USD) or DEFAULT_ALERT_THRESHOLD_USD)
    out: list[BuyAlertEvent] = []
    seen_keys: set[str] = set()
    for event in reversed(events):
        if not isinstance(event, dict):
            continue
        event_id = str(event.get("event_id") or "").strip()
        if not event_id:
            continue
        for action in list(event.get("actions") or []):
            if not isinstance(action, dict) or str(action.get("type") or "") != "JettonSwap":
                continue
            swap = action.get("JettonSwap") if isinstance(action.get("JettonSwap"), dict) else {}
            jetton_out = swap.get("jetton_master_out") if isinstance(swap.get("jetton_master_out"), dict) else {}
            master_out = str(jetton_out.get("address") or "").strip().lower()
            symbol_out = str(jetton_out.get("symbol") or "").strip().upper()
            if master_out != YODA_MASTER_ADDRESS or symbol_out != "YODA":
                continue
            amount_out_raw = str(swap.get("amount_out") or "").strip()
            decimals = int(jetton_out.get("decimals", 9) or 9)
            try:
                utya_amount = int(amount_out_raw) / (10 ** max(0, decimals))
            except Exception:
                continue
            fallback_usd_amount = float(utya_amount * snapshot.price_usd)
            gram_amount, usd_amount = gram_values_for_swap(
                gram_raw=swap.get("ton_in"),
                fallback_usd_amount=fallback_usd_amount,
                snapshot=snapshot,
            )
            if usd_amount < threshold_usd:
                continue
            user_wallet = swap.get("user_wallet") if isinstance(swap.get("user_wallet"), dict) else {}
            wallet_address = str(user_wallet.get("address") or "").strip() or "-"
            wallet_dns = extract_wallet_dns(user_wallet)
            tx_hash = ""
            for candidate in list(event.get("actions") or []):
                if not isinstance(candidate, dict):
                    continue
                base = candidate.get("base_transactions")
                if isinstance(base, list) and base:
                    first = base[0] if isinstance(base[0], dict) else {}
                    tx_hash = str(first.get("hash") or "").strip()
                    if tx_hash:
                        break
            timestamp = int(event.get("timestamp") or 0)
            happened_at = format_local_text(datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()) if timestamp else "-"
            alert_event = BuyAlertEvent(
                event_id=event_id,
                wallet_address=wallet_address,
                utya_amount=utya_amount,
                usd_amount=usd_amount,
                pool_label=pool_label,
                pool_address=pool_address,
                tx_hash=tx_hash,
                timestamp=timestamp,
                happened_at=happened_at,
                gram_amount=gram_amount,
                wallet_dns=wallet_dns,
            )
            dedupe_key = build_buy_alert_dedupe_key(alert_event)
            if dedupe_key in seen_keys:
                break
            seen_keys.add(dedupe_key)
            out.append(alert_event)
            break
    return out


def parse_sell_alert_events(state: dict[str, Any], events: list[dict[str, Any]], snapshot: MarketSnapshot) -> list[SellAlertEvent]:
    pool_label = str(state.get("sell_alert_pool_label") or DEFAULT_ALERT_POOL_LABEL).strip() or DEFAULT_ALERT_POOL_LABEL
    pool_address = str(state.get("sell_alert_pool_address") or DEFAULT_ALERT_POOL_ADDRESS).strip() or DEFAULT_ALERT_POOL_ADDRESS
    threshold_usd = float(state.get("sell_alert_threshold_usd", DEFAULT_ALERT_THRESHOLD_USD) or DEFAULT_ALERT_THRESHOLD_USD)
    out: list[SellAlertEvent] = []
    seen_keys: set[str] = set()
    for event in reversed(events):
        if not isinstance(event, dict):
            continue
        event_id = str(event.get("event_id") or "").strip()
        if not event_id:
            continue
        for action in list(event.get("actions") or []):
            if not isinstance(action, dict) or str(action.get("type") or "") != "JettonSwap":
                continue
            swap = action.get("JettonSwap") if isinstance(action.get("JettonSwap"), dict) else {}
            jetton_in = swap.get("jetton_master_in") if isinstance(swap.get("jetton_master_in"), dict) else {}
            master_in = str(jetton_in.get("address") or "").strip().lower()
            symbol_in = str(jetton_in.get("symbol") or "").strip().upper()
            if master_in != YODA_MASTER_ADDRESS or symbol_in != "YODA":
                continue
            amount_in_raw = str(swap.get("amount_in") or "").strip()
            decimals = int(jetton_in.get("decimals", 9) or 9)
            try:
                utya_amount = int(amount_in_raw) / (10 ** max(0, decimals))
            except Exception:
                continue
            fallback_usd_amount = float(utya_amount * snapshot.price_usd)
            gram_amount, usd_amount = gram_values_for_swap(
                gram_raw=swap.get("ton_out"),
                fallback_usd_amount=fallback_usd_amount,
                snapshot=snapshot,
            )
            if usd_amount < threshold_usd:
                continue
            user_wallet = swap.get("user_wallet") if isinstance(swap.get("user_wallet"), dict) else {}
            wallet_address = str(user_wallet.get("address") or "").strip() or "-"
            wallet_dns = extract_wallet_dns(user_wallet)
            tx_hash = ""
            for candidate in list(event.get("actions") or []):
                if not isinstance(candidate, dict):
                    continue
                base = candidate.get("base_transactions")
                if isinstance(base, list) and base:
                    first = base[0] if isinstance(base[0], dict) else {}
                    tx_hash = str(first.get("hash") or "").strip()
                    if tx_hash:
                        break
            timestamp = int(event.get("timestamp") or 0)
            happened_at = format_local_text(datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()) if timestamp else "-"
            alert_event = SellAlertEvent(
                event_id=event_id,
                wallet_address=wallet_address,
                utya_amount=utya_amount,
                usd_amount=usd_amount,
                pool_label=pool_label,
                pool_address=pool_address,
                tx_hash=tx_hash,
                timestamp=timestamp,
                happened_at=happened_at,
                gram_amount=gram_amount,
                wallet_dns=wallet_dns,
            )
            dedupe_key = build_sell_alert_dedupe_key(alert_event)
            if dedupe_key in seen_keys:
                break
            seen_keys.add(dedupe_key)
            out.append(alert_event)
            break
    return out


def format_alert_price(usd_amount: float, token_amount: float) -> str:
    if token_amount <= 0:
        return "-"
    price = usd_amount / token_amount
    if price >= 1:
        places = 4
    elif price >= 0.01:
        places = 6
    elif price >= 0.0001:
        places = 8
    else:
        places = 12
    return f"${price:,.{places}f}".rstrip("0").rstrip(".")


def alert_event_datetime(event: BuyAlertEvent | SellAlertEvent) -> datetime:
    if event.timestamp:
        return datetime.fromtimestamp(event.timestamp, tz=timezone.utc).astimezone()
    return utc_now().astimezone()


def tonviewer_transaction_url(tx_hash: str) -> str:
    return f"https://tonviewer.com/transaction/{urllib.parse.quote(str(tx_hash or '').strip())}"


def alert_template_values(
    event: BuyAlertEvent | SellAlertEvent,
    *,
    side: str,
) -> dict[str, str]:
    happened_at = alert_event_datetime(event)
    wallet = str(event.wallet_address or "-").strip() or "-"
    short_wallet = wallet_display_name(wallet, event.wallet_dns)
    short_address = shorten_wallet(wallet)
    tx_hash = str(event.tx_hash or "-").strip() or "-"
    values = {
        "[SIDE]": side.upper(),
        "[PRICE]": format_alert_price(event.usd_amount, event.utya_amount),
        "[USD_AMOUNT]": format_usd_value(event.usd_amount),
        "[GRAM_AMOUNT]": format_token_amount(event.gram_amount, 2),
        "[GRAM_USD]": format_usd_value(event.usd_amount),
        "[GRAM_SIZE]": format_gram_size(event.gram_amount, event.usd_amount),
        "[YODA_AMOUNT]": format_token_amount(event.utya_amount, 2),
        "[TOKEN_AMOUNT]": format_token_amount(event.utya_amount, 2),
        "[WALLET]": wallet,
        "[SHORT_WALLET]": short_wallet,
        "[SHORT_ADDRESS]": short_address,
        "[WALLET_SHORT]": short_wallet,
        "[WALLET_URL]": tonviewer_wallet_url(wallet) if wallet != "-" else "",
        "[TX_HASH]": tx_hash,
        "[TX_URL]": tonviewer_transaction_url(tx_hash) if tx_hash != "-" else "",
        "[POOL]": str(event.pool_label or "-").strip() or "-",
        "[POOL_ADDRESS]": str(event.pool_address or "-").strip() or "-",
        "[DATE]": happened_at.strftime("%d/%m/%Y"),
        "[TIME]": happened_at.strftime("%H:%M:%S"),
        "[UTC_TIME]": happened_at.astimezone(timezone.utc).strftime("%H:%M"),
        "[DATETIME]": happened_at.strftime("%d/%m/%Y %H:%M:%S"),
    }
    escaped = {key: html_lib.escape(value, quote=True) for key, value in values.items()}
    if wallet == "-":
        escaped["[WALLET_LINK]"] = "-"
        escaped["[WALLET_SHORT_LINK]"] = "-"
    else:
        wallet_url = escaped["[WALLET_URL]"]
        escaped["[WALLET_LINK]"] = f'<a href="{wallet_url}">Open wallet</a>'
        escaped["[WALLET_SHORT_LINK]"] = (
            f'<a href="{wallet_url}">{escaped["[SHORT_WALLET]"]}</a>'
        )
    return escaped


def validate_alert_template(template: str) -> str:
    value = normalize_alert_template_links(template)
    if not value:
        raise ValueError("the message template cannot be empty")
    if len(value) > MAX_ALERT_TEMPLATE_LENGTH:
        raise ValueError(f"the message template is longer than {MAX_ALERT_TEMPLATE_LENGTH} characters")
    unknown = sorted({token for token in ALERT_TEMPLATE_TOKEN_RE.findall(value) if token not in ALERT_TEMPLATE_PLACEHOLDERS})
    if unknown:
        raise ValueError(f"unknown placeholder(s): {', '.join(unknown)}")
    return value


def normalize_alert_template_links(template: str) -> str:
    def replace_escaped_wallet_link(match: re.Match[str]) -> str:
        label = html_lib.unescape(match.group(2)).strip()
        normalized_label = re.sub(r"<[^>]+>", "", label).strip().casefold()
        if normalized_label == "open wallet":
            return "[WALLET_LINK]"
        if label.casefold() in {"[wallet_short]", "[short_wallet]"}:
            return "[WALLET_SHORT_LINK]"
        safe_label = html_lib.escape(label or "Open wallet", quote=False)
        return f'<a href="[WALLET_URL]">{safe_label}</a>'

    value = str(template or "").strip()
    value = ALERT_TEMPLATE_ESCAPED_WALLET_LINK_RE.sub(replace_escaped_wallet_link, value)
    return ALERT_TEMPLATE_TOKEN_RE.sub(
        lambda match: ALERT_TEMPLATE_PLACEHOLDER_ALIASES.get(
            match.group(0).casefold(),
            match.group(0),
        ),
        value,
    )


def render_alert_template(
    template: str,
    event: BuyAlertEvent | SellAlertEvent,
    *,
    side: str,
) -> str:
    rendered = validate_alert_template(template)
    for placeholder, value in alert_template_values(event, side=side).items():
        rendered = rendered.replace(placeholder, value)
    if len(rendered) > 4096:
        raise ValueError("the rendered message exceeds Telegram's 4096-character limit")
    return rendered


def default_buy_alert_template(state: dict[str, Any]) -> str:
    return (
        "🟢 <b>BUY</b> • <b>[USD_AMOUNT]</b>\n\n"
        "💰 <b>[TOKEN_AMOUNT] YODA</b>\n"
        "⚖️ <b>[GRAM_AMOUNT] GRAM</b>\n"
        "📈 Price: <b>[PRICE]</b>\n\n"
        "👛 <code>[SHORT_ADDRESS]</code>\n"
        "🔗 <b>[WALLET_LINK]</b>\n"
        "⏰ [UTC_TIME] UTC"
    )


def default_sell_alert_template(state: dict[str, Any]) -> str:
    return (
        "🔴 <b>SELL</b> • <b>[USD_AMOUNT]</b>\n\n"
        "💰 <b>[TOKEN_AMOUNT] YODA</b>\n"
        "⚖️ <b>[GRAM_AMOUNT] GRAM</b>\n"
        "📉 Price: <b>[PRICE]</b>\n\n"
        "👛 <code>[SHORT_ADDRESS]</code>\n"
        "🔗 <b>[WALLET_LINK]</b>\n"
        "⏰ [UTC_TIME] UTC"
    )


def build_buy_alert_text(state: dict[str, Any], event: BuyAlertEvent) -> str:
    custom_template = str(state.get("buy_alert_template") or "").strip()
    if custom_template:
        return render_alert_template(custom_template, event, side="BUY")
    return render_alert_template(default_buy_alert_template(state), event, side="BUY")


def build_sell_alert_text(state: dict[str, Any], event: SellAlertEvent) -> str:
    custom_template = str(state.get("sell_alert_template") or "").strip()
    if custom_template:
        return render_alert_template(custom_template, event, side="SELL")
    return render_alert_template(default_sell_alert_template(state), event, side="SELL")


def format_price(price: float, decimal_places: int) -> str:
    places = clamp_decimal_places(decimal_places)
    text = f"{price:,.{places}f}"
    return f"${text}"


def format_change_percent(change: Optional[float]) -> str:
    if change is None:
        return "-"
    sign = "+" if change > 0 else ""
    return f"{sign}{change:.2f}%"


def build_channel_post_text(
    snapshot: MarketSnapshot,
    decimal_places: int,
    *,
    show_change_1h: bool,
    show_change_24h: bool,
    show_change_7d: bool,
) -> str:
    price_text = format_price(snapshot.price_usd, decimal_places)
    change_1h = format_change_percent(snapshot.change_1h_percent)
    change_24h = format_change_percent(snapshot.change_24h_percent)
    change_7d = format_change_percent(snapshot.change_7d_percent)
    lines = [f"<b>{price_text}</b>"]
    if show_change_1h:
        lines.append(f"<code>1h: {change_1h}</code>")
    if show_change_24h:
        lines.append(f"<code>24h: {change_24h}</code>")
    if show_change_7d:
        lines.append(f"<code>7d: {change_7d}</code>")
    return "\n".join(lines)


def send_message(
    runtime: RuntimeConfig,
    chat_id: str,
    text: str,
    *,
    parse_mode: Optional[str] = "HTML",
    reply_markup: Optional[dict[str, Any]] = None,
    thread_id: Optional[str] = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": "true",
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    if thread_id:
        payload["message_thread_id"] = thread_id
    return api_request(runtime, "sendMessage", payload)


def edit_message_text(
    runtime: RuntimeConfig,
    chat_id: str,
    message_id: int,
    text: str,
    *,
    parse_mode: Optional[str] = "HTML",
    reply_markup: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "disable_web_page_preview": "true",
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    return api_request(runtime, "editMessageText", payload)


def answer_callback_query(runtime: RuntimeConfig, callback_query_id: str, text: str = "") -> None:
    payload = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
        payload["show_alert"] = "false"
    try:
        api_request(runtime, "answerCallbackQuery", payload)
    except Exception:
        pass


def set_my_commands(runtime: RuntimeConfig) -> None:
    commands = [
        {"command": "start", "description": "Open the control menu"},
        {"command": "menu", "description": "Open the control menu"},
        {"command": "status", "description": "Show current status"},
        {"command": "activity", "description": "Show recent admin actions"},
        {"command": "post", "description": "Post the price now"},
        {"command": "pause", "description": "Pause scheduled posting"},
        {"command": "resume", "description": "Resume scheduled posting"},
        {"command": "logs", "description": "Show recent logs"},
        {"command": "alerts", "description": "Open buy alert settings"},
        {"command": "bind", "description": "Bind this chat/topic as control"},
    ]
    try:
        api_request(runtime, "setMyCommands", {"commands": commands})
    except Exception as exc:
        log(f"setMyCommands failed: {type(exc).__name__}: {exc}")


def get_updates(runtime: RuntimeConfig, offset: int, timeout_seconds: int) -> list[dict[str, Any]]:
    payload = {
        "offset": offset + 1,
        "timeout": max(1, timeout_seconds),
        "allowed_updates": ["message", "callback_query"],
    }
    result = api_request(runtime, "getUpdates", payload)
    updates = result.get("result")
    return updates if isinstance(updates, list) else []


def chat_id_from_message(message: dict[str, Any]) -> str:
    chat = message.get("chat") or {}
    return str(chat.get("id", "")).strip()


def chat_type_from_message(message: dict[str, Any]) -> str:
    chat = message.get("chat") or {}
    return str(chat.get("type", "")).strip().lower()


def thread_id_from_message(message: dict[str, Any]) -> str:
    return str(message.get("message_thread_id") or "").strip()


def user_id_from_message(message: dict[str, Any]) -> str:
    user = message.get("from") or {}
    return str(user.get("id", "")).strip()


def get_allowed_user_ids(state: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for item in list(state.get("allowed_user_ids") or []):
        token = str(item or "").strip()
        if token and token not in out:
            out.append(token)
    owner_id = str(state.get("control_user_id") or "").strip()
    if owner_id and owner_id not in out:
        out.insert(0, owner_id)
    state["allowed_user_ids"] = out
    return out


def get_ui_session(state: dict[str, Any], user_id: str, *, create: bool = False) -> dict[str, Any]:
    sessions = state.setdefault("ui_sessions", {})
    session = sessions.get(str(user_id))
    if not isinstance(session, dict):
        session = {}
        if create:
            sessions[str(user_id)] = session
    if create:
        session.setdefault("chat_id", "")
        session.setdefault("menu_message_id", 0)
        session.setdefault("menu_page", "home")
        session.setdefault("ui_notice", "")
        session.setdefault("ui_notice_at", "")
    return session


def set_pending_input(state: dict[str, Any], user_id: str, action: str, *, chat_id: str, thread_id: str = "") -> None:
    pending = state.setdefault("pending_inputs", {})
    pending[str(user_id)] = {
        "action": action,
        "chat_id": str(chat_id or "").strip(),
        "thread_id": str(thread_id or "").strip(),
        "created_at": utc_now().isoformat(),
    }


def pop_pending_input(state: dict[str, Any], user_id: str) -> Optional[dict[str, Any]]:
    pending = state.setdefault("pending_inputs", {})
    value = pending.pop(str(user_id), None)
    return value if isinstance(value, dict) else None


def get_pending_input(state: dict[str, Any], user_id: str) -> Optional[dict[str, Any]]:
    pending = state.setdefault("pending_inputs", {})
    value = pending.get(str(user_id))
    return value if isinstance(value, dict) else None


def format_interval(seconds: int) -> str:
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def parse_interval_input(raw: str) -> int:
    text = str(raw or "").strip().lower()
    if not text:
        raise ValueError("Interval is empty.")
    multipliers = {"s": 1, "sec": 1, "secs": 1, "second": 1, "seconds": 1, "m": 60, "min": 60, "mins": 60, "minute": 60, "minutes": 60, "h": 3600, "hr": 3600, "hrs": 3600, "hour": 3600, "hours": 3600}
    for suffix, mult in sorted(multipliers.items(), key=lambda item: len(item[0]), reverse=True):
        if text.endswith(suffix):
            number = text[: -len(suffix)].strip()
            if not number:
                raise ValueError("Missing interval number.")
            return clamp_interval(int(float(number) * mult))
    return clamp_interval(int(float(text) * 60))


def parse_decimal_places_input(raw: str) -> int:
    text = str(raw or "").strip()
    if not text:
        raise ValueError("Decimal places are empty.")
    return clamp_decimal_places(int(text))


def parse_threshold_input(raw: str) -> float:
    text = str(raw or "").strip().replace(",", "")
    if not text:
        raise ValueError("Threshold is empty.")
    value = float(text)
    if value < 0:
        raise ValueError("Threshold cannot be negative.")
    return value


def normalize_channel_input(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        raise ValueError("Channel cannot be empty.")
    if text.startswith("@"):
        if len(text) < 2:
            raise ValueError("Invalid channel username.")
        return text
    if text.startswith("-100") or text.lstrip("-").isdigit():
        return text
    raise ValueError("Channel must be @username or numeric chat id.")


def normalize_alert_pool_label_input(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        raise ValueError("Pool label cannot be empty.")
    return text[:80]


def format_usd_value(value: float) -> str:
    return f"${value:,.2f}"


def format_token_amount(value: float, decimal_places: int = 2) -> str:
    return f"{value:,.{max(0, decimal_places)}f}"


def format_gram_size(gram_amount: float, usd_amount: float) -> str:
    return f"{format_token_amount(gram_amount, 2)} GRAM ({format_usd_value(usd_amount)})"


def shorten_wallet(value: str) -> str:
    text = str(value or "").strip()
    if len(text) <= 20:
        return text or "-"
    return f"{text[:10]}...{text[-8:]}"


def wallet_display_name(wallet_address: str, wallet_dns: str = "") -> str:
    dns_name = str(wallet_dns or "").strip().lower().rstrip(".")
    if dns_name.endswith(".ton"):
        return dns_name
    return shorten_wallet(wallet_address)


def load_wallet_dns_cache() -> dict[str, dict[str, Any]]:
    global _WALLET_DNS_CACHE
    if _WALLET_DNS_CACHE is not None:
        return _WALLET_DNS_CACHE
    try:
        payload = json.loads(WALLET_DNS_CACHE_PATH.read_text(encoding="utf-8"))
        _WALLET_DNS_CACHE = payload if isinstance(payload, dict) else {}
    except Exception:
        _WALLET_DNS_CACHE = {}
    return _WALLET_DNS_CACHE


def save_wallet_dns_cache(cache: dict[str, dict[str, Any]]) -> None:
    try:
        temporary_path = WALLET_DNS_CACHE_PATH.with_suffix(".json.tmp")
        temporary_path.write_text(
            json.dumps(cache, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        temporary_path.replace(WALLET_DNS_CACHE_PATH)
    except Exception as exc:
        log(f"Wallet DNS cache save failed: {type(exc).__name__}: {exc}")


def parse_backresolved_dns(payload: Any) -> str:
    domains = payload.get("domains") if isinstance(payload, dict) else []
    candidates: list[str] = []
    for item in domains if isinstance(domains, list) else []:
        if isinstance(item, dict):
            value = str(item.get("name") or item.get("domain") or "")
        else:
            value = str(item or "")
        value = value.strip().lower().rstrip(".")
        if value.endswith(".ton") and re.fullmatch(r"[a-z0-9_-]+(?:\.[a-z0-9_-]+)*\.ton", value):
            candidates.append(value)
    return min(candidates, key=lambda item: (len(item), item)) if candidates else ""


def resolve_wallet_dns(runtime: RuntimeConfig, wallet_address: str) -> str:
    address = str(wallet_address or "").strip()
    if not address or address == "-":
        return ""
    cache = load_wallet_dns_cache()
    cache_key = address.lower()
    now = time.time()
    cached = cache.get(cache_key) if isinstance(cache.get(cache_key), dict) else {}
    cached_name = str(cached.get("name") or "").strip()
    checked_at = float(cached.get("checked_at") or 0.0)
    ttl = WALLET_DNS_POSITIVE_TTL_SECONDS if cached_name else WALLET_DNS_NEGATIVE_TTL_SECONDS
    if checked_at > 0 and now - checked_at < ttl:
        return cached_name

    url = TONAPI_ACCOUNT_DNS_BACKRESOLVE_URL.format(
        account_id=urllib.parse.quote(address, safe=""),
    )
    request = build_request(url, user_agent=runtime.user_agent)
    api_key = str(os.getenv("TONAPI_TOKEN") or os.getenv("TONAPI_KEY") or "").strip()
    if api_key:
        request.add_header("Authorization", f"Bearer {api_key}")
    try:
        with urllib.request.urlopen(
            request,
            timeout=min(runtime.timeout_seconds, EXTERNAL_DATA_TIMEOUT_SECONDS),
        ) as response:
            payload = json.loads(response.read().decode("utf-8"))
        dns_name = parse_backresolved_dns(payload)
    except Exception as exc:
        log(f"Wallet DNS lookup failed for {shorten_wallet(address)}: {type(exc).__name__}: {exc}")
        return cached_name

    cache[cache_key] = {"name": dns_name, "checked_at": now}
    save_wallet_dns_cache(cache)
    return dns_name


def enrich_alert_wallet_dns(
    runtime: RuntimeConfig,
    event: BuyAlertEvent | SellAlertEvent,
) -> BuyAlertEvent | SellAlertEvent:
    if event.wallet_dns:
        return event
    dns_name = resolve_wallet_dns(runtime, event.wallet_address)
    return replace(event, wallet_dns=dns_name) if dns_name else event


def tonviewer_wallet_url(address: str) -> str:
    return f"https://tonviewer.com/{urllib.parse.quote(str(address or '').strip())}"


def escape_html(value: str) -> str:
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def utf16_boundary_map(text: str) -> dict[int, int]:
    boundaries: dict[int, int] = {0: 0}
    units = 0
    for index, char in enumerate(text):
        units += 2 if ord(char) > 0xFFFF else 1
        boundaries[units] = index + 1
    return boundaries


def telegram_entity_tags(entity: dict[str, Any]) -> tuple[str, str]:
    entity_type = str(entity.get("type") or "").strip()
    if entity_type == "bold":
        return "<b>", "</b>"
    if entity_type == "italic":
        return "<i>", "</i>"
    if entity_type == "underline":
        return "<u>", "</u>"
    if entity_type == "strikethrough":
        return "<s>", "</s>"
    if entity_type == "spoiler":
        return '<span class="tg-spoiler">', "</span>"
    if entity_type == "code":
        return "<code>", "</code>"
    if entity_type == "pre":
        language = str(entity.get("language") or "").strip()
        if language:
            return f'<pre><code class="language-{html_lib.escape(language, quote=True)}">', "</code></pre>"
        return "<pre>", "</pre>"
    if entity_type == "text_link":
        url = html_lib.escape(str(entity.get("url") or "").strip(), quote=True)
        return (f'<a href="{url}">', "</a>") if url else ("", "")
    if entity_type == "text_mention":
        user = entity.get("user") if isinstance(entity.get("user"), dict) else {}
        target_id = str(user.get("id") or "").strip()
        return (f'<a href="tg://user?id={target_id}">', "</a>") if target_id else ("", "")
    if entity_type == "blockquote":
        return "<blockquote>", "</blockquote>"
    if entity_type == "expandable_blockquote":
        return "<blockquote expandable>", "</blockquote>"
    if entity_type == "custom_emoji":
        emoji_id = html_lib.escape(str(entity.get("custom_emoji_id") or "").strip(), quote=True)
        return (f'<tg-emoji emoji-id="{emoji_id}">', "</tg-emoji>") if emoji_id else ("", "")
    return "", ""


def rich_text_to_html(text: str, entities: object) -> str:
    value = str(text or "")
    entity_list = [item for item in list(entities or []) if isinstance(item, dict)]
    tagged: list[tuple[int, int, int, str, str]] = []
    boundaries = utf16_boundary_map(value)
    for order, entity in enumerate(entity_list):
        opening, closing = telegram_entity_tags(entity)
        if not opening:
            continue
        offset = int(entity.get("offset", 0) or 0)
        length = int(entity.get("length", 0) or 0)
        start = boundaries.get(offset)
        end = boundaries.get(offset + length)
        if start is None or end is None or end <= start:
            continue
        tagged.append((start, end, order, opening, closing))
    if not tagged:
        return value.strip()

    openings: dict[int, list[tuple[int, int, int, str, str]]] = {}
    closings: dict[int, list[tuple[int, int, int, str, str]]] = {}
    for item in tagged:
        openings.setdefault(item[0], []).append(item)
        closings.setdefault(item[1], []).append(item)

    out: list[str] = []
    for index in range(len(value) + 1):
        for item in sorted(closings.get(index, []), key=lambda row: (row[0], row[2]), reverse=True):
            out.append(item[4])
        for item in sorted(openings.get(index, []), key=lambda row: (row[1], -row[2]), reverse=True):
            out.append(item[3])
        if index < len(value):
            out.append(html_lib.escape(value[index], quote=False))
    return "".join(out).strip()


def sample_buy_alert_event() -> BuyAlertEvent:
    return BuyAlertEvent(
        event_id="template-preview-buy",
        wallet_address="EQDkExampleBuyerWalletAddress1234567890",
        utya_amount=250000.0,
        usd_amount=6250.0,
        pool_label=DEFAULT_ALERT_POOL_LABEL,
        pool_address=DEFAULT_ALERT_POOL_ADDRESS,
        tx_hash="example-buy-transaction-hash",
        timestamp=int(time.time()),
        happened_at=format_local_text(utc_now().isoformat()),
        gram_amount=4325.25,
        wallet_dns="buyer.ton",
    )


def sample_sell_alert_event() -> SellAlertEvent:
    return SellAlertEvent(
        event_id="template-preview-sell",
        wallet_address="EQDkExampleSellerWalletAddress123456789",
        utya_amount=180000.0,
        usd_amount=5400.0,
        pool_label=DEFAULT_ALERT_POOL_LABEL,
        pool_address=DEFAULT_ALERT_POOL_ADDRESS,
        tx_hash="example-sell-transaction-hash",
        timestamp=int(time.time()),
        happened_at=format_local_text(utc_now().isoformat()),
        gram_amount=3737.02,
        wallet_dns="seller.ton",
    )


def alert_template_for_copy(template: str) -> str:
    def replace_dynamic_link(match: re.Match[str]) -> str:
        url_template = match.group(2).strip()
        label = match.group(3).strip()
        if url_template == "[WALLET_URL]":
            plain_label = re.sub(r"<[^>]+>", "", label).strip().casefold()
            if "[wallet_short]" in label.casefold():
                return "[WALLET_SHORT_LINK]"
            if plain_label == "open wallet":
                return "[WALLET_LINK]"
        return f"{label}: {url_template}" if label else url_template

    return ALERT_TEMPLATE_DYNAMIC_LINK_RE.sub(replace_dynamic_link, str(template or "").strip())


def current_alert_template(state: dict[str, Any], *, side: str) -> tuple[str, bool]:
    is_buy = side.upper() == "BUY"
    saved = str(state.get("buy_alert_template" if is_buy else "sell_alert_template") or "").strip()
    template = saved or (default_buy_alert_template(state) if is_buy else default_sell_alert_template(state))
    return template, bool(saved)


def alert_template_preview_text(state: dict[str, Any], *, side: str) -> str:
    template, is_custom = current_alert_template(state, side=side)
    return (
        f"📋 <b>Current {side.upper()} alert layout</b>\n"
        f"<i>{'Custom' if is_custom else 'Default'} layout · values remain in brackets for editing</i>\n\n"
        f"{alert_template_for_copy(template)}"
    )


def alert_template_prompt_text(state: dict[str, Any], *, side: str) -> str:
    current, is_custom = current_alert_template(state, side=side)
    current_preview = current if len(current) <= 1800 else current[:1800] + "\n..."
    return (
        f"✏️ <b>Edit {side.upper()} alert message</b>\n\n"
        "Send the complete new layout in one message. You can use Telegram formatting and custom emojis.\n\n"
        "<b>Available values</b>\n"
        "<code>[PRICE]</code> current YODA price\n"
        "<code>[USD_AMOUNT]</code> trade value in USD\n"
        "<code>[GRAM_SIZE]</code> GRAM amount with USD equivalent in parentheses\n"
        "<code>[GRAM_AMOUNT]</code> GRAM amount only\n"
        "<code>[GRAM_USD]</code> GRAM value in USD only\n"
        "<code>[YODA_AMOUNT]</code> YODA amount\n"
        "<code>[TOKEN_AMOUNT]</code> token amount\n"
        "<code>[WALLET]</code> full wallet\n"
        "<code>[SHORT_WALLET]</code> .ton name when available, otherwise a shortened wallet\n"
        "<code>[SHORT_ADDRESS]</code> shortened raw wallet address\n"
        "<code>[WALLET_LINK]</code> clickable <i>Open wallet</i> link\n"
        "<code>[WALLET_SHORT_LINK]</code> clickable .ton name or shortened wallet\n"
        "<code>[WALLET_URL]</code> Tonviewer wallet URL\n"
        "<code>[TX_HASH]</code> transaction hash\n"
        "<code>[TX_URL]</code> Tonviewer transaction URL\n"
        "<code>[POOL]</code> pool name\n"
        "<code>[POOL_ADDRESS]</code> pool address\n"
        "<code>[SIDE]</code> BUY or SELL\n"
        "<code>[DATE]</code> <code>[TIME]</code> <code>[UTC_TIME]</code> <code>[DATETIME]</code>\n\n"
        "<b>Easy clickable-wallet examples</b>\n"
        "<code>🔗 [WALLET_LINK]</code>\n"
        "<code>👛 Wallet: [SHORT_WALLET]</code>\n"
        "<code>💵 Size: [GRAM_SIZE]</code>\n"
        "No HTML is required.\n\n"
        f"<b>Current {'custom' if is_custom else 'default'} layout</b>\n"
        "<i>Copy the formatted text below, edit it, and send it back.</i>\n\n"
        f"{alert_template_for_copy(current_preview)}\n\n"
        "Send <code>cancel</code> to keep it unchanged."
    )


def build_menu_text(state: dict[str, Any], runtime: RuntimeConfig, user_id: str) -> str:
    status = "on" if runtime.posting_enabled else "off"
    last_post = format_local_text(str(state.get("last_post_at") or ""))
    last_error = str(state.get("last_error") or "").strip() or "-"
    last_error_at = format_local_text(str(state.get("last_error_at") or ""))
    last_price = str(state.get("last_price") or "").strip() or "-"
    last_change_1h = str(state.get("last_change_1h") or "").strip() or "-"
    last_change_24h = str(state.get("last_change_24h") or "").strip() or "-"
    last_change_7d = str(state.get("last_change_7d") or "").strip() or "-"
    last_post_text = str(state.get("last_post_text") or "").strip() or "-"
    last_buy_alert_at = format_local_text(str(state.get("last_buy_alert_at") or ""))
    last_buy_alert_text = str(state.get("last_buy_alert_text") or "").strip() or "-"
    last_buy_alert_wallet = str(state.get("last_buy_alert_wallet") or "").strip() or "-"
    last_sell_alert_at = format_local_text(str(state.get("last_sell_alert_at") or ""))
    last_sell_alert_text = str(state.get("last_sell_alert_text") or "").strip() or "-"
    last_sell_alert_wallet = str(state.get("last_sell_alert_wallet") or "").strip() or "-"
    alert_display_flags = " | ".join(
        [
            f"wallet:{'on' if state.get('show_buy_alert_wallet', True) else 'off'}",
            f"utya:{'on' if state.get('show_buy_alert_utya', True) else 'off'}",
            f"usd:{'on' if state.get('show_buy_alert_usd', True) else 'off'}",
            f"link:{'on' if state.get('show_buy_alert_link', True) else 'off'}",
        ]
    )
    sell_alert_display_flags = " | ".join(
        [
            f"wallet:{'on' if state.get('show_sell_alert_wallet', True) else 'off'}",
            f"utya:{'on' if state.get('show_sell_alert_utya', True) else 'off'}",
            f"usd:{'on' if state.get('show_sell_alert_usd', True) else 'off'}",
            f"link:{'on' if state.get('show_sell_alert_link', True) else 'off'}",
        ]
    )
    display_flags = " | ".join(
        [
            f"1h:{'on' if state.get('show_change_1h', True) else 'off'}",
            f"24h:{'on' if state.get('show_change_24h', True) else 'off'}",
            f"7d:{'on' if state.get('show_change_7d', True) else 'off'}",
        ]
    )
    session = get_ui_session(state, user_id, create=True)
    bound_chat = str(session.get("chat_id") or "").strip() or "-"
    bound_users = ", ".join(get_allowed_user_ids(state)) or "-"
    lines = [
        "💸 <b>YODA Price Bot</b>",
        "",
        "Private control panel for channel posting, formatting, and admin-only updates.",
        "",
        "📡 <b>Posting</b>",
        f"• <b>Status:</b> <code>{status.upper()}</code>",
        f"• <b>Channel:</b> <code>{runtime.channel}</code>",
        f"• <b>Interval:</b> <code>{format_interval(runtime.interval_seconds)}</code>",
        f"• <b>Decimals:</b> <code>{runtime.decimal_places}</code>",
        f"• <b>Display lines:</b> <code>{display_flags}</code>",
        "",
        "📈 <b>Market Snapshot</b>",
        f"• <b>Last price:</b> <code>{last_price}</code>",
        f"• <b>± 1h:</b> <code>{last_change_1h}</code>",
        f"• <b>24h change:</b> <code>{last_change_24h}</code>",
        f"• <b>7d change:</b> <code>{last_change_7d}</code>",
        "",
        "🧾 <b>Last Delivery</b>",
        f"• <b>Sent text:</b> <code>{last_post_text}</code>",
        f"• <b>Posted at:</b> <code>{last_post}</code>",
        f"• <b>Last error:</b> <code>{last_error}</code>",
        f"• <b>Error time:</b> <code>{last_error_at}</code>",
        "",
        "📈 <b>Buy Alerts</b>",
        f"• <b>Status:</b> <code>{'ON' if state.get('buy_alerts_enabled', True) else 'OFF'}</code>",
        f"• <b>Alert channel:</b> <code>{get_buy_alert_channel(state, runtime)}</code>",
        f"• <b>Threshold:</b> <code>{format_usd_value(float(state.get('buy_alert_threshold_usd', DEFAULT_ALERT_THRESHOLD_USD) or DEFAULT_ALERT_THRESHOLD_USD))}</code>",
        f"• <b>Check every:</b> <code>{format_interval(int(state.get('buy_alert_interval_seconds', DEFAULT_ALERT_INTERVAL_SECONDS) or DEFAULT_ALERT_INTERVAL_SECONDS))}</code>",
        f"• <b>Display:</b> <code>{alert_display_flags}</code>",
        f"• <b>Last alert:</b> <code>{last_buy_alert_text}</code> <code>{last_buy_alert_at}</code>",
        f"• <b>Last buyer:</b> <code>{escape_html(shorten_wallet(last_buy_alert_wallet))}</code>",
        "",
        "📉 <b>Sell Alerts</b>",
        f"• <b>Status:</b> <code>{'ON' if state.get('sell_alerts_enabled', True) else 'OFF'}</code>",
        f"• <b>Alert channel:</b> <code>{get_sell_alert_channel(state, runtime)}</code>",
        f"• <b>Threshold:</b> <code>{format_usd_value(float(state.get('sell_alert_threshold_usd', DEFAULT_ALERT_THRESHOLD_USD) or DEFAULT_ALERT_THRESHOLD_USD))}</code>",
        f"• <b>Check every:</b> <code>{format_interval(int(state.get('sell_alert_interval_seconds', DEFAULT_ALERT_INTERVAL_SECONDS) or DEFAULT_ALERT_INTERVAL_SECONDS))}</code>",
        f"• <b>Display:</b> <code>{sell_alert_display_flags}</code>",
        f"• <b>Last alert:</b> <code>{last_sell_alert_text}</code> <code>{last_sell_alert_at}</code>",
        f"• <b>Last seller:</b> <code>{escape_html(shorten_wallet(last_sell_alert_wallet))}</code>",
        "",
        "🔐 <b>Control Access</b>",
        f"• <b>Your chat:</b> <code>{bound_chat}</code>",
        f"• <b>Allowed ids:</b> <code>{bound_users}</code>",
    ]
    notice = str(session.get("ui_notice") or "").strip()
    if notice:
        notice_at = format_local_text(str(session.get("ui_notice_at") or ""))
        lines.extend(["", f"ℹ️ <b>Notice</b> <code>{notice_at}</code>", f"<code>{escape_html(notice)}</code>"])
    return "\n".join(lines)


def build_logs_text(state: dict[str, Any], user_id: str, limit: int = DEFAULT_LOG_LINES) -> str:
    lines = tail_log_lines(limit)
    rendered = "\n".join(lines[-limit:]) if lines else "No log lines yet."
    out = [
        "🧾 <b>Recent Logs</b>",
        "",
        f"<pre>{escape_html(rendered)}</pre>",
    ]
    session = get_ui_session(state, user_id, create=True)
    notice = str(session.get("ui_notice") or "").strip()
    if notice:
        out.extend(["", "ℹ️ <b>Notice</b>", f"<code>{escape_html(notice)}</code>"])
    return "\n".join(out)


def build_activity_text(state: dict[str, Any], user_id: str, limit: int = DEFAULT_LOG_LINES) -> str:
    entries = list(state.get("activity_log") or [])
    if not entries:
        rendered = "No admin activity yet."
    else:
        lines: list[str] = []
        for item in entries[-limit:]:
            if not isinstance(item, dict):
                continue
            at = format_local_text(str(item.get("at") or ""))
            actor = str(item.get("actor") or "").strip() or "unknown"
            action = str(item.get("action") or "").strip() or "-"
            lines.append(f"[{at}] {actor} {action}")
        rendered = "\n".join(lines) if lines else "No admin activity yet."
    out = [
        "🕘 <b>Recent Activity</b>",
        "",
        f"<pre>{escape_html(rendered)}</pre>",
    ]
    session = get_ui_session(state, user_id, create=True)
    notice = str(session.get("ui_notice") or "").strip()
    if notice:
        out.extend(["", "ℹ️ <b>Notice</b>", f"<code>{escape_html(notice)}</code>"])
    return "\n".join(out)


def build_menu_markup(state: dict[str, Any], runtime: RuntimeConfig, page: str = "home") -> dict[str, Any]:
    if page == "logs":
        return {
            "inline_keyboard": [
                [{"text": "🔄 Refresh Logs", "callback_data": "menu:logs"}],
                [{"text": "⬅️ Back", "callback_data": "menu:home"}],
            ]
        }
    if page == "activity":
        return {
            "inline_keyboard": [
                [{"text": "🔄 Refresh Activity", "callback_data": "menu:activity"}],
                [{"text": "⬅️ Back", "callback_data": "menu:home"}],
            ]
        }
    if page == "display":
        return {
            "inline_keyboard": [
                [
                    {"text": f"{'✅' if state.get('show_change_1h', True) else '🚫'} ± 1h", "callback_data": "action:toggle_change:1h"},
                    {"text": f"{'✅' if state.get('show_change_24h', True) else '🚫'} 24h", "callback_data": "action:toggle_change:24h"},
                ],
                [
                    {"text": f"{'✅' if state.get('show_change_7d', True) else '🚫'} 7d", "callback_data": "action:toggle_change:7d"},
                ],
                [{"text": "⬅️ Back", "callback_data": "menu:home"}],
            ]
        }
    if page == "alerts":
        current_seconds = int(state.get("buy_alert_interval_seconds", DEFAULT_ALERT_INTERVAL_SECONDS) or DEFAULT_ALERT_INTERVAL_SECONDS)
        threshold_text = format_usd_value(float(state.get("buy_alert_threshold_usd", DEFAULT_ALERT_THRESHOLD_USD) or DEFAULT_ALERT_THRESHOLD_USD))
        channel_text = get_buy_alert_channel(state, runtime)
        return {
            "inline_keyboard": [
                [
                    {"text": "🟢 Enabled" if state.get("buy_alerts_enabled", True) else "🔴 Disabled", "callback_data": "action:toggle_buy_alerts"},
                    {"text": f"💰 {threshold_text}", "callback_data": "prompt:set_buy_alert_threshold"},
                ],
                [
                    {"text": f"{'✅ ' if current_seconds == 10 else ''}10s", "callback_data": "action:set_buy_alert_interval:10"},
                    {"text": f"{'✅ ' if current_seconds == 30 else ''}30s", "callback_data": "action:set_buy_alert_interval:30"},
                    {"text": f"{'✅ ' if current_seconds == 60 else ''}1m", "callback_data": "action:set_buy_alert_interval:60"},
                ],
                [
                    {"text": f"{'✅ ' if current_seconds == 120 else ''}2m", "callback_data": "action:set_buy_alert_interval:120"},
                    {"text": "✍️ Custom", "callback_data": "prompt:set_buy_alert_interval"},
                ],
                [
                    {"text": "🧩 Display Fields", "callback_data": "menu:alert_display"},
                    {"text": "📣 Alert Channel", "callback_data": "prompt:set_buy_alert_channel"},
                ],
                [
                    {"text": "✏️ Edit Buy Message", "callback_data": "prompt:set_buy_alert_template"},
                    {"text": "👁 Preview", "callback_data": "action:preview_buy_alert_template"},
                ],
                [
                    {"text": "↩️ Reset Layout", "callback_data": "action:reset_buy_alert_template"},
                ],
                [
                    {"text": "📉 Sell Alerts", "callback_data": "menu:sell_alerts"},
                ],
                [
                    {"text": f"🔗 {channel_text[:20]}..." if len(channel_text) > 20 else f"🔗 {channel_text}", "callback_data": "menu:alerts"},
                ],
                [{"text": "⬅️ Back", "callback_data": "menu:home"}],
            ]
        }
    if page == "sell_alerts":
        current_seconds = int(state.get("sell_alert_interval_seconds", DEFAULT_ALERT_INTERVAL_SECONDS) or DEFAULT_ALERT_INTERVAL_SECONDS)
        threshold_text = format_usd_value(float(state.get("sell_alert_threshold_usd", DEFAULT_ALERT_THRESHOLD_USD) or DEFAULT_ALERT_THRESHOLD_USD))
        channel_text = get_sell_alert_channel(state, runtime)
        return {
            "inline_keyboard": [
                [
                    {"text": "🟢 Enabled" if state.get("sell_alerts_enabled", True) else "🔴 Disabled", "callback_data": "action:toggle_sell_alerts"},
                    {"text": f"💸 {threshold_text}", "callback_data": "prompt:set_sell_alert_threshold"},
                ],
                [
                    {"text": f"{'✅ ' if current_seconds == 10 else ''}10s", "callback_data": "action:set_sell_alert_interval:10"},
                    {"text": f"{'✅ ' if current_seconds == 30 else ''}30s", "callback_data": "action:set_sell_alert_interval:30"},
                    {"text": f"{'✅ ' if current_seconds == 60 else ''}1m", "callback_data": "action:set_sell_alert_interval:60"},
                ],
                [
                    {"text": f"{'✅ ' if current_seconds == 120 else ''}2m", "callback_data": "action:set_sell_alert_interval:120"},
                    {"text": "✍️ Custom", "callback_data": "prompt:set_sell_alert_interval"},
                ],
                [
                    {"text": "🧩 Display Fields", "callback_data": "menu:sell_alert_display"},
                    {"text": "📣 Alert Channel", "callback_data": "prompt:set_sell_alert_channel"},
                ],
                [
                    {"text": "✏️ Edit Sell Message", "callback_data": "prompt:set_sell_alert_template"},
                    {"text": "👁 Preview", "callback_data": "action:preview_sell_alert_template"},
                ],
                [
                    {"text": "↩️ Reset Layout", "callback_data": "action:reset_sell_alert_template"},
                ],
                [
                    {"text": "📈 Buy Alerts", "callback_data": "menu:alerts"},
                ],
                [
                    {"text": f"🔗 {channel_text[:20]}..." if len(channel_text) > 20 else f"🔗 {channel_text}", "callback_data": "menu:sell_alerts"},
                ],
                [{"text": "⬅️ Back", "callback_data": "menu:home"}],
            ]
        }
    if page == "alert_display":
        return {
            "inline_keyboard": [
                [
                    {"text": f"{'✅' if state.get('show_buy_alert_wallet', True) else '🚫'} Wallet", "callback_data": "action:toggle_buy_alert_field:wallet"},
                    {"text": f"{'✅' if state.get('show_buy_alert_utya', True) else '🚫'} YODA", "callback_data": "action:toggle_buy_alert_field:utya"},
                ],
                [
                    {"text": f"{'✅' if state.get('show_buy_alert_usd', True) else '🚫'} USD", "callback_data": "action:toggle_buy_alert_field:usd"},
                    {"text": f"{'✅' if state.get('show_buy_alert_link', True) else '🚫'} Link", "callback_data": "action:toggle_buy_alert_field:link"},
                ],
                [{"text": "⬅️ Back", "callback_data": "menu:alerts"}],
            ]
        }
    if page == "sell_alert_display":
        return {
            "inline_keyboard": [
                [
                    {"text": f"{'✅' if state.get('show_sell_alert_wallet', True) else '🚫'} Wallet", "callback_data": "action:toggle_sell_alert_field:wallet"},
                    {"text": f"{'✅' if state.get('show_sell_alert_utya', True) else '🚫'} YODA", "callback_data": "action:toggle_sell_alert_field:utya"},
                ],
                [
                    {"text": f"{'✅' if state.get('show_sell_alert_usd', True) else '🚫'} USD", "callback_data": "action:toggle_sell_alert_field:usd"},
                    {"text": f"{'✅' if state.get('show_sell_alert_link', True) else '🚫'} Link", "callback_data": "action:toggle_sell_alert_field:link"},
                ],
                [{"text": "⬅️ Back", "callback_data": "menu:sell_alerts"}],
            ]
        }
    if page == "timing":
        current_minutes = max(1, runtime.interval_seconds // 60)
        return {
            "inline_keyboard": [
                [
                    {"text": f"{'✅ ' if current_minutes == 1 else ''}1m", "callback_data": "action:set_interval_minutes:1"},
                    {"text": f"{'✅ ' if current_minutes == 2 else ''}2m", "callback_data": "action:set_interval_minutes:2"},
                    {"text": f"{'✅ ' if current_minutes == 5 else ''}5m", "callback_data": "action:set_interval_minutes:5"},
                ],
                [
                    {"text": f"{'✅ ' if current_minutes == 10 else ''}10m", "callback_data": "action:set_interval_minutes:10"},
                    {"text": f"{'✅ ' if current_minutes == 15 else ''}15m", "callback_data": "action:set_interval_minutes:15"},
                    {"text": "✍️ Custom", "callback_data": "prompt:set_interval"},
                ],
                [{"text": "⬅️ Back", "callback_data": "menu:home"}],
            ]
        }
    if page == "format":
        return {
            "inline_keyboard": [
                [
                    {"text": f"{'✅ ' if runtime.decimal_places == 2 else ''}2 dp", "callback_data": "action:set_decimals:2"},
                    {"text": f"{'✅ ' if runtime.decimal_places == 4 else ''}4 dp", "callback_data": "action:set_decimals:4"},
                    {"text": f"{'✅ ' if runtime.decimal_places == 6 else ''}6 dp", "callback_data": "action:set_decimals:6"},
                ],
                [
                    {"text": f"{'✅ ' if runtime.decimal_places == 8 else ''}8 dp", "callback_data": "action:set_decimals:8"},
                    {"text": "✍️ Custom", "callback_data": "prompt:set_decimals"},
                ],
                [{"text": "⬅️ Back", "callback_data": "menu:home"}],
            ]
        }
    toggle_text = "⏸️ Pause" if runtime.posting_enabled else "▶️ Resume"
    return {
        "inline_keyboard": [
            [{"text": "📊 Dashboard", "callback_data": "menu:home"}, {"text": "💸 Post Now", "callback_data": "action:post_now"}],
            [{"text": toggle_text, "callback_data": "action:toggle_posting"}, {"text": "⏱️ Timing", "callback_data": "menu:timing"}],
            [{"text": "🔢 Decimals", "callback_data": "menu:format"}, {"text": "📈 Change Lines", "callback_data": "menu:display"}],
            [{"text": "📈 Buy Alerts", "callback_data": "menu:alerts"}, {"text": "📉 Sell Alerts", "callback_data": "menu:sell_alerts"}],
            [{"text": "📣 Target Channel", "callback_data": "prompt:set_channel"}],
            [{"text": "🕘 Activity", "callback_data": "menu:activity"}],
            [{"text": "🧾 Runtime Logs", "callback_data": "menu:logs"}],
            [{"text": "🔄 Refresh Panel", "callback_data": "menu:home"}],
        ]
    }


def build_menu_payload(state: dict[str, Any], runtime: RuntimeConfig, user_id: str, page: Optional[str] = None) -> tuple[str, dict[str, Any], str]:
    session = get_ui_session(state, user_id, create=True)
    active_page = str(page or session.get("menu_page") or "home").strip() or "home"
    if active_page not in {"home", "logs", "timing", "format", "display", "activity", "alerts", "alert_display", "sell_alerts", "sell_alert_display"}:
        active_page = "home"
    if active_page == "logs":
        text = build_logs_text(state, user_id)
    elif active_page == "activity":
        text = build_activity_text(state, user_id)
    elif active_page == "alerts":
        text = (
            "📈 <b>Buy Alert Settings</b>\n\n"
            "Post a whale alert when a YODA buy on the main pool crosses your USD threshold.\n\n"
            f"• <b>Status:</b> <code>{'ON' if state.get('buy_alerts_enabled', True) else 'OFF'}</code>\n"
            f"• <b>Threshold:</b> <code>{format_usd_value(float(state.get('buy_alert_threshold_usd', DEFAULT_ALERT_THRESHOLD_USD) or DEFAULT_ALERT_THRESHOLD_USD))}</code>\n"
            f"• <b>Channel:</b> <code>{get_buy_alert_channel(state, runtime)}</code>\n"
            f"• <b>Check every:</b> <code>{format_interval(int(state.get('buy_alert_interval_seconds', DEFAULT_ALERT_INTERVAL_SECONDS) or DEFAULT_ALERT_INTERVAL_SECONDS))}</code>\n"
            f"• <b>Message layout:</b> <code>{'CUSTOM' if str(state.get('buy_alert_template') or '').strip() else 'DEFAULT'}</code>"
        )
    elif active_page == "alert_display":
        text = (
            "🧩 <b>Buy Alert Display</b>\n\n"
            "Choose which fields appear in the default buy alert. A custom message layout controls its own fields.\n\n"
            f"• <b>Wallet:</b> <code>{'ON' if state.get('show_buy_alert_wallet', True) else 'OFF'}</code>\n"
            f"• <b>YODA amount:</b> <code>{'ON' if state.get('show_buy_alert_utya', True) else 'OFF'}</code>\n"
            f"• <b>USD amount:</b> <code>{'ON' if state.get('show_buy_alert_usd', True) else 'OFF'}</code>\n"
            f"• <b>Tonviewer link:</b> <code>{'ON' if state.get('show_buy_alert_link', True) else 'OFF'}</code>"
        )
    elif active_page == "sell_alerts":
        text = (
            "📉 <b>Sell Alert Settings</b>\n\n"
            "Post a whale alert when a YODA sell on the main pool crosses your USD threshold.\n\n"
            f"• <b>Status:</b> <code>{'ON' if state.get('sell_alerts_enabled', True) else 'OFF'}</code>\n"
            f"• <b>Threshold:</b> <code>{format_usd_value(float(state.get('sell_alert_threshold_usd', DEFAULT_ALERT_THRESHOLD_USD) or DEFAULT_ALERT_THRESHOLD_USD))}</code>\n"
            f"• <b>Channel:</b> <code>{get_sell_alert_channel(state, runtime)}</code>\n"
            f"• <b>Check every:</b> <code>{format_interval(int(state.get('sell_alert_interval_seconds', DEFAULT_ALERT_INTERVAL_SECONDS) or DEFAULT_ALERT_INTERVAL_SECONDS))}</code>\n"
            f"• <b>Message layout:</b> <code>{'CUSTOM' if str(state.get('sell_alert_template') or '').strip() else 'DEFAULT'}</code>"
        )
    elif active_page == "sell_alert_display":
        text = (
            "🧩 <b>Sell Alert Display</b>\n\n"
            "Choose which fields appear in the default sell alert. A custom message layout controls its own fields.\n\n"
            f"• <b>Wallet:</b> <code>{'ON' if state.get('show_sell_alert_wallet', True) else 'OFF'}</code>\n"
            f"• <b>YODA amount:</b> <code>{'ON' if state.get('show_sell_alert_utya', True) else 'OFF'}</code>\n"
            f"• <b>USD amount:</b> <code>{'ON' if state.get('show_sell_alert_usd', True) else 'OFF'}</code>\n"
            f"• <b>Tonviewer link:</b> <code>{'ON' if state.get('show_sell_alert_link', True) else 'OFF'}</code>"
        )
    else:
        text = build_menu_text(state, runtime, user_id)
    return text, build_menu_markup(state, runtime, active_page), active_page


def set_notice(state: dict[str, Any], user_id: str, message: str) -> None:
    session = get_ui_session(state, user_id, create=True)
    session["ui_notice"] = str(message or "").strip()
    session["ui_notice_at"] = utc_now().isoformat()


def clear_notice(state: dict[str, Any], user_id: str) -> None:
    session = get_ui_session(state, user_id, create=True)
    session["ui_notice"] = ""
    session["ui_notice_at"] = ""


def admin_label_from_user(user: dict[str, Any]) -> str:
    username = str(user.get("username") or "").strip()
    if username:
        return f"@{username}"
    first_name = str(user.get("first_name") or "").strip()
    user_id = str(user.get("id") or "").strip()
    if first_name and user_id:
        return f"{first_name} ({user_id})"
    return user_id or "unknown"


def add_activity_entry(state: dict[str, Any], actor_id: str, actor_label: str, action: str) -> None:
    entries = list(state.get("activity_log") or [])
    entries.append(
        {
            "at": utc_now().isoformat(),
            "actor_id": str(actor_id or "").strip(),
            "actor": str(actor_label or "").strip() or "unknown",
            "action": str(action or "").strip() or "-",
        }
    )
    state["activity_log"] = entries[-20:]


def notify_admin_change(
    runtime: RuntimeConfig,
    state: dict[str, Any],
    *,
    actor_id: str,
    actor_label: str,
    action: str,
) -> None:
    summary = f"{actor_label} {action}"
    add_activity_entry(state, actor_id, actor_label, action)
    for target_user_id in get_allowed_user_ids(state):
        set_notice(state, target_user_id, summary)
    save_state(state)

    # Update the admin who pressed the button before sending secondary notices.
    # A slow notification to another chat must not hold up the active menu.
    actor_session = get_ui_session(state, actor_id, create=True)
    if int(actor_session.get("menu_message_id", 0) or 0):
        upsert_menu_message(runtime_from_state(load_env_config(), state), state, actor_id)

    for target_user_id in get_allowed_user_ids(state):
        if target_user_id == actor_id:
            continue
        try:
            send_control_message(
                runtime,
                state,
                target_user_id,
                f"🔔 <b>Admin Update</b>\n<code>{escape_html(summary)}</code>",
            )
        except Exception as exc:
            log(f"Admin notify failed for {target_user_id}: {type(exc).__name__}: {exc}")


def is_message_not_modified_error(exc: Exception) -> bool:
    return "message is not modified" in str(exc).lower()


def extract_message_id(result: dict[str, Any]) -> int:
    payload = result.get("result")
    if isinstance(payload, dict):
        return int(payload.get("message_id", 0) or 0)
    return int(result.get("message_id", 0) or 0)


def upsert_menu_message(
    runtime: RuntimeConfig,
    state: dict[str, Any],
    user_id: str,
    *,
    page: Optional[str] = None,
    force_new: bool = False,
    allow_send_new: bool = False,
) -> None:
    session = get_ui_session(state, user_id, create=True)
    chat_id = str(session.get("chat_id") or "").strip()
    if not chat_id:
        return
    text, markup, active_page = build_menu_payload(state, runtime, user_id, page)
    session["menu_page"] = active_page
    existing_message_id = 0 if force_new else int(session.get("menu_message_id", 0) or 0)
    if existing_message_id:
        try:
            edit_message_text(runtime, chat_id, existing_message_id, text, parse_mode="HTML", reply_markup=markup)
            save_state(state)
            return
        except Exception as exc:
            if is_message_not_modified_error(exc):
                save_state(state)
                return
            log(f"Menu edit failed; not sending a replacement automatically: {type(exc).__name__}: {exc}")
            session["menu_message_id"] = 0
            save_state(state)
            if not (force_new or allow_send_new):
                return
    if not (force_new or allow_send_new):
        save_state(state)
        return
    result = send_message(runtime, chat_id, text, parse_mode="HTML", reply_markup=markup, thread_id=None)
    session["menu_message_id"] = extract_message_id(result)
    save_state(state)


def is_private_chat(message: dict[str, Any]) -> bool:
    return chat_type_from_message(message) == "private"


def ensure_control_binding(state: dict[str, Any], message: dict[str, Any], *, force: bool = False) -> bool:
    if not is_private_chat(message):
        return False
    chat_id = chat_id_from_message(message)
    user_id = user_id_from_message(message)
    current_user_id = str(state.get("control_user_id") or "").strip()
    allowed_ids = get_allowed_user_ids(state)
    changed = False
    if not current_user_id:
        state["control_chat_id"] = chat_id
        state["control_thread_id"] = ""
        state["control_user_id"] = user_id
        current_user_id = user_id
        changed = True
    is_allowed = (
        force
        or user_id == current_user_id
        or user_id in allowed_ids
    )
    if not is_allowed:
        return False
    if user_id not in allowed_ids:
        allowed_ids.append(user_id)
        state["allowed_user_ids"] = allowed_ids
        changed = True
    session = get_ui_session(state, user_id, create=True)
    if str(session.get("chat_id") or "").strip() != chat_id:
        session["chat_id"] = chat_id
        changed = True
    if user_id == current_user_id and str(state.get("control_chat_id") or "").strip() != chat_id:
        state["control_chat_id"] = chat_id
        changed = True
    return changed


def is_authorized_chat(state: dict[str, Any], message: dict[str, Any]) -> bool:
    if not is_private_chat(message):
        return False
    message_user_id = user_id_from_message(message)
    control_user_id = str(state.get("control_user_id") or "").strip()
    if not control_user_id:
        return True
    if message_user_id == control_user_id:
        return True
    if message_user_id in get_allowed_user_ids(state):
        return True
    return False


def send_control_message(runtime: RuntimeConfig, state: dict[str, Any], user_id: str, text: str, *, reply_markup: Optional[dict[str, Any]] = None, force_reply: bool = False) -> None:
    session = get_ui_session(state, user_id, create=True)
    chat_id = str(session.get("chat_id") or "").strip()
    if not chat_id:
        return
    markup = reply_markup
    if force_reply:
        markup = {"force_reply": True, "selective": True}
    send_message(runtime, chat_id, text, parse_mode="HTML", reply_markup=markup, thread_id=None)


def post_price(runtime: RuntimeConfig, state: dict[str, Any], *, reason: str) -> str:
    snapshot = fetch_utya_market_snapshot(runtime)
    text = format_price(snapshot.price_usd, runtime.decimal_places)
    state["last_change_1h"] = format_change_percent(snapshot.change_1h_percent)
    state["last_change_24h"] = format_change_percent(snapshot.change_24h_percent)
    state["last_change_7d"] = format_change_percent(snapshot.change_7d_percent)
    send_message(
        runtime,
        runtime.channel,
        build_channel_post_text(
            snapshot,
            runtime.decimal_places,
            show_change_1h=bool(state.get("show_change_1h", True)),
            show_change_24h=bool(state.get("show_change_24h", True)),
            show_change_7d=bool(state.get("show_change_7d", True)),
        ),
        parse_mode="HTML",
    )
    state["last_post_at"] = utc_now().isoformat()
    state["last_post_text"] = text
    state["last_price"] = text
    state["last_error"] = ""
    state["last_error_at"] = ""
    save_state(state)
    log(
        "Posted "
        f"{text} ({reason}) "
        f"[1h {state['last_change_1h']} | 24h {state['last_change_24h']} | 7d {state['last_change_7d']}]"
    )
    return text


def get_buy_alert_channel(state: dict[str, Any], runtime: RuntimeConfig) -> str:
    return str(state.get("buy_alert_channel") or runtime.channel).strip() or runtime.channel


def get_sell_alert_channel(state: dict[str, Any], runtime: RuntimeConfig) -> str:
    return str(state.get("sell_alert_channel") or runtime.channel).strip() or runtime.channel


def check_buy_alerts(
    runtime: RuntimeConfig,
    state: dict[str, Any],
    *,
    events: Optional[list[dict[str, Any]]] = None,
    snapshot: Optional[MarketSnapshot] = None,
) -> list[BuyAlertEvent]:
    if not bool(state.get("buy_alerts_enabled", True)):
        return []
    events = events if events is not None else fetch_pool_events(runtime, state, limit=DEFAULT_ALERT_FETCH_LIMIT)
    event_ids = dedupe_text_values(
        [str(item.get("event_id") or "").strip() for item in events if isinstance(item, dict) and str(item.get("event_id") or "").strip()],
        limit=300,
    )
    seen_ids = dedupe_text_values(state.get("buy_alert_seen_event_ids") or [], limit=300)
    seen_keys = dedupe_text_values(state.get("buy_alert_seen_keys") or [], limit=300)
    snapshot = snapshot or fetch_utya_market_snapshot(runtime)
    if not bool(state.get("buy_alert_bootstrapped", False)):
        alerts = parse_buy_alert_events(state, events, snapshot)
        alert_keys = dedupe_text_values([build_buy_alert_dedupe_key(item) for item in alerts], limit=300)
        state["buy_alert_seen_event_ids"] = dedupe_text_values(seen_ids + event_ids, limit=300)
        state["buy_alert_seen_keys"] = dedupe_text_values(seen_keys + alert_keys, limit=300)
        state["buy_alert_bootstrapped"] = True
        save_state(state)
        return []
    alerts = parse_buy_alert_events(state, events, snapshot)
    fresh_alerts = [item for item in alerts if item.event_id not in seen_ids and build_buy_alert_dedupe_key(item) not in seen_keys]
    state["buy_alert_seen_event_ids"] = dedupe_text_values(seen_ids + event_ids, limit=300)
    state["buy_alert_seen_keys"] = dedupe_text_values(seen_keys + [build_buy_alert_dedupe_key(item) for item in alerts], limit=300)
    if not fresh_alerts:
        save_state(state)
        return []
    target_chat = get_buy_alert_channel(state, runtime)
    for alert in fresh_alerts:
        alert = enrich_alert_wallet_dns(runtime, alert)
        send_message(runtime, target_chat, build_buy_alert_text(state, alert), parse_mode="HTML")
        state["last_buy_alert_at"] = utc_now().isoformat()
        state["last_buy_alert_text"] = format_usd_value(alert.usd_amount)
        state["last_buy_alert_wallet"] = alert.wallet_address
        log(f"Posted buy alert {format_usd_value(alert.usd_amount)} for {shorten_wallet(alert.wallet_address)}")
    save_state(state)
    return fresh_alerts


def check_sell_alerts(
    runtime: RuntimeConfig,
    state: dict[str, Any],
    *,
    events: Optional[list[dict[str, Any]]] = None,
    snapshot: Optional[MarketSnapshot] = None,
) -> list[SellAlertEvent]:
    if not bool(state.get("sell_alerts_enabled", True)):
        return []
    events = events if events is not None else fetch_pool_events(runtime, state, limit=DEFAULT_ALERT_FETCH_LIMIT)
    event_ids = dedupe_text_values(
        [str(item.get("event_id") or "").strip() for item in events if isinstance(item, dict) and str(item.get("event_id") or "").strip()],
        limit=300,
    )
    seen_ids = dedupe_text_values(state.get("sell_alert_seen_event_ids") or [], limit=300)
    seen_keys = dedupe_text_values(state.get("sell_alert_seen_keys") or [], limit=300)
    snapshot = snapshot or fetch_utya_market_snapshot(runtime)
    if not bool(state.get("sell_alert_bootstrapped", False)):
        alerts = parse_sell_alert_events(state, events, snapshot)
        alert_keys = dedupe_text_values([build_sell_alert_dedupe_key(item) for item in alerts], limit=300)
        state["sell_alert_seen_event_ids"] = dedupe_text_values(seen_ids + event_ids, limit=300)
        state["sell_alert_seen_keys"] = dedupe_text_values(seen_keys + alert_keys, limit=300)
        state["sell_alert_bootstrapped"] = True
        save_state(state)
        return []
    alerts = parse_sell_alert_events(state, events, snapshot)
    fresh_alerts = [item for item in alerts if item.event_id not in seen_ids and build_sell_alert_dedupe_key(item) not in seen_keys]
    state["sell_alert_seen_event_ids"] = dedupe_text_values(seen_ids + event_ids, limit=300)
    state["sell_alert_seen_keys"] = dedupe_text_values(seen_keys + [build_sell_alert_dedupe_key(item) for item in alerts], limit=300)
    if not fresh_alerts:
        save_state(state)
        return []
    target_chat = get_sell_alert_channel(state, runtime)
    for alert in fresh_alerts:
        alert = enrich_alert_wallet_dns(runtime, alert)
        send_message(runtime, target_chat, build_sell_alert_text(state, alert), parse_mode="HTML")
        state["last_sell_alert_at"] = utc_now().isoformat()
        state["last_sell_alert_text"] = format_usd_value(alert.usd_amount)
        state["last_sell_alert_wallet"] = alert.wallet_address
        log(f"Posted sell alert {format_usd_value(alert.usd_amount)} for {shorten_wallet(alert.wallet_address)}")
    save_state(state)
    return fresh_alerts


def handle_text_prompt(runtime: RuntimeConfig, state: dict[str, Any], message: dict[str, Any]) -> bool:
    user = message.get("from") or {}
    user_id = str(user.get("id", "")).strip()
    actor_label = admin_label_from_user(user)
    pending = get_pending_input(state, user_id)
    if not pending:
        return False
    if str(pending.get("chat_id") or "") != chat_id_from_message(message):
        return True
    pending_thread = str(pending.get("thread_id") or "").strip()
    current_thread = thread_id_from_message(message)
    if pending_thread != current_thread:
        return True

    text = str(message.get("text") or "").strip()
    if text.startswith("/") and text.lower() != "/cancel":
        # Menu commands must never be consumed as replies to an editor prompt.
        return False
    rich_text = rich_text_to_html(text, message.get("entities"))
    pop_pending_input(state, user_id)

    if text.lower() in {"cancel", "/cancel"}:
        set_notice(state, user_id, "Cancelled pending input.")
        save_state(state)
        upsert_menu_message(runtime, state, user_id, page="home")
        return True

    action = str(pending.get("action") or "").strip()
    target_page = "home"
    try:
        if action == "set_interval":
            interval_seconds = parse_interval_input(text)
            state["interval_seconds"] = interval_seconds
            notify_admin_change(
                runtime,
                state,
                actor_id=user_id,
                actor_label=actor_label,
                action=f"changed interval to {format_interval(interval_seconds)}.",
            )
            log(f"Interval changed to {interval_seconds}s via bot")
            return True
        if action == "set_channel":
            channel = normalize_channel_input(text)
            state["channel"] = channel
            notify_admin_change(
                runtime,
                state,
                actor_id=user_id,
                actor_label=actor_label,
                action=f"changed channel to {channel}.",
            )
            log(f"Channel changed to {channel} via bot")
            return True
        if action == "set_decimals":
            decimal_places = parse_decimal_places_input(text)
            state["decimal_places"] = decimal_places
            notify_admin_change(
                runtime,
                state,
                actor_id=user_id,
                actor_label=actor_label,
                action=f"changed decimals to {decimal_places}.",
            )
            log(f"Decimal places changed to {decimal_places} via bot")
            return True
        if action == "set_buy_alert_threshold":
            threshold_usd = parse_threshold_input(text)
            state["buy_alert_threshold_usd"] = threshold_usd
            target_page = "alerts"
            notify_admin_change(
                runtime,
                state,
                actor_id=user_id,
                actor_label=actor_label,
                action=f"changed buy-alert threshold to {format_usd_value(threshold_usd)}.",
            )
            log(f"Buy alert threshold changed to {threshold_usd:.2f} via bot")
            return True
        if action == "set_buy_alert_interval":
            interval_seconds = clamp_alert_interval(parse_interval_input(text))
            state["buy_alert_interval_seconds"] = interval_seconds
            target_page = "alerts"
            notify_admin_change(
                runtime,
                state,
                actor_id=user_id,
                actor_label=actor_label,
                action=f"changed buy-alert interval to {format_interval(interval_seconds)}.",
            )
            log(f"Buy alert interval changed to {interval_seconds}s via bot")
            return True
        if action == "set_buy_alert_channel":
            channel = normalize_channel_input(text)
            state["buy_alert_channel"] = channel
            target_page = "alerts"
            notify_admin_change(
                runtime,
                state,
                actor_id=user_id,
                actor_label=actor_label,
                action=f"changed buy-alert channel to {channel}.",
            )
            log(f"Buy alert channel changed to {channel} via bot")
            return True
        if action == "set_buy_alert_template":
            template = validate_alert_template(rich_text)
            preview = render_alert_template(template, sample_buy_alert_event(), side="BUY")
            send_message(
                runtime,
                chat_id_from_message(message),
                "👁 <b>Buy alert preview</b>\n\n" + preview,
                parse_mode="HTML",
                thread_id=None,
            )
            state["buy_alert_template"] = template
            target_page = "alerts"
            notify_admin_change(
                runtime,
                state,
                actor_id=user_id,
                actor_label=actor_label,
                action="updated the buy-alert message layout.",
            )
            log("Buy alert message template changed via bot")
            return True
        if action == "set_sell_alert_threshold":
            threshold_usd = parse_threshold_input(text)
            state["sell_alert_threshold_usd"] = threshold_usd
            target_page = "sell_alerts"
            notify_admin_change(
                runtime,
                state,
                actor_id=user_id,
                actor_label=actor_label,
                action=f"changed sell-alert threshold to {format_usd_value(threshold_usd)}.",
            )
            log(f"Sell alert threshold changed to {threshold_usd:.2f} via bot")
            return True
        if action == "set_sell_alert_interval":
            interval_seconds = clamp_alert_interval(parse_interval_input(text))
            state["sell_alert_interval_seconds"] = interval_seconds
            target_page = "sell_alerts"
            notify_admin_change(
                runtime,
                state,
                actor_id=user_id,
                actor_label=actor_label,
                action=f"changed sell-alert interval to {format_interval(interval_seconds)}.",
            )
            log(f"Sell alert interval changed to {interval_seconds}s via bot")
            return True
        if action == "set_sell_alert_channel":
            channel = normalize_channel_input(text)
            state["sell_alert_channel"] = channel
            target_page = "sell_alerts"
            notify_admin_change(
                runtime,
                state,
                actor_id=user_id,
                actor_label=actor_label,
                action=f"changed sell-alert channel to {channel}.",
            )
            log(f"Sell alert channel changed to {channel} via bot")
            return True
        if action == "set_sell_alert_template":
            template = validate_alert_template(rich_text)
            preview = render_alert_template(template, sample_sell_alert_event(), side="SELL")
            send_message(
                runtime,
                chat_id_from_message(message),
                "👁 <b>Sell alert preview</b>\n\n" + preview,
                parse_mode="HTML",
                thread_id=None,
            )
            state["sell_alert_template"] = template
            target_page = "sell_alerts"
            notify_admin_change(
                runtime,
                state,
                actor_id=user_id,
                actor_label=actor_label,
                action="updated the sell-alert message layout.",
            )
            log("Sell alert message template changed via bot")
            return True
        save_state(state)
        return False
    except Exception as exc:
        set_notice(state, user_id, f"Invalid value: {exc}")
        set_pending_input(
            state,
            user_id,
            action,
            chat_id=chat_id_from_message(message),
            thread_id=current_thread,
        )
        save_state(state)
        if action in {"set_buy_alert_threshold", "set_buy_alert_interval", "set_buy_alert_channel", "set_buy_alert_template"}:
            target_page = "alerts"
        if action in {"set_sell_alert_threshold", "set_sell_alert_interval", "set_sell_alert_channel", "set_sell_alert_template"}:
            target_page = "sell_alerts"
        upsert_menu_message(runtime, state, user_id, page=target_page)
        send_control_message(
            runtime,
            state,
            user_id,
            f"❌ <b>Invalid value</b>\n• <b>Error:</b> <code>{escape_html(str(exc))}</code>\nReply again or send <code>cancel</code>.",
            force_reply=True,
        )
        return True


def process_message(runtime: RuntimeConfig, state: dict[str, Any], message: dict[str, Any]) -> None:
    text = str(message.get("text") or "").strip()
    if not text:
        return

    if not is_private_chat(message):
        return

    user_id = user_id_from_message(message)
    bound = ensure_control_binding(state, message, force=False)
    if bound:
        set_notice(state, user_id, "Private control linked to this chat.")
        save_state(state)
        log(f"Bound control chat to {chat_id_from_message(message)} user {user_id}")

    if handle_text_prompt(runtime, state, message):
        return

    if not is_authorized_chat(state, message):
        return

    state["last_command_at"] = utc_now().isoformat()
    chat_id = chat_id_from_message(message)

    command = text.split()[0].lower()
    if command == "/menu":
        if not str(state.get("control_chat_id") or "").strip() or not str(state.get("control_user_id") or "").strip():
            ensure_control_binding(state, message, force=True)
            set_notice(state, user_id, "Private control linked to this chat.")
            save_state(state)
        upsert_menu_message(runtime, state, user_id, page="home", force_new=True)
        return
    if command == "/bind":
        ensure_control_binding(state, message, force=True)
        set_notice(state, user_id, "Private control locked here.")
        save_state(state)
        upsert_menu_message(runtime, state, user_id, page="home")
        return
    if command == "/status":
        upsert_menu_message(runtime, state, user_id, page="home")
        return
    if command == "/logs":
        upsert_menu_message(runtime, state, user_id, page="logs")
        return
    if command == "/activity":
        upsert_menu_message(runtime, state, user_id, page="activity")
        return
    if command == "/alerts":
        upsert_menu_message(runtime, state, user_id, page="alerts")
        return
    if command == "/pause":
        state["posting_enabled"] = False
        notify_admin_change(
            runtime,
            state,
            actor_id=user_id,
            actor_label=admin_label_from_user(message.get("from") or {}),
            action="paused posting.",
        )
        log("Posting paused via bot")
        return
    if command == "/resume":
        state["posting_enabled"] = True
        notify_admin_change(
            runtime,
            state,
            actor_id=user_id,
            actor_label=admin_label_from_user(message.get("from") or {}),
            action="resumed posting.",
        )
        log("Posting resumed via bot")
        return
    if command == "/post":
        try:
            posted = post_price(runtime, state, reason="manual")
            notify_admin_change(
                runtime,
                state,
                actor_id=user_id,
                actor_label=admin_label_from_user(message.get("from") or {}),
                action=f"posted manually: {posted}.",
            )
        except Exception as exc:
            state["last_error"] = f"{type(exc).__name__}: {exc}"
            state["last_error_at"] = utc_now().isoformat()
            set_notice(state, user_id, f"Post failed: {type(exc).__name__}")
            save_state(state)
            log(f"Manual post failed: {type(exc).__name__}: {exc}")
        upsert_menu_message(runtime, state, user_id, page="home")
        return
    if command == "/setinterval":
        set_pending_input(state, user_id, "set_interval", chat_id=chat_id, thread_id="")
        set_notice(state, user_id, "Waiting for interval reply in minutes.")
        save_state(state)
        upsert_menu_message(runtime, state, user_id, page="timing")
        send_message(runtime, chat_id, "⏱️ <b>Reply with the new interval</b>\nSend minutes like <code>1</code>, <code>5</code>, <code>15</code>.\nYou can also send <code>90s</code>, <code>2m</code>, <code>1h</code>.", parse_mode="HTML", reply_markup={"force_reply": True, "selective": True}, thread_id=None)
        return
    if command == "/setchannel":
        set_pending_input(state, user_id, "set_channel", chat_id=chat_id, thread_id="")
        set_notice(state, user_id, "Waiting for channel reply.")
        save_state(state)
        upsert_menu_message(runtime, state, user_id, page="home")
        send_message(runtime, chat_id, "📣 <b>Reply with the new channel</b>\nExamples: <code>@yodaprices</code> or <code>-100...</code>", parse_mode="HTML", reply_markup={"force_reply": True, "selective": True}, thread_id=None)
        return


def process_callback(runtime: RuntimeConfig, state: dict[str, Any], callback: dict[str, Any]) -> None:
    callback_id = str(callback.get("id") or "").strip()
    message = callback.get("message") or {}
    if not message:
        answer_callback_query(runtime, callback_id)
        return
    callback_user = callback.get("from") or {}
    auth_message = dict(message)
    auth_message["from"] = callback_user
    if not is_private_chat(auth_message):
        answer_callback_query(runtime, callback_id, "Use the bot in private chat.")
        return
    if not is_authorized_chat(state, auth_message):
        answer_callback_query(runtime, callback_id, "Not authorized.")
        return
    answer_callback_query(runtime, callback_id)

    data = str(callback.get("data") or "").strip()
    chat_id = chat_id_from_message(message)
    message_id = int(message.get("message_id"))
    user_id = str(callback_user.get("id", "")).strip()

    if data.startswith("menu:"):
        page = data.split(":", 1)[1].strip() or "home"
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        upsert_menu_message(runtime, state, user_id, page=page)
        return
    if data.startswith("action:set_interval_minutes:"):
        minutes_text = data.split(":", 2)[2].strip()
        minutes = max(1, int(minutes_text))
        state["interval_seconds"] = clamp_interval(minutes * 60)
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        notify_admin_change(
            runtime,
            state,
            actor_id=user_id,
            actor_label=admin_label_from_user(callback_user),
            action=f"changed interval to {minutes} minute(s).",
        )
        return
    if data == "action:toggle_buy_alerts":
        alerts_were_fully_disabled = not bool(state.get("buy_alerts_enabled", True)) and not bool(
            state.get("sell_alerts_enabled", True)
        )
        state["buy_alerts_enabled"] = not bool(state.get("buy_alerts_enabled", True))
        if state["buy_alerts_enabled"] and alerts_were_fully_disabled:
            state["ston_alert_cursor_block"] = 0
            state["dedust_alert_cursor_lt"] = 0
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        notify_admin_change(
            runtime,
            state,
            actor_id=user_id,
            actor_label=admin_label_from_user(callback_user),
            action="enabled buy alerts." if state["buy_alerts_enabled"] else "disabled buy alerts.",
        )
        log(f"Buy alerts {'enabled' if state['buy_alerts_enabled'] else 'disabled'} via menu")
        return
    if data == "action:toggle_sell_alerts":
        alerts_were_fully_disabled = not bool(state.get("buy_alerts_enabled", True)) and not bool(
            state.get("sell_alerts_enabled", True)
        )
        state["sell_alerts_enabled"] = not bool(state.get("sell_alerts_enabled", True))
        if state["sell_alerts_enabled"] and alerts_were_fully_disabled:
            state["ston_alert_cursor_block"] = 0
            state["dedust_alert_cursor_lt"] = 0
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        notify_admin_change(
            runtime,
            state,
            actor_id=user_id,
            actor_label=admin_label_from_user(callback_user),
            action="enabled sell alerts." if state["sell_alerts_enabled"] else "disabled sell alerts.",
        )
        log(f"Sell alerts {'enabled' if state['sell_alerts_enabled'] else 'disabled'} via menu")
        return
    if data == "action:preview_buy_alert_template":
        send_message(runtime, chat_id, alert_template_preview_text(state, side="BUY"), parse_mode="HTML", thread_id=None)
        return
    if data == "action:preview_sell_alert_template":
        send_message(runtime, chat_id, alert_template_preview_text(state, side="SELL"), parse_mode="HTML", thread_id=None)
        return
    if data == "action:reset_buy_alert_template":
        state["buy_alert_template"] = ""
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        notify_admin_change(
            runtime,
            state,
            actor_id=user_id,
            actor_label=admin_label_from_user(callback_user),
            action="restored the default buy-alert message layout.",
        )
        log("Buy alert message template reset via menu")
        return
    if data == "action:reset_sell_alert_template":
        state["sell_alert_template"] = ""
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        notify_admin_change(
            runtime,
            state,
            actor_id=user_id,
            actor_label=admin_label_from_user(callback_user),
            action="restored the default sell-alert message layout.",
        )
        log("Sell alert message template reset via menu")
        return
    if data.startswith("action:set_buy_alert_interval:"):
        seconds_text = data.rsplit(":", 1)[1].strip()
        seconds = clamp_alert_interval(int(seconds_text))
        state["buy_alert_interval_seconds"] = seconds
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        notify_admin_change(
            runtime,
            state,
            actor_id=user_id,
            actor_label=admin_label_from_user(callback_user),
            action=f"changed buy-alert interval to {format_interval(seconds)}.",
        )
        log(f"Buy alert interval changed to {seconds}s via menu")
        return
    if data.startswith("action:set_sell_alert_interval:"):
        seconds_text = data.rsplit(":", 1)[1].strip()
        seconds = clamp_alert_interval(int(seconds_text))
        state["sell_alert_interval_seconds"] = seconds
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        notify_admin_change(
            runtime,
            state,
            actor_id=user_id,
            actor_label=admin_label_from_user(callback_user),
            action=f"changed sell-alert interval to {format_interval(seconds)}.",
        )
        log(f"Sell alert interval changed to {seconds}s via menu")
        return
    if data.startswith("action:toggle_buy_alert_field:"):
        field = data.rsplit(":", 1)[1].strip()
        field_map = {
            "wallet": ("show_buy_alert_wallet", "wallet"),
            "utya": ("show_buy_alert_utya", "YODA amount"),
            "usd": ("show_buy_alert_usd", "USD amount"),
            "link": ("show_buy_alert_link", "Tonviewer link"),
        }
        item = field_map.get(field)
        if not item:
            return
        state_key, label = item
        state[state_key] = not bool(state.get(state_key, True))
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        notify_admin_change(
            runtime,
            state,
            actor_id=user_id,
            actor_label=admin_label_from_user(callback_user),
            action=f"turned buy-alert {label} {'on' if state[state_key] else 'off'}.",
        )
        log(f"Buy alert field {field} toggled to {state[state_key]} via menu")
        return
    if data.startswith("action:toggle_sell_alert_field:"):
        field = data.rsplit(":", 1)[1].strip()
        field_map = {
            "wallet": ("show_sell_alert_wallet", "wallet"),
            "utya": ("show_sell_alert_utya", "YODA amount"),
            "usd": ("show_sell_alert_usd", "USD amount"),
            "link": ("show_sell_alert_link", "Tonviewer link"),
        }
        item = field_map.get(field)
        if not item:
            return
        state_key, label = item
        state[state_key] = not bool(state.get(state_key, True))
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        notify_admin_change(
            runtime,
            state,
            actor_id=user_id,
            actor_label=admin_label_from_user(callback_user),
            action=f"turned sell-alert {label} {'on' if state[state_key] else 'off'}.",
        )
        log(f"Sell alert field {field} toggled to {state[state_key]} via menu")
        return
    if data.startswith("action:set_decimals:"):
        decimals_text = data.split(":", 2)[2].strip()
        decimals = parse_decimal_places_input(decimals_text)
        state["decimal_places"] = decimals
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        notify_admin_change(
            runtime,
            state,
            actor_id=user_id,
            actor_label=admin_label_from_user(callback_user),
            action=f"changed decimals to {decimals}.",
        )
        return
    if data.startswith("action:toggle_change:"):
        change_key = data.split(":", 2)[2].strip()
        key_map = {
            "1h": ("show_change_1h", "± 1h"),
            "24h": ("show_change_24h", "24h"),
            "7d": ("show_change_7d", "7d"),
        }
        state_key, label = key_map[change_key]
        state[state_key] = not bool(state.get(state_key, True))
        state_value = "on" if state[state_key] else "off"
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        notify_admin_change(
            runtime,
            state,
            actor_id=user_id,
            actor_label=admin_label_from_user(callback_user),
            action=f"turned {label} display {state_value}.",
        )
        return
    if data == "action:toggle_posting":
        state["posting_enabled"] = not bool(state.get("posting_enabled", True))
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        notify_admin_change(
            runtime,
            state,
            actor_id=user_id,
            actor_label=admin_label_from_user(callback_user),
            action="resumed posting." if state["posting_enabled"] else "paused posting.",
        )
        runtime_now = runtime_from_state(load_env_config(), state)
        log(f"Posting {'resumed' if runtime_now.posting_enabled else 'paused'} via menu")
        return
    if data == "action:post_now":
        try:
            posted = post_price(runtime, state, reason="manual")
            session = get_ui_session(state, user_id, create=True)
            session["menu_message_id"] = message_id
            notify_admin_change(
                runtime,
                state,
                actor_id=user_id,
                actor_label=admin_label_from_user(callback_user),
                action=f"posted manually: {posted}.",
            )
        except Exception as exc:
            state["last_error"] = f"{type(exc).__name__}: {exc}"
            state["last_error_at"] = utc_now().isoformat()
            set_notice(state, user_id, f"Post failed: {type(exc).__name__}")
            save_state(state)
            log(f"Manual post failed: {type(exc).__name__}: {exc}")
        runtime_now = runtime_from_state(load_env_config(), state)
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        upsert_menu_message(runtime_now, state, user_id, page="home")
        return
    if data == "prompt:set_interval":
        set_pending_input(state, user_id, "set_interval", chat_id=chat_id, thread_id="")
        set_notice(state, user_id, "Waiting for interval reply in minutes.")
        save_state(state)
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        upsert_menu_message(runtime, state, user_id, page="timing")
        send_message(runtime, chat_id, "⏱️ <b>Reply with the new interval</b>\nSend minutes like <code>1</code>, <code>5</code>, <code>15</code>.\nYou can also send <code>90s</code>, <code>2m</code>, <code>1h</code>.", parse_mode="HTML", reply_markup={"force_reply": True, "selective": True}, thread_id=None)
        return
    if data == "prompt:set_channel":
        set_pending_input(state, user_id, "set_channel", chat_id=chat_id, thread_id="")
        set_notice(state, user_id, "Waiting for channel reply.")
        save_state(state)
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        upsert_menu_message(runtime, state, user_id, page="home")
        send_message(runtime, chat_id, "📣 <b>Reply with the new channel</b>\nExamples: <code>@yodaprices</code> or <code>-100...</code>", parse_mode="HTML", reply_markup={"force_reply": True, "selective": True}, thread_id=None)
        return
    if data == "prompt:set_buy_alert_threshold":
        set_pending_input(state, user_id, "set_buy_alert_threshold", chat_id=chat_id, thread_id="")
        set_notice(state, user_id, "Waiting for buy-alert threshold reply.")
        save_state(state)
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        upsert_menu_message(runtime, state, user_id, page="alerts")
        send_message(runtime, chat_id, "📈 <b>Reply with the buy-alert threshold in USD</b>\nExamples: <code>5000</code>, <code>7500</code>, <code>12500.50</code>.", parse_mode="HTML", reply_markup={"force_reply": True, "selective": True}, thread_id=None)
        return
    if data == "prompt:set_buy_alert_interval":
        set_pending_input(state, user_id, "set_buy_alert_interval", chat_id=chat_id, thread_id="")
        set_notice(state, user_id, "Waiting for buy-alert interval reply.")
        save_state(state)
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        upsert_menu_message(runtime, state, user_id, page="alerts")
        send_message(runtime, chat_id, "⏱️ <b>Reply with the buy-alert check interval</b>\nExamples: <code>10s</code>, <code>30s</code>, <code>1m</code>, <code>2m</code>.", parse_mode="HTML", reply_markup={"force_reply": True, "selective": True}, thread_id=None)
        return
    if data == "prompt:set_buy_alert_channel":
        set_pending_input(state, user_id, "set_buy_alert_channel", chat_id=chat_id, thread_id="")
        set_notice(state, user_id, "Waiting for buy-alert channel reply.")
        save_state(state)
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        upsert_menu_message(runtime, state, user_id, page="alerts")
        send_message(runtime, chat_id, "📣 <b>Reply with the buy-alert channel</b>\nExamples: <code>@yodaprices</code> or <code>-100...</code>.", parse_mode="HTML", reply_markup={"force_reply": True, "selective": True}, thread_id=None)
        return
    if data == "prompt:set_buy_alert_template":
        set_pending_input(state, user_id, "set_buy_alert_template", chat_id=chat_id, thread_id="")
        set_notice(state, user_id, "Waiting for the complete buy-alert layout.")
        save_state(state)
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        upsert_menu_message(runtime, state, user_id, page="alerts")
        send_message(
            runtime,
            chat_id,
            alert_template_prompt_text(state, side="BUY"),
            parse_mode="HTML",
            reply_markup={"force_reply": True, "selective": True},
            thread_id=None,
        )
        return
    if data == "prompt:set_sell_alert_threshold":
        set_pending_input(state, user_id, "set_sell_alert_threshold", chat_id=chat_id, thread_id="")
        set_notice(state, user_id, "Waiting for sell-alert threshold reply.")
        save_state(state)
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        upsert_menu_message(runtime, state, user_id, page="sell_alerts")
        send_message(runtime, chat_id, "📉 <b>Reply with the sell-alert threshold in USD</b>\nExamples: <code>5000</code>, <code>7500</code>, <code>12500.50</code>.", parse_mode="HTML", reply_markup={"force_reply": True, "selective": True}, thread_id=None)
        return
    if data == "prompt:set_sell_alert_interval":
        set_pending_input(state, user_id, "set_sell_alert_interval", chat_id=chat_id, thread_id="")
        set_notice(state, user_id, "Waiting for sell-alert interval reply.")
        save_state(state)
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        upsert_menu_message(runtime, state, user_id, page="sell_alerts")
        send_message(runtime, chat_id, "⏱️ <b>Reply with the sell-alert check interval</b>\nExamples: <code>10s</code>, <code>30s</code>, <code>1m</code>, <code>2m</code>.", parse_mode="HTML", reply_markup={"force_reply": True, "selective": True}, thread_id=None)
        return
    if data == "prompt:set_sell_alert_channel":
        set_pending_input(state, user_id, "set_sell_alert_channel", chat_id=chat_id, thread_id="")
        set_notice(state, user_id, "Waiting for sell-alert channel reply.")
        save_state(state)
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        upsert_menu_message(runtime, state, user_id, page="sell_alerts")
        send_message(runtime, chat_id, "📣 <b>Reply with the sell-alert channel</b>\nExamples: <code>@yodaprices</code> or <code>-100...</code>.", parse_mode="HTML", reply_markup={"force_reply": True, "selective": True}, thread_id=None)
        return
    if data == "prompt:set_sell_alert_template":
        set_pending_input(state, user_id, "set_sell_alert_template", chat_id=chat_id, thread_id="")
        set_notice(state, user_id, "Waiting for the complete sell-alert layout.")
        save_state(state)
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        upsert_menu_message(runtime, state, user_id, page="sell_alerts")
        send_message(
            runtime,
            chat_id,
            alert_template_prompt_text(state, side="SELL"),
            parse_mode="HTML",
            reply_markup={"force_reply": True, "selective": True},
            thread_id=None,
        )
        return
    if data == "prompt:set_decimals":
        set_pending_input(state, user_id, "set_decimals", chat_id=chat_id, thread_id="")
        set_notice(state, user_id, "Waiting for decimal places reply.")
        save_state(state)
        session = get_ui_session(state, user_id, create=True)
        session["menu_message_id"] = message_id
        upsert_menu_message(runtime, state, user_id, page="format")
        send_message(runtime, chat_id, "🔢 <b>Reply with decimal places</b>\nExamples: <code>2</code>, <code>4</code>, <code>6</code>, <code>8</code>.", parse_mode="HTML", reply_markup={"force_reply": True, "selective": True}, thread_id=None)
        return


def process_updates(runtime: RuntimeConfig, state: dict[str, Any]) -> None:
    offset = int(state.get("last_update_id", 0) or 0)
    timeout_seconds = max(1, min(CONTROL_POLL_TIMEOUT_SECONDS, runtime.command_timeout_seconds))
    updates = get_updates(runtime, offset, timeout_seconds)
    if not updates:
        return
    for update in updates:
        update_id = int(update.get("update_id", offset))
        offset = max(offset, update_id)
        callback = update.get("callback_query")
        if isinstance(callback, dict):
            callback_started_at = time.monotonic()
            try:
                process_callback(runtime, state, callback)
            except Exception as exc:
                state["last_error"] = f"{type(exc).__name__}: {exc}"
                state["last_error_at"] = utc_now().isoformat()
                log(f"Callback error: {type(exc).__name__}: {exc}")
            finally:
                callback_elapsed = time.monotonic() - callback_started_at
                if callback_elapsed >= 2:
                    callback_data = str(callback.get("data") or "-").strip() or "-"
                    log(f"Slow callback {callback_data}: {callback_elapsed:.2f}s")
        message = update.get("message")
        if isinstance(message, dict):
            try:
                process_message(runtime, state, message)
            except Exception as exc:
                state["last_error"] = f"{type(exc).__name__}: {exc}"
                state["last_error_at"] = utc_now().isoformat()
                log(f"Message error: {type(exc).__name__}: {exc}")
    state["last_update_id"] = offset
    save_state(state)


def process_updates_safely(runtime: RuntimeConfig, state: dict[str, Any]) -> None:
    try:
        process_updates(runtime, state)
    except Exception as exc:
        state["last_error"] = f"{type(exc).__name__}: {exc}"
        state["last_error_at"] = utc_now().isoformat()
        save_state(state)
        log(f"Update loop error: {type(exc).__name__}: {exc}")
        time.sleep(min(runtime.retry_seconds, 2))


def handle_stop(signum: int, _frame) -> None:
    global _STOP
    _STOP = True
    log(f"Received signal {signum}; shutting down.")


def install_signal_handlers() -> None:
    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)


def run() -> int:
    install_signal_handlers()
    env = load_env_config()
    state = load_state(env)
    runtime = runtime_from_state(env, state)
    save_state(state)
    set_my_commands(runtime)
    log(f"Starting YODA price bot -> {runtime.channel} every {runtime.interval_seconds}s")

    next_post_at = time.monotonic()
    next_buy_alert_check_at = time.monotonic()
    next_sell_alert_check_at = time.monotonic()

    while not _STOP:
        env = load_env_config()
        state = load_state(env)
        runtime = runtime_from_state(env, state)

        # Control-plane work comes first so market APIs cannot starve buttons.
        process_updates_safely(runtime, state)
        runtime = runtime_from_state(env, state)

        now_monotonic = time.monotonic()
        if runtime.posting_enabled and now_monotonic >= next_post_at:
            try:
                post_price(runtime, state, reason="scheduled")
            except Exception as exc:
                state["last_error"] = f"{type(exc).__name__}: {exc}"
                state["last_error_at"] = utc_now().isoformat()
                for allowed_user_id in get_allowed_user_ids(state):
                    set_notice(state, allowed_user_id, f"Scheduled post failed: {type(exc).__name__}")
                save_state(state)
                log(f"Scheduled post failed: {type(exc).__name__}: {exc}")
                backoff_seconds = rate_limit_post_backoff_seconds(runtime) if is_http_too_many_requests(exc) else runtime.retry_seconds
                next_post_at = now_monotonic + backoff_seconds
            else:
                next_post_at = now_monotonic + runtime.interval_seconds
        elif not runtime.posting_enabled:
            next_post_at = now_monotonic + 2

        buy_enabled = bool(state.get("buy_alerts_enabled", True))
        sell_enabled = bool(state.get("sell_alerts_enabled", True))
        buy_due = buy_enabled and now_monotonic >= next_buy_alert_check_at
        sell_due = sell_enabled and now_monotonic >= next_sell_alert_check_at
        if buy_due or sell_due:
            try:
                queued, delivered = sync_reliable_alerts(runtime, state)
                if queued or delivered:
                    log(f"Reliable alert sync queued {queued} and delivered {delivered} alert(s)")
            except Exception as exc:
                state["last_error"] = f"{type(exc).__name__}: {exc}"
                state["last_error_at"] = utc_now().isoformat()
                if DEFAULT_ALERT_DEX == "dedust":
                    state["dedust_alert_last_error"] = state["last_error"]
                else:
                    state["ston_alert_last_error"] = state["last_error"]
                save_state(state)
                log(f"Reliable {DEFAULT_ALERT_DEX} alert sync failed: {type(exc).__name__}: {exc}")
                # The durable cursor is intentionally unchanged. The next successful
                # check resumes from the exact point that was not processed.
                retry_at = now_monotonic + max(10, int(runtime.retry_seconds))
                if buy_enabled:
                    next_buy_alert_check_at = retry_at
                if sell_enabled:
                    next_sell_alert_check_at = retry_at
            else:
                if buy_enabled:
                    next_buy_alert_check_at = now_monotonic + int(
                        state.get("buy_alert_interval_seconds", DEFAULT_ALERT_INTERVAL_SECONDS)
                        or DEFAULT_ALERT_INTERVAL_SECONDS
                    )
                if sell_enabled:
                    next_sell_alert_check_at = now_monotonic + int(
                        state.get("sell_alert_interval_seconds", DEFAULT_ALERT_INTERVAL_SECONDS)
                        or DEFAULT_ALERT_INTERVAL_SECONDS
                    )
        if not buy_enabled:
            next_buy_alert_check_at = now_monotonic + 2
        if not sell_enabled:
            next_sell_alert_check_at = now_monotonic + 2

        time.sleep(0.2)

    log("Stopped.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(run())
    except RuntimeError as exc:
        log(f"Startup failed: {exc}")
        raise SystemExit(1)
