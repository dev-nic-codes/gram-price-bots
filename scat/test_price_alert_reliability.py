from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import main


POOL = main.DEFAULT_ALERT_POOL_ADDRESS


def alert_state(*, threshold: float = 1_000) -> dict:
    return {
        "buy_alerts_enabled": True,
        "sell_alerts_enabled": True,
        "buy_alert_threshold_usd": threshold,
        "sell_alert_threshold_usd": threshold,
        "buy_alert_pool_address": POOL,
        "sell_alert_pool_address": POOL,
        "buy_alert_pool_label": main.DEFAULT_ALERT_POOL_LABEL,
        "sell_alert_pool_label": main.DEFAULT_ALERT_POOL_LABEL,
        "buy_alert_template": "",
        "sell_alert_template": "",
        "show_buy_alert_usd": True,
        "show_buy_alert_utya": True,
        "show_buy_alert_wallet": True,
        "show_buy_alert_link": True,
        "show_sell_alert_usd": True,
        "show_sell_alert_utya": True,
        "show_sell_alert_wallet": True,
        "show_sell_alert_link": True,
        "buy_alert_seen_event_ids": [],
        "buy_alert_seen_keys": [],
        "sell_alert_seen_event_ids": [],
        "sell_alert_seen_keys": [],
        "ston_alert_cursor_block": 100,
        "dedust_alert_cursor_lt": 100,
        "ston_alert_outbox": [],
    }


def ston_swap(*, transaction: str, block: int, buy: bool) -> dict:
    event = {
        "block": {"blockNumber": block, "blockTimestamp": 1_784_626_000 + block},
        "eventType": "swap",
        "txnId": transaction,
        "txnIndex": block * 100,
        "eventIndex": 1,
        "maker": f"wallet-{transaction}",
        "pairId": POOL,
    }
    if buy:
        event.update({"amount1In": "1000", "amount0Out": "60000"})
    else:
        event.update({"amount0In": "50000", "amount1Out": "800"})
    return event


def dedust_trade(*, lt: int, buy: bool) -> dict:
    native = {"type": "native"}
    token = {"type": "jetton", "address": main.SCAT_FRIENDLY_MASTER_ADDRESS}
    if buy:
        asset_in, asset_out = native, token
        amount_in, amount_out = str(1000 * 10**9), str(60000 * 10**main.TOKEN_DECIMALS)
    else:
        asset_in, asset_out = token, native
        amount_in, amount_out = str(50000 * 10**main.TOKEN_DECIMALS), str(800 * 10**9)
    return {
        "sender": f"wallet-{lt}",
        "assetIn": asset_in,
        "assetOut": asset_out,
        "amountIn": amount_in,
        "amountOut": amount_out,
        "lt": str(lt),
        "createdAt": "2026-07-21T09:00:00.000Z",
    }


class FakeResponse:
    def __init__(self, payload: object) -> None:
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self) -> bytes:
        return json.dumps(self.payload).encode()


class PriceSourceTests(unittest.TestCase):
    def test_primary_pool_price_and_native_gram_rate_are_validated(self) -> None:
        payload = {
            "pairs": [{
                "pairAddress": POOL,
                "baseToken": {"address": main.SCAT_FRIENDLY_MASTER_ADDRESS},
                "priceUsd": "0.02",
                "priceNative": "0.01",
                "priceChange": {"h1": 1.2, "h24": -3.4},
            }]
        }
        runtime = main.RuntimeConfig("token", "@channel", 60, 4, True, "test", 20, 15, 20)
        with patch.object(main.urllib.request, "urlopen", return_value=FakeResponse(payload)), patch.object(
            main, "load_market_snapshot_cache", side_effect=FileNotFoundError
        ), patch.object(main, "load_shared_gram_price_cache", return_value=None):
            snapshot = main.fetch_dexscreener_market_snapshot_live(runtime)
        self.assertEqual(snapshot.price_usd, 0.02)
        self.assertEqual(snapshot.gram_price_usd, 2.0)
        self.assertEqual(snapshot.change_1h_percent, 1.2)
        self.assertEqual(snapshot.change_24h_percent, -3.4)


class StonTests(unittest.TestCase):
    def test_ston_classifies_buy_and_sell(self) -> None:
        snapshot = main.MarketSnapshot(0.02, None, None, None, gram_price_usd=1.5)
        parsed = main.parse_ston_alert_events(
            alert_state(),
            [ston_swap(transaction="buy", block=101, buy=True), ston_swap(transaction="sell", block=102, buy=False)],
            snapshot,
            {POOL: (0, True)},
        )
        self.assertEqual([side for side, _event in parsed], ["BUY", "SELL"])
        self.assertEqual(parsed[0][1].usd_amount, 1500)
        self.assertEqual(parsed[1][1].usd_amount, 1200)


class DedustTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runtime = main.RuntimeConfig("token", "@channel", 60, 4, True, "test", 20, 15, 20)
        self.snapshot = main.MarketSnapshot(0.02, None, None, None, gram_price_usd=1.5)

    def test_dedust_classifies_raw_buy_and_sell_amounts(self) -> None:
        parsed = main.parse_dedust_alert_events(
            alert_state(),
            [dedust_trade(lt=101, buy=True), dedust_trade(lt=102, buy=False)],
            self.snapshot,
            POOL,
        )
        self.assertEqual([side for side, _event in parsed], ["BUY", "SELL"])
        self.assertEqual(parsed[0][1].gram_amount, 1000)
        self.assertEqual(parsed[0][1].utya_amount, 60000)
        self.assertEqual(parsed[1][1].usd_amount, 1200)

    def test_dedust_cursor_advances_and_alert_is_sent_once(self) -> None:
        state = alert_state()
        with tempfile.TemporaryDirectory() as directory, patch.object(
            main, "STATE_PATH", Path(directory) / "state.json"
        ), patch.object(
            main, "load_alert_market_snapshot", return_value=self.snapshot
        ), patch.object(
            main, "fetch_dedust_trades", side_effect=[[dedust_trade(lt=101, buy=True)], []]
        ) as fetch_trades, patch.object(
            main, "enrich_alert_wallet_dns", side_effect=lambda _runtime, alert: alert
        ), patch.object(main, "send_message") as send_message:
            first = main.sync_dedust_alerts(self.runtime, state)
            second = main.sync_dedust_alerts(self.runtime, state)
        self.assertEqual(first, (1, 1))
        self.assertEqual(second, (0, 0))
        self.assertEqual(state["dedust_alert_cursor_lt"], 101)
        self.assertEqual(send_message.call_count, 1)
        self.assertEqual(fetch_trades.call_count, 2)


class DeliveryTests(unittest.TestCase):
    def test_failed_telegram_send_keeps_alert_in_outbox(self) -> None:
        state = alert_state()
        event = main.BuyAlertEvent(
            "event", "wallet", 60000, 1500, "pool", POOL, "tx", 123, "-", 1000
        )
        state["ston_alert_outbox"] = [main.serialize_alert_outbox_item("BUY", event)]
        runtime = main.RuntimeConfig("token", "@channel", 60, 4, True, "test", 20, 15, 20)
        with tempfile.TemporaryDirectory() as directory, patch.object(
            main, "STATE_PATH", Path(directory) / "state.json"
        ), patch.object(
            main, "enrich_alert_wallet_dns", side_effect=lambda _runtime, alert: alert
        ), patch.object(main, "send_message", side_effect=RuntimeError("temporary failure")):
            with self.assertRaisesRegex(RuntimeError, "temporary failure"):
                main.deliver_alert_outbox(runtime, state)
        self.assertEqual(len(state["ston_alert_outbox"]), 1)
        self.assertEqual(state["buy_alert_seen_event_ids"], [])


class AlertTemplateTests(unittest.TestCase):
    def test_default_buy_and_sell_alerts_use_compact_trade_layout(self) -> None:
        wallet = "EQ" + ("A" * 40) + "BCDEFGHI"
        event_args = (
            "event",
            wallet,
            85_000,
            3_965.25,
            "pool",
            POOL,
            "tx",
            1_784_626_000,
            "-",
            2_640,
        )
        buy_text = main.build_buy_alert_text(alert_state(), main.BuyAlertEvent(*event_args))
        sell_text = main.build_sell_alert_text(alert_state(), main.SellAlertEvent(*event_args))

        self.assertIn("🟢 <b>BUY</b> • <b>$3,965.25</b>", buy_text)
        self.assertIn(f"💰 <b>85,000.00 {main.TOKEN_TICKER}</b>", buy_text)
        self.assertIn("⚖️ <b>2,640.00 GRAM</b>", buy_text)
        self.assertIn("📈 Price: <b>$0.04665</b>", buy_text)
        self.assertIn("👛 <code>EQAAAAAAAA...BCDEFGHI</code>", buy_text)
        self.assertIn(">Open wallet</a>", buy_text)
        self.assertIn("⏰ 09:26 UTC", buy_text)
        self.assertIn("🔴 <b>SELL</b> • <b>$3,965.25</b>", sell_text)
        self.assertIn("📉 Price: <b>$0.04665</b>", sell_text)


class PromptTests(unittest.TestCase):
    def test_menu_command_is_not_consumed_by_template_editor(self) -> None:
        state = {"pending_inputs": {}}
        main.set_pending_input(state, "123", "set_buy_alert_template", chat_id="123")
        message = {"from": {"id": 123}, "chat": {"id": 123, "type": "private"}, "text": "/menu"}
        self.assertFalse(main.handle_text_prompt(object(), state, message))
        self.assertIsNotNone(main.get_pending_input(state, "123"))


class MenuTests(unittest.TestCase):
    def test_menu_exposes_complete_buy_and_sell_controls(self) -> None:
        env = main.EnvConfig("token", "@channel", 60, "test", 20, 15, 20)
        state = main.default_state(env)
        runtime = main.runtime_from_state(env, state)
        callbacks: set[str] = set()
        for page in ("alerts", "sell_alerts"):
            text, markup, active_page = main.build_menu_payload(state, runtime, "123", page)
            self.assertEqual(active_page, page)
            self.assertIn(main.TOKEN_TICKER, text)
            for row in markup.get("inline_keyboard", []):
                for button in row:
                    callback = str(button.get("callback_data") or "")
                    if callback:
                        callbacks.add(callback)
        required = {
            "action:toggle_buy_alerts",
            "prompt:set_buy_alert_threshold",
            "prompt:set_buy_alert_interval",
            "prompt:set_buy_alert_channel",
            "prompt:set_buy_alert_template",
            "action:toggle_sell_alerts",
            "prompt:set_sell_alert_threshold",
            "prompt:set_sell_alert_interval",
            "prompt:set_sell_alert_channel",
            "prompt:set_sell_alert_template",
        }
        self.assertTrue(required.issubset(callbacks), required - callbacks)


if __name__ == "__main__":
    unittest.main()
