# Protocol references

Reviewed on 2026-07-22. Exchange documentation is the source of truth; recheck it whenever a feed stops parsing or before a major deployment upgrade.

## Binance USDⓈ-M Futures

- WebSocket market-stream overview and routed endpoints: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams
- Aggregate trade stream: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams
- Liquidation order stream: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams

The project uses the current routed market endpoint and combined streams. Binance force-order events are snapshots, so liquidation totals can be lower than the true market-wide amount during dense activity.

## Bybit V5

- Public WebSocket topics: https://bybit-exchange.github.io/docs/v5/ws/connect
- Public trades: https://bybit-exchange.github.io/docs/v5/websocket/public/trade
- All liquidations: https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation

The linear public endpoint is used. Bybit documents `Buy` in the liquidation payload as a liquidated long position and `Sell` as a liquidated short position.

## OKX V5

- API/WebSocket documentation: https://www.okx.com/docs-v5/en/
- Public instruments REST endpoint: https://www.okx.com/docs-v5/en/#public-data-rest-api-get-instruments
- Trades WebSocket channel: https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-trades-channel

Swap trade size is converted through current instrument `ctVal × ctMult`. If metadata cannot be loaded, the adapter logs a warning; relative volume shape may remain useful, but absolute quote-notional filters can be wrong.

## Notifications

- ntfy publish API: https://docs.ntfy.sh/publish/
- ntfy web app: https://docs.ntfy.sh/subscribe/web/
- Telegram Bot API: https://core.telegram.org/bots/api

## Maintenance checklist

1. Run `crypto-sentinel --config config.yaml doctor`.
2. Compare current payload examples with parser fixtures in `tests/test_parsers.py`.
3. Update endpoint URLs and fixtures together.
4. Run the complete test suite and offline simulation.
5. Run a monitored live soak before restoring unattended alarms.
