# Architecture

## Design objective

Generate a small number of explainable, replayable crypto market alarms without paid data or trading credentials. Deterministic code decides whether an alarm exists. AI may later summarize the evidence, but is not in the trigger path.

## Data flow

```text
Binance aggregate trades + force-order snapshots ─┐
Bybit public trades + full liquidations ───────────┼─> normalized events
OKX public trades + instrument metadata ──────────┘
                                                      │
                                                      v
                                      fixed 5-second market buckets
                                                      │
                   ┌──────────────────────────────────┼───────────────────────────┐
                   v                                  v                           v
          return/volume baselines           liquidation windows       latest venue prices
                   │                                  │                           │
                   v                                  v                           v
       robust anomaly rules                 burst thresholds          spread/divergence rules
                   └──────────────────────────────────┼───────────────────────────┘
                                                      v
                                      merge, severity, dedup, cooldown
                                                      │
               ┌──────────────────────────┬───────────┼─────────────┬──────────────┐
               v                          v           v             v              v
         local sound                browser/SSE     SQLite         ntfy          Telegram/webhook
```

## Feed adapters

Each exchange adapter owns connection, subscription, payload normalization, heartbeat, reconnect, and health counters. Connections use capped exponential backoff with jitter. A stable connection resets the backoff so routine exchange disconnects recover quickly.

Normalized events are only:

- `Trade(exchange, symbol, timestamp, price, base quantity, taker side, event ID)`
- `Liquidation(exchange, symbol, timestamp, price, base quantity, liquidated side, event ID)`

No adapter can trade or authenticate to an exchange.

## State model

Trades are aggregated into fixed-time buckets containing OHLC, quote volume, taker-buy quote volume, taker-sell quote volume, and trade count. Late events inside retention are inserted in time order; OHLC open/close are protected by event timestamps.

Trade-event deduplication is intentionally bounded to a short reconnect window and a fixed maximum number of IDs. Retaining all IDs for a one-hour baseline would cause memory growth on active symbols.

The bucket state and recent liquidations are periodically written to an atomic gzip-compressed checkpoint. A clean or ordinary crash restart can restore the rolling baseline without waiting an hour. Stale, future-dated, incompatible, non-finite, or wrong-symbol checkpoint entries are rejected.

## Robust statistics

For each configured window, current return and quote volume are compared with historical windows from the rolling baseline. The score uses the median and median absolute deviation (MAD), with a fallback for degenerate samples.

This is preferable to mean/standard-deviation scoring for heavy-tailed market data, but it does not make the observations independent or normally distributed. Scores are a prioritization device, not calibrated probabilities.

## Alert logic

A normal market anomaly needs:

1. absolute price displacement,
2. an unusual robust return score,
3. either unusual volume or taker imbalance aligned with price direction,
4. minimum quote volume,
5. fresh data.

Severity can be upgraded by stronger thresholds and cross-exchange agreement. During baseline warm-up, only an extreme absolute move can produce an emergency warning.

Liquidation bursts are evaluated separately and merged with directionally consistent price anomalies into a `market_shock`. If 60-second and 300-second rules fire together, only the stronger alert is retained; ties prefer the shorter window.

Cross-exchange spread alerts are deliberately categorized as operational/risk signals. They may indicate arbitrage, a bad wick, contract differences, or a venue/data problem. Never assume they are executable arbitrage.

## Delivery guarantees

There is no exactly-once guarantee across external notification services. Internally:

- event IDs reduce reconnect duplicates,
- alert dedup keys enforce a configurable cooldown,
- recent cooldown keys are restored from SQLite after restart,
- severity upgrades bypass a weaker previous alert,
- SQLite persistence is attempted before delivery, but persistence failure does not suppress browser/local alarms,
- local sound is serialized independently of remote HTTP delivery,
- remote delivery uses a bounded queue, two workers, and three attempts per enabled provider,
- one failing notifier does not block other channels or the detector loop.

Remote retries are deliberately at-least-once: a provider can accept a request and lose the response, causing a duplicate on retry. The in-memory remote queue is not a durable outbox and can lose unsent messages in an abrupt process failure.

The browser uses server-sent events. Browser audio requires a user gesture and can be throttled by the browser, so the process-level sound or mobile notifier remains primary.

## Failure modes

- DNS/network failure: adapter reconnects; feed-health alarm fires through surviving channels.
- One venue stale: multi-exchange critical confirmation may be unavailable; status dashboard shows it.
- Process crash/host sleep: no local detection. External uptime monitoring is needed for a true dead-man check.
- Queue overload: events are dropped and counted. Reduce symbols or increase capacity only after inspecting memory.
- Exchange schema change: parser may stop producing normalized events; stale-feed alarm should surface this.
- OKX metadata failure: relative volume remains useful but absolute quote-volume estimates may be wrong.
- Notification outage: remote delivery retries and logs failure; other configured channels continue.
- SQLite failure: detector and alarm delivery continue, but history/metrics can be incomplete.
- Remote queue exhaustion: browser/local paths still run; the dropped remote delivery is logged.

## Build vs. free SaaS

This core replaces overlapping free scanners with one auditable detector. Free SaaS tools can still be used as corroborating inputs through `/api/ingest`. This reduces vendor quota dependence and duplicate alarms, at the cost of maintaining exchange adapters and a continuously running host.
