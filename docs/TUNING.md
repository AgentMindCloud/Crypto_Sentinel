# Tuning guide

## Goal

Optimize for a small, trustworthy alert stream—not maximum sensitivity. A detector that alarms constantly becomes functionally silent because the operator stops responding.

## Baseline first

Run the bundled balanced preset for several days before changing thresholds. The first hour is a warm-up period for robust baselines. Extreme absolute moves can still generate emergency warnings during warm-up.

Record for each alert:

- useful / not useful
- early / late
- correct direction
- confirmed on multiple venues
- associated liquidation burst
- data/feed issue rather than market event
- duplicate of another alert

Do not tune based on one dramatic market day.

## Key controls

### Absolute return threshold

`min_abs_return_bps` prevents a statistically unusual but economically tiny move from alarming. One basis point is 0.01%.

- Raise it to reduce noise in quiet, liquid markets.
- Lower it only for instruments where small changes matter operationally.
- Small-cap symbols usually need wider thresholds, not narrower ones.

### Robust return score

`min_abs_return_z` measures abnormality versus the symbol/venue's recent regime.

- 3–4 is a practical starting range.
- Higher reduces alerts but may miss regime transitions.
- A robust score is not a Gaussian probability or a guarantee of rarity.

### Volume score

`min_volume_z` confirms participation. Thin symbols can generate misleading scores from very small notional volume, so combine it with `minimum_window_quote_volume`.

### Taker imbalance

The score ranges from -1 (all taker-sell notional) to +1 (all taker-buy notional). It is only accepted when aligned with return direction.

- Values above roughly 0.55–0.70 in magnitude are strong starting filters.
- Aggregated trade classification differs slightly by venue.
- Imbalance can reverse quickly and should not be treated as predictive on its own.

### Confirmations

Critical alarms should normally require two venues. One-venue warnings remain useful for exchange-specific incidents.

Requiring all three venues is fragile: maintenance, contract differences, regional network problems, and metadata failures can suppress valid alarms.

### Cooldown

A 10-minute default prevents repeated alarms during the same cascade. Severity upgrades are still allowed.

- Shorten for scalping-oriented monitoring.
- Lengthen if the same trend repeatedly triggers.
- Keep unique dedup categories for operational spread alerts versus market shocks.

## Suggested profiles

These are starting estimates, not validated trading signals.

### Conservative risk monitor

- 60s warning: 0.60%, z 3.5, volume z 3.0, imbalance 0.65
- 60s critical: 1.20%, z 4.5, volume z 4.0, imbalance 0.75, two venues
- 300s warning: 1.50%
- 300s critical: 3.00%, two venues
- cooldown: 15 minutes

### Balanced bundled profile

- 60s warning: 0.40%
- 60s critical: 0.80%, two venues
- 300s warning: 1.00%
- 300s critical: 2.00%, two venues
- cooldown: 10 minutes

### Sensitive research profile

- Use only with silent/low-priority warnings.
- 60s warning around 0.25%, z 2.5, volume z 2.0
- Keep critical thresholds close to balanced values.
- Increase minimum quote volume and retain two-venue critical confirmation.

## Liquidity tiers

A single global threshold is simple but imperfect. The next major enhancement should be per-symbol or per-tier thresholds:

- Tier 1: BTC, ETH
- Tier 2: SOL, BNB, XRP and other highly liquid majors
- Tier 3: smaller instruments

Until per-symbol thresholds are implemented, monitor only liquid markets with similar behavior. Do not add dozens of small caps to the default configuration.

## Liquidations

Dollar thresholds should reflect the market and the venue coverage. Binance force-order updates are snapshots and may undercount dense bursts; Bybit's stream is more complete but still depends on public feed delivery.

Treat liquidation totals as corroboration, not an exact market-wide liquidation census.

## Spread alerts

A spread alert may be:

- a genuine temporary venue dislocation
- an exchange-specific wick
- different contract mechanics
- stale data
- a conversion/contract-value problem

Confirm order-book depth and executable prices before interpreting it as arbitrage. Raise the warning threshold if normal venue basis causes persistent alerts.

## Validation process

1. Run `simulate` after every material configuration change.
2. Run `validate-config` and preserve the changed YAML in version control.
3. Test all enabled notification channels.
4. Compare one week before and after the change.
5. Change one group of thresholds at a time.
6. Keep a rollback copy of the previous config.

A useful future evaluation set is a timestamped list of known market shocks plus ordinary days. Replay raw trades through both old and new configurations and compare detection delay and false positives. Do not optimize on the same event set used for final evaluation.
