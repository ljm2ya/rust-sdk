# rusk-sdk

Unified venue SDK extracted from the `perppair`, `mondayfuck`, and
`predictmm` bots, so every repo shares one implementation per venue.

## Modules

- `order` — shared trading primitives (`Side`, `ActiveOrder`, `OrderInfo`).
- `book` — local orderbook management (`OrderBook`): snapshot + sequenced
  diff sync (Binance rules), overlay merging, `aggregate_quote`, dashboard
  `BookSnapshot`s. The venue streaming methods manage this book internally
  and emit ready-to-use book states.
- `hyperliquid` *(feature, default)* — perp venue capsule:
  `connect` (read-only) / `connect_trading`, `place_limit`, `place_market`
  (synthetic IOC crossing ±5%), `modify_order` (atomic `batchModify`,
  replacement oid tracked), `cancel_order`, `fetch_order`, `open_orders`,
  `position_snapshot`, `book`/`mid_price` — all rounded onto the market's
  real tick + size grids. `stream_book(coin, BookStreamConfig, tx)` merges a
  fast L2 WS feed with an optional slow WS overlay and an optional
  (gateable) periodic REST underlay, emitting managed `OrderBook`s.
- `binance` *(feature, default)* — USD-M futures trade WS capsule
  (`BinanceFutures`: place/status/cancel/modify/position with reconnect
  retry) and `stream_book(symbol, rest_gate, tx)`: depth diffs applied to a
  managed local book with automatic snapshot resync, emitting
  `BookUpdate::{Book, Notice}`.
- `error` / `types` / `venue` / `polymarket` *(feature `polymarket`)* — the
  prediction-market venue abstraction (`OrderbookTrader`) and the Polymarket
  adapter, extracted from predictmm (which re-exports them).

`vendor/hypersdk` carries a local patch on top of hypersdk 0.2.14
(`always_place` on `BatchModify`).

## Consumers

- `../perppair` — `hyperliquid` feature only.
- `../mondayfuck` — `hyperliquid` + `binance`.
- `../predictmm` — `polymarket` feature (re-exports `error`/`types`/`venue`/
  `polymarket`; its Pascal and HIP-4 Hyperliquid adapters build on the same
  re-exported traits).

## Tests

```sh
cargo test --all-features        # offline unit tests + doctests
```

Live real-order Hyperliquid tests (~$10.5 per order, `#[ignore]`d; needs
`PRIVATE_KEY`/`WALLET_ADDRESS` in env or `.env`; market defaults to `SKHY`,
override with `RUSK_LIVE_COIN`):

```sh
cargo test --test hyperliquid_live -- --ignored --nocapture --test-threads=1
```
