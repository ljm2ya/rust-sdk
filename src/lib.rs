//! # rusk-sdk
//!
//! Unified venue SDK extracted from the perppair / mondayfuck / predictmm
//! bots, so every repo shares one implementation per venue:
//!
//! - [`order`] — shared trading primitives ([`order::Side`],
//!   [`order::ActiveOrder`], [`order::OrderInfo`]).
//! - [`book`] — local orderbook management ([`book::OrderBook`]): snapshot +
//!   sequenced-diff sync (Binance style), overlay merging, quote
//!   aggregation, dashboard snapshots. The venue streaming methods manage
//!   this book internally and emit ready-to-use book states.
//! - [`hyperliquid`] *(feature `hyperliquid`, default)* — perp venue capsule
//!   (place/modify/cancel/fetch/positions on the market's real tick + size
//!   grids), [`hyperliquid::stream_book`] (fast/slow WS + optional REST
//!   overlay, emitting managed [`book::OrderBook`] states) and
//!   [`hyperliquid::stream_user_events`] (private `userFills` and
//!   `orderUpdates` WS).
//! - [`binance`] *(feature `binance`, default)* — USD-M futures trade
//!   WebSocket capsule and [`binance::stream_book`] (depth diffs applied to
//!   a managed local book with automatic snapshot resync).
//! - [`lighter`] *(feature `lighter`, default)* — Lighter (zkLighter) perp
//!   venue capsule (place/modify/cancel/fetch on the market's wire grids,
//!   positions, account summary) plus [`lighter::stream_book`]
//!   (`order_book/{market_id}` WS channel with nonce-continuity resync).
//!   Transaction signing runs through the vendored official Go signer
//!   shared library (`vendor/lighter-signer`).
//! - [`upbit`] *(feature `upbit`, default)* — Upbit spot venue capsule
//!   (JWT-signed REST: limit/market orders keyed by numeric client
//!   identifiers, fetch/cancel, open orders, balances, fees,
//!   deposit/withdraw queries; public ticker / book / trades / candles with
//!   KRW tick-grid rounding), [`upbit::stream_book`] (full-snapshot WS
//!   books) and [`upbit::stream_user_events`] (private `myOrder` /
//!   `myAsset` WS).
//! - [`coinone`] *(feature `coinone`, default)* — Coinone spot venue capsule
//!   (HMAC-SHA512-signed REST: limit/market orders keyed by numeric client
//!   ids echoed via `user_order_id`, fetch/cancel, open orders, balances,
//!   trade fees; public ticker / book / trades / candles rounded onto the
//!   market's `price_unit`/`qty_unit` grids), [`coinone::stream_book`]
//!   (full-snapshot `ORDERBOOK` WS) and [`coinone::stream_user_events`]
//!   (private `MYORDER` / `MYASSET` WS).
//! - [`error`] / [`types`] / [`venue`] / [`polymarket`] *(feature
//!   `polymarket`)* — the prediction-market venue abstraction
//!   ([`venue::OrderbookTrader`]) and the Polymarket adapter, extracted from
//!   predictmm (which re-exports them).
//!
//! ## Environment keys
//!
//! | Module | Key | Meaning |
//! |--------|-----|---------|
//! | hyperliquid | `WALLET_ADDRESS` | master account address for info queries |
//! | hyperliquid | `PRIVATE_KEY` | hex signing key (trading mode) |
//! | hyperliquid | `HYPERLIQUID_COIN_<NAME>` | per-coin market override |
//! | binance | `BINANCE_API_KEY` / `BINANCE_API_SECRET` | trade WS credentials |
//! | binance | `PRICE_DECIMALS` / `QTY_DECIMALS` | log/modify formatting |
//! | lighter | `LIGHTER_PRIVATE_KEY` / `LIGHTER_PUBLIC_KEY` | API key pair (trading mode) |
//! | lighter | `LIGHTER_ACCOUNT_INDEX` | account index (else derived from `WALLET_ADDRESS`) |
//! | lighter | `LIGHTER_API_KEY_INDEX` | API key slot, default `2` |
//! | lighter | `LIGHTER_BASE_URL` | REST base, default mainnet |
//! | lighter | `LIGHTER_SIGNER_LIB` | signer shared-library override |
//! | upbit | `UPBIT_ACCESS_KEY` / `UPBIT_SECRET_KEY` | JWT credentials (trading mode + private WS) |
//! | coinone | `COINONE_ACCESS_TOKEN` / `COINONE_SECRET_KEY` | HMAC credentials (trading mode + private WS) |
//! | polymarket | `POLYMARKET_PRIVATE_KEY` | EOA private key |

pub mod book;
pub mod order;

#[cfg(feature = "binance")]
pub mod binance;
#[cfg(feature = "coinone")]
pub mod coinone;
#[cfg(feature = "hyperliquid")]
pub mod hyperliquid;
#[cfg(feature = "lighter")]
pub mod lighter;
#[cfg(feature = "upbit")]
pub mod upbit;

#[cfg(feature = "polymarket")]
pub mod error;
#[cfg(feature = "polymarket")]
pub mod polymarket;
#[cfg(feature = "polymarket")]
pub mod types;
#[cfg(feature = "polymarket")]
pub mod venue;

#[cfg(feature = "polymarket")]
pub use error::{Error, Result};
#[cfg(feature = "polymarket")]
pub use types::{
    Level, MarketId, Order, OrderStatus, OrderType, Orderbook, PlaceOrderRequest, Position, Side,
    TimeInForce,
};
#[cfg(feature = "polymarket")]
pub use venue::OrderbookTrader;

/// Load `.env` from the working directory (or ancestors) exactly once.
///
/// Called automatically by every venue's `from_env`-style constructor;
/// call it yourself before reading other keys. Values already present in the
/// process environment always take precedence.
pub fn load_dotenv() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = dotenvy::dotenv();
    });
}

/// `$key` from the environment (after [`load_dotenv`]), or `default` when
/// unset/empty.
pub fn env_or(key: &str, default: &str) -> String {
    load_dotenv();
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default.to_owned())
}
