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
//!   grids) and [`hyperliquid::stream_book`] (fast/slow WS + optional REST
//!   overlay, emitting managed [`book::OrderBook`] states).
//! - [`binance`] *(feature `binance`, default)* — USD-M futures trade
//!   WebSocket capsule and [`binance::stream_book`] (depth diffs applied to
//!   a managed local book with automatic snapshot resync).
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
//! | polymarket | `POLYMARKET_PRIVATE_KEY` | EOA private key |

pub mod book;
pub mod order;

#[cfg(feature = "binance")]
pub mod binance;
#[cfg(feature = "hyperliquid")]
pub mod hyperliquid;

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
    Level, MarketId, Order, Orderbook, OrderStatus, OrderType, PlaceOrderRequest, Position, Side,
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
