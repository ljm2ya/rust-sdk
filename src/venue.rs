//! The abstract trading interface implemented by every venue adapter.

use std::sync::Arc;

use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use crate::error::Result;
use crate::types::{MarketId, Order, Orderbook, PlaceOrderRequest, Position};

/// `price -> tick size` function resolved once by [`OrderbookTrader::fetch_market`]
/// and safe to call repeatedly afterwards with **no further I/O** — the
/// venue's raw spec parameters (Pascal: `tick_sig_figs`/`tick_size_min`;
/// Polymarket: the fixed `minimum_tick_size`) are captured at fetch time, and
/// this closure only does the local arithmetic (`pascal::tick::tick_size` for
/// Pascal; a constant for Polymarket). Callers should fetch the [`MarketSpec`]
/// once (e.g. at startup) and reuse this closure on every quoting cycle
/// instead of re-fetching per price.
pub type TickSizeFn = Arc<dyn Fn(Decimal) -> Result<Decimal> + Send + Sync>;

/// Fee/tick parameters for a market, as of the time of the call.
///
/// - `taker_fee_rate` / `maker_rebate_share` are fractions (e.g. `0.02` =
///   2%, `0.25` = 25%), matching each venue's documented fee formula.
/// - `tick_size` is a [`TickSizeFn`]: Pascal's tick size is a function of
///   price (see `pascal::tick::tick_size` and
///   <https://docs.pascal.trade/#tick-sizes>), resolved locally per call;
///   Polymarket's tick size is fixed per market and the closure ignores its
///   input. Either way, computing it never makes a network call.
#[derive(Clone)]
pub struct MarketSpec {
    pub taker_fee_rate: Decimal,
    pub maker_rebate_share: Decimal,
    pub tick_size: TickSizeFn,
}

/// Abstracted orderbook trading interface.
///
/// Implemented by [`crate::polymarket::PolymarketVenue`],
/// [`crate::pascal::PascalVenue`], and
/// [`crate::hyperliquid::HyperliquidVenue`].
#[async_trait]
pub trait OrderbookTrader: Send + Sync {
    /// Place a new order and return its normalized snapshot.
    async fn place_order(&self, request: PlaceOrderRequest) -> Result<Order>;

    /// Cancel an open order by exchange order id.
    async fn cancel_order(&self, order_id: &str) -> Result<()>;

    /// Fetch a single order by exchange order id.
    async fn fetch_order(&self, order_id: &str) -> Result<Order>;

    /// Replace an open order's price/size.
    ///
    /// - Pascal: atomic replace (`replace_client_order_id`).
    /// - Polymarket: cancel + re-place (no native modify); the returned order
    ///   has a new `order_id`.
    async fn modify_order(
        &self,
        order_id: &str,
        new_price: Decimal,
        new_size: Decimal,
    ) -> Result<Order>;

    /// Fetch the current position for a market. Returns a zero-size position
    /// when the account holds nothing in that market.
    async fn fetch_position(&self, market: &MarketId) -> Result<Position>;

    /// Fetch fee/tick parameters for `market`. Does the network fetch once;
    /// call this at startup (or on a slow refresh timer) and reuse the
    /// returned [`MarketSpec::tick_size`] closure across quoting cycles, not
    /// once per cycle — see [`MarketSpec`].
    async fn fetch_market(&self, market: &MarketId) -> Result<MarketSpec>;

    /// Subscribe to L2 orderbook updates for a market.
    ///
    /// Spawns a background task; every message on the receiver is a full book
    /// snapshot. The subscription ends when the receiver is dropped or the
    /// connection closes.
    async fn subscribe_orderbook(&self, market: &MarketId) -> Result<mpsc::Receiver<Orderbook>>;
    async fn subscribe_prices(&self, market: &MarketId) -> Result<mpsc::Receiver<Orderbook>>;
}
