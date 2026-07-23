//! Capsulized Hyperliquid **perp** venue adapter, wrapping the vendored
//! `hypersdk` crate (local patch: `always_place` on `BatchModify`).
//! Unifies the adapters previously duplicated in perppair and mondayfuck.
//!
//! ## Design
//!
//! - [`Hyperliquid`] is one capsule per market: it resolves the coin (env
//!   alias → optional HIP-3 `dex:COIN` prefix → asset index) once at connect
//!   time and caches the market's price-tick table and size decimals, so
//!   every order is rounded onto the venue's real grid instead of a
//!   hardcoded decimal count.
//! - Two modes: [`Hyperliquid::connect`] (read-only: books, positions,
//!   open orders — needs `WALLET_ADDRESS`) and
//!   [`Hyperliquid::connect_trading`] (adds the `PRIVATE_KEY` signer).
//! - All methods take `&self`; nonces come from the SDK's atomic
//!   [`NonceHandler`], so one capsule can be shared across tasks.
//! - Market orders are synthetic: HyperCore has no pure market order type,
//!   so [`Hyperliquid::place_market`] sends an IOC limit crossing the
//!   opposite top of book by 5% (tick-rounded).
//! - `orderStatus` is eventually consistent — [`Hyperliquid::fetch_order`]
//!   polls briefly before concluding an oid is unknown.
//! - [`stream_book`] manages a local [`OrderBook`] internally: a fast L2 WS
//!   feed, optionally overlaid on a slow WS feed and a periodically
//!   refreshed REST snapshot (mondayfuck's combined-book scheme), emitting
//!   ready-to-use [`OrderBook`] states.
//!
//! ## Environment keys
//!
//! - `WALLET_ADDRESS` — master account address for info queries.
//! - `PRIVATE_KEY` — hex signing key (trading mode only).
//! - `HYPERLIQUID_COIN_<NAME>` — optional per-coin market override
//!   (e.g. `HYPERLIQUID_COIN_SKHYNIX=xyz:SKHX`).

use std::{collections::BTreeMap, env, str::FromStr, sync::Arc, time::Duration};

use anyhow::{Context, bail};
use either::Either;
use futures_util::StreamExt;
use hypersdk::{
    Address,
    hypercore::{
        self, HttpClient, NonceHandler, PriceTick, PrivateKeySigner,
        types::{
            AssetPosition, BasicOrder, BatchCancel, BatchModify, BatchOrder, Cancel, Incoming,
            L2Book, Modify, OrderGrouping, OrderRequest, OrderResponseStatus,
            OrderStatus as HlOrderStatus, OrderTypePlacement, OrderUpdate, Side as BookSide,
            Subscription, TimeInForce,
        },
        ws::Event,
    },
};
use rust_decimal::Decimal;
use tokio::{sync::mpsc, time::interval};

use crate::book::{OrderBook, levels_to_map, overlay_source};
use crate::order::{ActiveOrder, OrderInfo, OrderState, Side};

/// Multiplicative slippage bound for synthetic market orders (cross the
/// opposite top of book by 5%).
const MARKET_ORDER_SLIPPAGE: f64 = 0.05;

/// `orderStatus` is eventually consistent: poll this many times, this far
/// apart, before concluding an oid is unknown.
const FETCH_ATTEMPTS: u32 = 5;
const FETCH_DELAY: Duration = Duration::from_millis(200);

/// Book side used for tick rounding (`Bid` rounds down, `Ask` up).
fn book_side(side: Side) -> BookSide {
    match side {
        Side::Buy => BookSide::Bid,
        Side::Sell => BookSide::Ask,
    }
}

/// Perp position snapshot from `clearinghouseState` (size signed: long > 0).
#[derive(Clone, Copy, Debug, Default)]
pub struct PositionSnapshot {
    pub size: f64,
    pub position_value: f64,
    pub unrealized_pnl: f64,
    pub return_on_equity: f64,
    pub entry_px: Option<f64>,
}

/// Market identity + grids, resolved once at connect time.
#[derive(Clone)]
struct MarketSpec {
    /// Full coin name as the exchange knows it (e.g. `xyz:SKHY`).
    coin: String,
    /// HIP-3 dex prefix when present (e.g. `xyz`).
    dex_name: Option<String>,
    /// Asset index used in signed actions.
    asset: usize,
    /// Decimal places allowed in order sizes.
    sz_decimals: i64,
    /// Valid price increments for this market.
    tick: PriceTick,
}

/// Hyperliquid perp venue capsule for a single market.
pub struct Hyperliquid {
    client: HttpClient,
    user: Address,
    signer: Option<PrivateKeySigner>,
    nonces: NonceHandler,
    market: MarketSpec,
}

impl Hyperliquid {
    /// Read-only mode: books, positions, open orders. Needs `WALLET_ADDRESS`.
    pub async fn connect(coin: &str) -> anyhow::Result<Self> {
        Self::build(coin, false).await
    }

    /// Trading mode: everything in [`connect`](Self::connect) plus signed
    /// actions (place/modify/cancel). Needs `PRIVATE_KEY` too.
    pub async fn connect_trading(coin: &str) -> anyhow::Result<Self> {
        Self::build(coin, true).await
    }

    async fn build(coin: &str, trading: bool) -> anyhow::Result<Self> {
        crate::load_dotenv();
        let client = hypercore::mainnet();
        let user: Address = env::var("WALLET_ADDRESS")
            .context("WALLET_ADDRESS is required")?
            .parse()
            .context("WALLET_ADDRESS is not a valid address")?;
        let signer = match trading {
            true => {
                let key = env::var("PRIVATE_KEY").context("PRIVATE_KEY is required")?;
                let key = if key.starts_with("0x") { key } else { format!("0x{key}") };
                Some(key.parse::<PrivateKeySigner>().context("PRIVATE_KEY is not a valid hex key")?)
            }
            false => None,
        };
        let market = resolve_market(&client, &normalize_coin(coin)).await?;
        Ok(Self { client, user, signer, nonces: NonceHandler::default(), market })
    }

    /// Full market name this capsule trades (e.g. `xyz:SKHY`).
    pub fn coin(&self) -> &str {
        &self.market.coin
    }

    fn signer(&self) -> anyhow::Result<&PrivateKeySigner> {
        self.signer.as_ref().with_context(|| {
            format!("Hyperliquid {} is read-only: connect with connect_trading (PRIVATE_KEY)", self.market.coin)
        })
    }

    /// REST L2 book snapshot.
    pub async fn book(&self) -> anyhow::Result<OrderBook> {
        let book = self.client.l2_book(self.market.coin.clone(), None, None).await?;
        let (bids, asks) = sdk_book_levels(&book)?;
        Ok(OrderBook::from_sides(bids, asks))
    }

    /// Mid of the current top of book.
    pub async fn mid_price(&self) -> anyhow::Result<f64> {
        self.book()
            .await?
            .mid()
            .with_context(|| format!("Hyperliquid {} book is one-sided or empty", self.market.coin))
    }

    /// Place a GTC limit order. Price is rounded onto the market tick grid
    /// (conservatively: bids down, asks up) and size truncated onto the
    /// market's size decimals.
    pub async fn place_limit(&self, side: Side, qty: f64, price: f64, reduce_only: bool) -> anyhow::Result<ActiveOrder> {
        let px = round_price(&self.market.tick, side, price)?;
        let sz = round_size(self.market.sz_decimals, qty)?;
        self.submit(side, sz, px, reduce_only, TimeInForce::Gtc).await
    }

    /// Synthetic market order: IOC limit crossing the opposite top of book
    /// by [`MARKET_ORDER_SLIPPAGE`]. The unfilled remainder (if any) is
    /// canceled by the venue, never rests.
    pub async fn place_market(&self, side: Side, qty: f64, reduce_only: bool) -> anyhow::Result<ActiveOrder> {
        let book = self.book().await?;
        let opposite_top = match side {
            Side::Buy => book.best_ask(),
            Side::Sell => book.best_bid(),
        };
        let px = crossing_price(&self.market.tick, side, opposite_top)?;
        let sz = round_size(self.market.sz_decimals, qty)?;
        self.submit(side, sz, px, reduce_only, TimeInForce::Ioc).await
    }

    async fn submit(&self, side: Side, sz: Decimal, px: Decimal, reduce_only: bool, tif: TimeInForce) -> anyhow::Result<ActiveOrder> {
        let batch = BatchOrder {
            orders: vec![self.order_request(side, sz, px, reduce_only, tif)],
            grouping: OrderGrouping::Na,
            builder: None,
        };
        let statuses = self
            .client
            .place(self.signer()?, batch, self.nonces.next(), None, None)
            .await
            .map_err(|err| anyhow::anyhow!("Hyperliquid {} place failed: {}", self.market.coin, err.message()))?;
        active_from_status(statuses.into_iter().next(), side, decimal_to_f64(sz)?, &self.market.coin)
    }

    /// Re-price a live order (native atomic `batchModify`, keeping the
    /// original side/size). Returns:
    ///
    /// - `Ok(Some(order))` — still live (possibly under a new oid);
    /// - `Ok(None)` — the order no longer exists to modify (filled by the
    ///   time the modify landed).
    pub async fn modify_order(&self, order: &ActiveOrder, price: f64, reduce_only: bool) -> anyhow::Result<Option<ActiveOrder>> {
        if order.order_id == 0 {
            return Ok(None);
        }
        let px = round_price(&self.market.tick, order.side, price)?;
        let sz = round_size(self.market.sz_decimals, order.qty)?;
        let result = self
            .client
            .modify(
                self.signer()?,
                BatchModify {
                    always_place: true,
                    modifies: vec![Modify {
                        oid: Either::Left(order.order_id),
                        order: self.order_request(order.side, sz, px, reduce_only, TimeInForce::Gtc),
                    }],
                },
                self.nonces.next(),
                None,
                None,
            )
            .await;
        let outcome = result
            .map(|statuses| statuses.into_iter().next())
            .map_err(|err| err.message().to_string());
        match classify_modify(outcome, &self.market.coin)? {
            ModifyOutcome::Gone => Ok(None),
            ModifyOutcome::Live(oid) => Ok(Some(ActiveOrder {
                order_id: oid.unwrap_or(order.order_id),
                side: order.side,
                qty: order.qty,
            })),
        }
    }

    /// Cancel a live order by oid.
    pub async fn cancel_order(&self, order_id: u64) -> anyhow::Result<()> {
        let statuses = self
            .client
            .cancel(
                self.signer()?,
                BatchCancel { cancels: vec![Cancel { asset: self.market.asset, oid: order_id }] },
                self.nonces.next(),
                None,
                None,
            )
            .await
            .map_err(|err| anyhow::anyhow!("Hyperliquid {} cancel failed: {}", self.market.coin, err.message()))?;
        match statuses.into_iter().next() {
            Some(OrderResponseStatus::Error(err)) => bail!("Hyperliquid {} cancel rejected: {err}", self.market.coin),
            Some(_) => Ok(()),
            None => bail!("Hyperliquid {} cancel returned empty response", self.market.coin),
        }
    }

    /// `orderStatus` lookup by oid — works for closed orders too.
    /// `Ok(None)` when the exchange has never seen the id (polled briefly:
    /// the endpoint is eventually consistent right after a place).
    pub async fn fetch_order(&self, order_id: u64) -> anyhow::Result<Option<OrderInfo>> {
        for attempt in 0..FETCH_ATTEMPTS {
            let update = self.client.order_status(self.user, Either::Left(order_id)).await?;
            if let Some(update) = update {
                return Ok(Some(order_info_from_update(&update)?));
            }
            if attempt + 1 < FETCH_ATTEMPTS {
                tokio::time::sleep(FETCH_DELAY).await;
            }
        }
        Ok(None)
    }

    /// Open orders on this capsule's market (`frontendOpenOrders` scan).
    pub async fn open_orders(&self) -> anyhow::Result<Vec<OrderInfo>> {
        let orders = self.client.open_orders(self.user, self.market.dex_name.clone()).await?;
        orders
            .iter()
            .filter(|order| coin_matches(&order.coin, &self.market.coin))
            .map(order_info_from_basic)
            .collect()
    }

    /// Perp position on this capsule's market (zeroed default when flat).
    pub async fn position_snapshot(&self) -> anyhow::Result<PositionSnapshot> {
        let state = self.client.clearinghouse_state(self.user, self.market.dex_name.clone()).await?;
        snapshot_from_positions(&state.asset_positions, &self.market.coin)
    }

    /// Signed position size (long > 0, short < 0, flat = 0).
    pub async fn position_amt(&self) -> anyhow::Result<f64> {
        Ok(self.position_snapshot().await?.size)
    }

    fn order_request(&self, side: Side, sz: Decimal, px: Decimal, reduce_only: bool, tif: TimeInForce) -> OrderRequest {
        OrderRequest {
            asset: self.market.asset,
            is_buy: side.is_buy(),
            limit_px: px,
            sz,
            reduce_only,
            order_type: OrderTypePlacement::Limit { tif },
            cloid: Default::default(),
        }
    }
}

/// Resolve a (normalized) coin to its market spec, honoring HIP-3
/// `dex:COIN` prefixes.
async fn resolve_market(client: &HttpClient, coin: &str) -> anyhow::Result<MarketSpec> {
    let dex_name = split_dex(coin);
    let markets = if let Some(dex_name) = &dex_name {
        let dex = client
            .perp_dexes()
            .await?
            .into_iter()
            .find(|dex| dex.name() == dex_name)
            .with_context(|| format!("Hyperliquid perp DEX not found: {dex_name}"))?;
        client.perps_from(dex).await?
    } else {
        client.perps().await?
    };
    let market = markets
        .into_iter()
        .find(|market| coin_matches(&market.name, coin))
        .with_context(|| format!("Hyperliquid perp market not found: {coin}"))?;
    Ok(MarketSpec {
        coin: coin.to_string(),
        dex_name,
        asset: market.index,
        sz_decimals: market.sz_decimals,
        tick: market.table,
    })
}

// ---------------------------------------------------------------------------
// Book streaming (WebSocket, managed local orderbook)
// ---------------------------------------------------------------------------

/// How [`stream_book`] assembles the local book.
#[derive(Clone)]
pub struct BookStreamConfig {
    /// Overlay a second, slow (`fast: false`) L2 WS feed under the fast one.
    pub slow_overlay: bool,
    /// Periodically refresh a REST snapshot underlay at this interval.
    pub rest_refresh: Option<Duration>,
    /// Gate REST snapshot fetches (e.g. skip after a market-close cutoff).
    pub rest_gate: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    /// Levels per side in the emitted books.
    pub depth: usize,
}

impl Default for BookStreamConfig {
    fn default() -> Self {
        Self { slow_overlay: false, rest_refresh: None, rest_gate: None, depth: 50 }
    }
}

impl BookStreamConfig {
    fn rest_allowed(&self) -> bool {
        self.rest_gate.as_ref().is_none_or(|gate| gate())
    }
}

/// Stream managed L2 books for `coin` into `tx` (top `depth` levels per
/// side), merging REST underlay ← slow WS ← fast WS overlays and seeding
/// with one REST snapshot so consumers get data immediately.
pub async fn stream_book(coin: String, config: BookStreamConfig, tx: mpsc::Sender<OrderBook>) -> anyhow::Result<()> {
    let coin = normalize_coin(&coin);
    let client = hypercore::mainnet();
    let mut fast_ws = hypercore::mainnet_ws();
    fast_ws.subscribe(Subscription::L2Book { coin: coin.clone(), n_sig_figs: None, mantissa: None, fast: true });
    let mut slow_ws = config.slow_overlay.then(|| {
        let ws = hypercore::mainnet_ws();
        ws.subscribe(Subscription::L2Book { coin: coin.clone(), n_sig_figs: None, mantissa: None, fast: false });
        ws
    });
    let mut rest_tick = config.rest_refresh.map(interval);

    let mut rest_book = SourceOverlay::default();
    let mut slow_book = SourceOverlay::default();
    let mut fast_book = SourceOverlay::default();

    if config.rest_allowed()
        && let Ok(book) = client.l2_book(coin.clone(), None, None).await
        && let Ok((bids, asks)) = sdk_book_levels(&book)
    {
        rest_book.set(bids, asks);
        if send_combined(&tx, &rest_book, &slow_book, &fast_book, config.depth).await.is_err() {
            return Ok(());
        }
    }

    loop {
        tokio::select! {
            Some(event) = fast_ws.next() => {
                if let Event::Message(Incoming::L2Book(book)) = event {
                    if book.coin != coin { continue; }
                    let (bids, asks) = sdk_book_levels(&book)?;
                    fast_book.set(bids, asks);
                    if config.rest_refresh.is_none() {
                        // Without periodic refreshes the REST book is only a
                        // pre-WS seed; drop it so stale levels outside the WS
                        // feed's range can never cross the live top of book.
                        rest_book = SourceOverlay::default();
                    }
                    if send_combined(&tx, &rest_book, &slow_book, &fast_book, config.depth).await.is_err() {
                        return Ok(());
                    }
                }
            }
            Some(event) = next_or_pending(&mut slow_ws) => {
                if let Event::Message(Incoming::L2Book(book)) = event {
                    if book.coin != coin { continue; }
                    let (bids, asks) = sdk_book_levels(&book)?;
                    slow_book.set(bids, asks);
                    if send_combined(&tx, &rest_book, &slow_book, &fast_book, config.depth).await.is_err() {
                        return Ok(());
                    }
                }
            }
            _ = tick_or_pending(&mut rest_tick) => {
                if config.rest_allowed()
                    && let Ok(book) = client.l2_book(coin.clone(), None, None).await
                    && let Ok((bids, asks)) = sdk_book_levels(&book)
                {
                    rest_book.set(bids, asks);
                    if send_combined(&tx, &rest_book, &slow_book, &fast_book, config.depth).await.is_err() {
                        return Ok(());
                    }
                }
            }
        }
    }
}

#[derive(Default)]
struct SourceOverlay {
    bids: BTreeMap<i64, f64>,
    asks: BTreeMap<i64, f64>,
}

impl SourceOverlay {
    fn set(&mut self, bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>) {
        self.bids = levels_to_map(bids);
        self.asks = levels_to_map(asks);
    }
}

/// Merge REST ← slow ← fast overlays and emit the top `depth` levels.
async fn send_combined(
    tx: &mpsc::Sender<OrderBook>,
    rest_book: &SourceOverlay,
    slow_book: &SourceOverlay,
    fast_book: &SourceOverlay,
    depth: usize,
) -> Result<(), ()> {
    let mut bids = rest_book.bids.clone();
    let mut asks = rest_book.asks.clone();
    overlay_source(&mut bids, &slow_book.bids);
    overlay_source(&mut asks, &slow_book.asks);
    overlay_source(&mut bids, &fast_book.bids);
    overlay_source(&mut asks, &fast_book.asks);

    let book = OrderBook::from_sides(
        bids.iter().rev().take(depth).map(|(price, qty)| (crate::book::key_to_price(*price), *qty)).collect(),
        asks.iter().take(depth).map(|(price, qty)| (crate::book::key_to_price(*price), *qty)).collect(),
    );
    tx.send(book).await.map_err(|_| ())
}

async fn next_or_pending(ws: &mut Option<hypercore::WebSocket>) -> Option<Event> {
    match ws {
        Some(ws) => ws.next().await,
        None => std::future::pending().await,
    }
}

async fn tick_or_pending(tick: &mut Option<tokio::time::Interval>) {
    match tick {
        Some(tick) => {
            tick.tick().await;
        }
        None => std::future::pending().await,
    }
}

// ---------------------------------------------------------------------------
// Pure helpers (unit tested offline below)
// ---------------------------------------------------------------------------

/// Env alias override (`HYPERLIQUID_COIN_<NAME>`) → built-in alias → as-is.
fn normalize_coin(coin: &str) -> String {
    env::var(format!("HYPERLIQUID_COIN_{}", coin.replace(':', "_").to_ascii_uppercase()))
        .unwrap_or_else(|_| default_coin_alias(coin).unwrap_or(coin).to_string())
}

/// Built-in dashboard-label → HIP-3 market aliases.
fn default_coin_alias(coin: &str) -> Option<&'static str> {
    match coin.to_ascii_uppercase().as_str() {
        "SKHY" => Some("xyz:SKHY"),
        "SKHYNIX" | "SKHX" => Some("xyz:SKHX"),
        "WTIOIL" | "XYZ:WTIOIL" => Some("xyz:CL"),
        _ => None,
    }
}

/// `xyz:SKHY` → `Some("xyz")`; plain coins → `None`.
fn split_dex(coin: &str) -> Option<String> {
    coin.split_once(':').map(|(dex_name, _)| dex_name.to_string())
}

/// Does an exchange-reported market/position coin name refer to `coin`?
/// (HIP-3 endpoints sometimes drop the `dex:` prefix.)
fn coin_matches(candidate: &str, coin: &str) -> bool {
    candidate == coin || coin.ends_with(&format!(":{candidate}"))
}

/// Round a price onto the market tick grid, conservatively per side
/// (bids down, asks up) so the order is never more aggressive than asked.
fn round_price(tick: &PriceTick, side: Side, price: f64) -> anyhow::Result<Decimal> {
    let raw = Decimal::from_str(&format!("{price}"))
        .with_context(|| format!("price {price} is not representable"))?;
    if raw <= Decimal::ZERO {
        // PriceTick panics on log10 of non-positive prices; refuse first.
        bail!("price {price} must be positive");
    }
    tick.round_by_side(book_side(side), raw, true)
        .with_context(|| format!("price {price} cannot be rounded onto the market tick grid"))
}

/// Truncate a size onto the market's size decimals (never round up: an
/// oversized reduce-only close could flip the position).
fn round_size(sz_decimals: i64, qty: f64) -> anyhow::Result<Decimal> {
    let raw = Decimal::from_str(&format!("{qty}"))
        .with_context(|| format!("size {qty} is not representable"))?;
    let truncated = raw.trunc_with_scale(sz_decimals.max(0) as u32).normalize();
    if truncated <= Decimal::ZERO {
        bail!("size {qty} truncates to zero at {sz_decimals} size decimals");
    }
    Ok(truncated)
}

/// Synthetic market-order price: opposite top of book ± 5%, tick-rounded.
fn crossing_price(tick: &PriceTick, side: Side, opposite_top: Option<f64>) -> anyhow::Result<Decimal> {
    let top = opposite_top.context("market order with an empty opposite book side")?;
    let raw = match side {
        Side::Buy => top * (1.0 + MARKET_ORDER_SLIPPAGE),
        Side::Sell => top * (1.0 - MARKET_ORDER_SLIPPAGE),
    };
    round_price(tick, side, raw)
}

/// Normalize the first status of a place response into an [`ActiveOrder`].
fn active_from_status(status: Option<OrderResponseStatus>, side: Side, qty: f64, coin: &str) -> anyhow::Result<ActiveOrder> {
    match status {
        Some(OrderResponseStatus::Error(err)) => bail!("Hyperliquid {coin} order rejected: {err}"),
        Some(status) => Ok(ActiveOrder { order_id: status.oid().unwrap_or(0), side, qty }),
        None => bail!("Hyperliquid {coin} place returned empty response"),
    }
}

/// What a modify attempt means for the tracked order.
#[derive(Debug, PartialEq, Eq)]
enum ModifyOutcome {
    /// The order no longer exists to modify (filled/canceled underneath us).
    Gone,
    /// Still live; `Some(oid)` when the response carried the (new) oid.
    Live(Option<u64>),
}

/// Classify a modify response (`Ok(first status)`) or transport/batch error
/// message into a [`ModifyOutcome`].
fn classify_modify(result: Result<Option<OrderResponseStatus>, String>, coin: &str) -> anyhow::Result<ModifyOutcome> {
    match result {
        Ok(Some(OrderResponseStatus::Error(err))) if order_gone(&err) => Ok(ModifyOutcome::Gone),
        Ok(Some(OrderResponseStatus::Error(err))) => bail!("Hyperliquid {coin} modify rejected: {err}"),
        Ok(Some(OrderResponseStatus::Filled { .. })) => Ok(ModifyOutcome::Gone),
        Ok(Some(status)) => Ok(ModifyOutcome::Live(status.oid())),
        Ok(None) => bail!("Hyperliquid {coin} modify returned empty response"),
        Err(err) if order_gone(&err) => Ok(ModifyOutcome::Gone),
        Err(err) if err.contains("No need") || err.contains("same") => Ok(ModifyOutcome::Live(None)),
        Err(err) => bail!("Hyperliquid {coin} modify failed: {err}"),
    }
}

/// Error messages meaning "that oid is not live anymore".
pub fn order_gone(err: &str) -> bool {
    err.contains("does not exist")
        || err.contains("not found")
        || err.contains("never placed, already canceled, or filled")
        || err.contains("Cannot modify canceled or filled order")
}

/// Normalize an `orderStatus` update. Fill accounting is `orig_sz - sz`
/// (`sz` is the remaining size).
fn order_info_from_update(update: &OrderUpdate<BasicOrder>) -> anyhow::Result<OrderInfo> {
    let order = &update.order;
    let state = if update.status.is_filled() {
        OrderState::Filled
    } else if update.status.is_rejected() {
        OrderState::Rejected
    } else if matches!(update.status, HlOrderStatus::Open | HlOrderStatus::Triggered) {
        OrderState::Open
    } else {
        // Every remaining finished state is a cancellation flavor.
        OrderState::Canceled
    };
    order_info(order, state)
}

/// Normalize a `frontendOpenOrders` row (open by definition).
fn order_info_from_basic(order: &BasicOrder) -> anyhow::Result<OrderInfo> {
    order_info(order, OrderState::Open)
}

fn order_info(order: &BasicOrder, state: OrderState) -> anyhow::Result<OrderInfo> {
    let filled = (order.orig_sz - order.sz).max(Decimal::ZERO);
    Ok(OrderInfo {
        order_id: order.oid,
        state,
        side: if order.side == BookSide::Bid { Side::Buy } else { Side::Sell },
        limit_px: decimal_to_f64(order.limit_px)?,
        filled_qty: decimal_to_f64(filled)?,
        remaining_qty: decimal_to_f64(order.sz)?,
    })
}

/// Find this market's perp position in a `clearinghouseState` (defaults to
/// a zeroed snapshot when flat).
fn snapshot_from_positions(positions: &[AssetPosition], coin: &str) -> anyhow::Result<PositionSnapshot> {
    let Some(pos) = positions.iter().find(|pos| coin_matches(&pos.position.coin, coin)) else {
        return Ok(PositionSnapshot::default());
    };
    Ok(PositionSnapshot {
        size: decimal_to_f64(pos.position.szi)?,
        position_value: decimal_to_f64(pos.position.position_value)?,
        unrealized_pnl: decimal_to_f64(pos.position.unrealized_pnl)?,
        return_on_equity: decimal_to_f64(pos.position.return_on_equity)?,
        entry_px: pos.position.entry_px.map(decimal_to_f64).transpose()?,
    })
}

/// SDK `l2Book` payload → best-first level vectors.
fn sdk_book_levels(book: &L2Book) -> anyhow::Result<(Vec<(f64, f64)>, Vec<(f64, f64)>)> {
    let convert = |levels: &[hypercore::types::BookLevel]| {
        levels
            .iter()
            .map(|level| Ok((decimal_to_f64(level.px)?, decimal_to_f64(level.sz)?)))
            .collect::<anyhow::Result<Vec<_>>>()
    };
    Ok((convert(book.bids())?, convert(book.asks())?))
}

fn decimal_to_f64(value: Decimal) -> anyhow::Result<f64> {
    value.to_string().parse().map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(value: &str) -> Decimal {
        value.parse().expect("decimal literal")
    }

    #[test]
    fn normalizes_coin_aliases() {
        // Built-in dashboard-label aliases (union of perppair + mondayfuck).
        assert_eq!(default_coin_alias("SKHY"), Some("xyz:SKHY"));
        assert_eq!(default_coin_alias("skhynix"), Some("xyz:SKHX"));
        assert_eq!(default_coin_alias("SKHX"), Some("xyz:SKHX"));
        assert_eq!(default_coin_alias("WTIOIL"), Some("xyz:CL"));
        assert_eq!(default_coin_alias("xyz:WTIOIL"), Some("xyz:CL"));
        assert_eq!(default_coin_alias("BTC"), None);
        assert_eq!(normalize_coin("SKHYNIX"), "xyz:SKHX");
        assert_eq!(normalize_coin("WTIOIL"), "xyz:CL");
        assert_eq!(normalize_coin("BTC"), "BTC");
        // Env override wins over the built-in alias.
        unsafe { env::set_var("HYPERLIQUID_COIN_TESTONLYCOIN", "xyz:TOC") };
        assert_eq!(normalize_coin("TESTONLYCOIN"), "xyz:TOC");
        unsafe { env::remove_var("HYPERLIQUID_COIN_TESTONLYCOIN") };
    }

    #[test]
    fn splits_dex_prefixes_and_matches_coins() {
        assert_eq!(split_dex("xyz:SKHY"), Some("xyz".to_string()));
        assert_eq!(split_dex("BTC"), None);
        // Exchange responses may or may not carry the dex prefix.
        assert!(coin_matches("xyz:SKHY", "xyz:SKHY"));
        assert!(coin_matches("SKHY", "xyz:SKHY"));
        assert!(!coin_matches("SKHX", "xyz:SKHY"));
        assert!(!coin_matches("xyz:SKHX", "xyz:SKHY"));
    }

    #[test]
    fn rounds_prices_conservatively_per_side() {
        // Perp grid with sz_decimals = 2 → max 4 decimals, 5 sig figs.
        let tick = PriceTick::for_perp(2);
        // Bids round down, asks round up: never more aggressive than asked.
        assert_eq!(round_price(&tick, Side::Buy, 25.13579).unwrap(), d("25.135"));
        assert_eq!(round_price(&tick, Side::Sell, 25.13571).unwrap(), d("25.136"));
        // Already on-grid prices survive unchanged.
        assert_eq!(round_price(&tick, Side::Buy, 25.135).unwrap(), d("25.135"));
        // Invalid price.
        assert!(round_price(&tick, Side::Buy, -1.0).is_err());
    }

    #[test]
    fn truncates_sizes_to_market_decimals() {
        assert_eq!(round_size(2, 1.239).unwrap(), d("1.23"));
        assert_eq!(round_size(0, 7.9).unwrap(), d("7"));
        assert_eq!(round_size(4, 0.5).unwrap(), d("0.5"));
        // Sizes that vanish on the grid (or are non-positive) are refused.
        assert!(round_size(0, 0.4).is_err());
        assert!(round_size(2, 0.0).is_err());
        assert!(round_size(2, -3.0).is_err());
    }

    #[test]
    fn crossing_prices_cross_by_five_percent() {
        let tick = PriceTick::for_perp(2);
        // Buy: ask 20.0 * 1.05 = 21.0; sell: bid 20.0 * 0.95 = 19.0.
        assert_eq!(crossing_price(&tick, Side::Buy, Some(20.0)).unwrap(), d("21"));
        assert_eq!(crossing_price(&tick, Side::Sell, Some(20.0)).unwrap(), d("19"));
        // Off-grid results round conservatively (still crossing).
        assert_eq!(crossing_price(&tick, Side::Buy, Some(25.137)).unwrap(), d("26.393"));
        // Empty opposite side → error.
        assert!(crossing_price(&tick, Side::Buy, None).is_err());
    }

    #[test]
    fn maps_place_response_statuses() {
        let resting = active_from_status(
            Some(OrderResponseStatus::Resting { oid: 7, cloid: None }),
            Side::Buy,
            1.5,
            "xyz:SKHY",
        )
        .unwrap();
        assert_eq!(resting.order_id, 7);
        assert_eq!(resting.side, Side::Buy);
        assert_eq!(resting.qty, 1.5);

        let filled = active_from_status(
            Some(OrderResponseStatus::Filled { total_sz: d("1.5"), avg_px: d("25.1"), oid: 8 }),
            Side::Sell,
            1.5,
            "xyz:SKHY",
        )
        .unwrap();
        assert_eq!(filled.order_id, 8);

        let rejected = active_from_status(
            Some(OrderResponseStatus::Error("Order must have minimum value of $10.".into())),
            Side::Buy,
            0.1,
            "xyz:SKHY",
        )
        .unwrap_err();
        assert!(rejected.to_string().contains("minimum value"), "{rejected}");

        assert!(active_from_status(None, Side::Buy, 1.0, "xyz:SKHY").is_err());
    }

    #[test]
    fn classifies_modify_outcomes() {
        let coin = "xyz:SKHY";
        // Resting under a (possibly new) oid → still live.
        assert_eq!(
            classify_modify(Ok(Some(OrderResponseStatus::Resting { oid: 42, cloid: None })), coin).unwrap(),
            ModifyOutcome::Live(Some(42)),
        );
        // Filled during modify → gone (leg complete).
        assert_eq!(
            classify_modify(
                Ok(Some(OrderResponseStatus::Filled { total_sz: d("1"), avg_px: d("25"), oid: 42 })),
                coin
            )
            .unwrap(),
            ModifyOutcome::Gone,
        );
        // Order vanished underneath us → gone, whether reported in-band or as an error.
        for gone in [
            "Order 42 does not exist",
            "oid not found",
            "Order was never placed, already canceled, or filled.",
            "Cannot modify canceled or filled order",
        ] {
            assert_eq!(classify_modify(Ok(Some(OrderResponseStatus::Error(gone.into()))), coin).unwrap(), ModifyOutcome::Gone);
            assert_eq!(classify_modify(Err(gone.into()), coin).unwrap(), ModifyOutcome::Gone);
        }
        // "No change needed" transport errors keep the original order.
        assert_eq!(classify_modify(Err("No need to modify".into()), coin).unwrap(), ModifyOutcome::Live(None));
        // Real rejections propagate.
        assert!(classify_modify(Ok(Some(OrderResponseStatus::Error("Insufficient margin".into()))), coin).is_err());
        assert!(classify_modify(Err("transport timeout".into()), coin).is_err());
        assert!(classify_modify(Ok(None), coin).is_err());
    }

    /// `orderStatus`-style payloads (parsing belongs to the SDK; this covers
    /// OUR state/fill mapping).
    fn update(status: &str, sz: &str, orig: &str) -> OrderUpdate<BasicOrder> {
        serde_json::from_value(serde_json::json!({
            "status": status,
            "statusTimestamp": 1_753_200_000_000_u64,
            "order": {
                "coin": "xyz:SKHY",
                "side": "B",
                "limitPx": "25.135",
                "sz": sz,
                "oid": 123,
                "timestamp": 1_753_200_000_000_u64,
                "origSz": orig,
                "orderType": "Limit",
                "tif": "Gtc",
                "reduceOnly": false,
                "cloid": null
            }
        }))
        .expect("fixture parses")
    }

    #[test]
    fn maps_order_updates() {
        let open = order_info_from_update(&update("open", "1.5", "1.5")).unwrap();
        assert_eq!(open.state, OrderState::Open);
        assert_eq!(open.order_id, 123);
        assert_eq!(open.side, Side::Buy);
        assert_eq!(open.limit_px, 25.135);
        assert_eq!(open.remaining_qty, 1.5);
        assert_eq!(open.filled_qty, 0.0);

        let partial = order_info_from_update(&update("open", "0.6", "1.5")).unwrap();
        assert_eq!(partial.state, OrderState::Open);
        assert_eq!(partial.filled_qty, 0.9);

        let filled = order_info_from_update(&update("filled", "0.0", "1.5")).unwrap();
        assert_eq!(filled.state, OrderState::Filled);
        assert_eq!(filled.filled_qty, 1.5);

        // Canceled after a partial fill stays Canceled, not Filled.
        let canceled = order_info_from_update(&update("canceled", "0.6", "1.5")).unwrap();
        assert_eq!(canceled.state, OrderState::Canceled);
        assert_eq!(canceled.filled_qty, 0.9);

        let rejected = order_info_from_update(&update("minTradeNtlRejected", "1.5", "1.5")).unwrap();
        assert_eq!(rejected.state, OrderState::Rejected);
    }

    #[test]
    fn maps_positions_from_clearinghouse_state() {
        let positions: Vec<AssetPosition> = serde_json::from_value(serde_json::json!([{
            "type": "oneWay",
            "position": {
                "coin": "SKHY",
                "szi": "-12.5",
                "leverage": { "type": "cross", "value": 3 },
                "entryPx": "25.4",
                "positionValue": "317.5",
                "unrealizedPnl": "-1.25",
                "returnOnEquity": "-0.0118",
                "liquidationPx": "33.9",
                "marginUsed": "105.8",
                "maxLeverage": 3,
                "cumFunding": { "allTime": "0.12", "sinceOpen": "0.05", "sinceChange": "0.01" }
            }
        }]))
        .expect("fixture parses");

        // HIP-3 positions come back without the dex prefix.
        let snapshot = snapshot_from_positions(&positions, "xyz:SKHY").unwrap();
        assert_eq!(snapshot.size, -12.5);
        assert_eq!(snapshot.position_value, 317.5);
        assert_eq!(snapshot.unrealized_pnl, -1.25);
        assert_eq!(snapshot.return_on_equity, -0.0118);
        assert_eq!(snapshot.entry_px, Some(25.4));

        // No position on the coin → zeroed snapshot.
        let flat = snapshot_from_positions(&positions, "xyz:SKHX").unwrap();
        assert_eq!(flat.size, 0.0);
        assert_eq!(flat.entry_px, None);
    }

    #[test]
    fn converts_l2_book_snapshots() {
        let book: L2Book = serde_json::from_value(serde_json::json!({
            "coin": "xyz:SKHY",
            "time": 1_753_200_000_000_u64,
            "levels": [
                [
                    { "px": "25.132", "sz": "10.0", "n": 1 },
                    { "px": "25.101", "sz": "42.0", "n": 2 }
                ],
                [
                    { "px": "25.150", "sz": "7.5", "n": 1 },
                    { "px": "25.180", "sz": "30.0", "n": 1 }
                ]
            ]
        }))
        .expect("fixture parses");
        let (bids, asks) = sdk_book_levels(&book).unwrap();
        assert_eq!(bids, vec![(25.132, 10.0), (25.101, 42.0)]);
        assert_eq!(asks, vec![(25.15, 7.5), (25.18, 30.0)]);
        // Streamed into a managed book: best-first with top-of-book mid.
        let book = OrderBook::from_sides(bids, asks);
        assert_eq!(book.best_bid(), Some(25.132));
        assert_eq!(book.best_ask(), Some(25.15));
        assert_eq!(book.mid(), Some((25.132 + 25.15) / 2.0));
    }
}
