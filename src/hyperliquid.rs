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

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    env,
    str::FromStr,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use anyhow::{Context, bail};
use either::Either;
use futures_util::StreamExt;
use hypersdk::{
    Address,
    hypercore::{
        self, HttpClient, NonceHandler, PriceTick, PrivateKeySigner,
        types::{
            AbstractionMode, AssetPosition, BasicOrder, BatchCancel, BatchModify, BatchOrder,
            Cancel, Fill, Incoming, L2Book, Modify, OrderGrouping, OrderRequest, OrderResponseStatus,
            OrderStatus as HlOrderStatus, OrderTypePlacement, OrderUpdate, Side as BookSide,
            Subscription, TimeInForce, WsBasicOrder,
        },
        ws::Event,
    },
};
use rust_decimal::Decimal;
use tokio::{
    sync::{Mutex, mpsc},
    time::interval,
};

use crate::book::{OrderBook, levels_to_map, overlay_source};
use crate::order::{ActiveOrder, OrderInfo, OrderState, Side};

/// Multiplicative slippage bound for synthetic market orders (cross the
/// opposite top of book by 5%).
const MARKET_ORDER_SLIPPAGE: f64 = 0.05;

/// Venue-wide minimum order value in USDC ("Order must have minimum value
/// of $10."). Enforced client-side for non-reduce-only orders.
pub const MIN_NOTIONAL_USDC: f64 = 10.0;

/// Perp price grids allow at most `6 - sz_decimals` decimal places (and 5
/// significant figures, handled by [`PriceTick`]).
const PERP_MAX_PX_DECIMALS: i64 = 6;

/// `orderStatus` is eventually consistent: poll this many times, this far
/// apart, before concluding an oid is unknown.
const FETCH_ATTEMPTS: u32 = 5;
const FETCH_DELAY: Duration = Duration::from_millis(200);

const IP_WEIGHT_LIMIT: u32 = 1_200;
const EXCHANGE_WEIGHT_RESERVE: u32 = 100;
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(60);
const ADDRESS_ACTION_INTERVAL: Duration = Duration::from_secs(10);
const MAX_USER_FILLS_ITEMS: usize = 2_000;
/// How long an account's balance-source mode is trusted before it is re-read.
/// See [`Hyperliquid::abstraction_mode_cached`].
const ABSTRACTION_MODE_TTL: Duration = Duration::from_secs(300);

#[derive(Default)]
struct WeightWindow {
    used: u32,
    entries: VecDeque<(Instant, u32)>,
}

struct RateLimits {
    weights: Mutex<WeightWindow>,
    address_action_next: Mutex<Option<Instant>>,
}

impl RateLimits {
    fn new() -> Self {
        Self {
            weights: Mutex::new(WeightWindow::default()),
            address_action_next: Mutex::new(None),
        }
    }

    async fn acquire(&self, weight: u32, exchange_action: bool) {
        let limit = if exchange_action {
            IP_WEIGHT_LIMIT
        } else {
            IP_WEIGHT_LIMIT - EXCHANGE_WEIGHT_RESERVE
        };
        loop {
            let wait = {
                let now = Instant::now();
                let mut window = self.weights.lock().await;
                while window
                    .entries
                    .front()
                    .is_some_and(|(at, _)| now.duration_since(*at) >= RATE_LIMIT_WINDOW)
                {
                    let (_, expired) = window.entries.pop_front().expect("front exists");
                    window.used = window.used.saturating_sub(expired);
                }
                if window.used.saturating_add(weight) <= limit {
                    window.used += weight;
                    window.entries.push_back((now, weight));
                    return;
                }
                window
                    .entries
                    .front()
                    .map(|(at, _)| (*at + RATE_LIMIT_WINDOW).saturating_duration_since(now))
                    .unwrap_or(RATE_LIMIT_WINDOW)
            };
            tokio::time::sleep(wait).await;
        }
    }

    async fn try_acquire(&self, weight: u32, limit: u32) -> bool {
        let now = Instant::now();
        let mut window = self.weights.lock().await;
        while window
            .entries
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) >= RATE_LIMIT_WINDOW)
        {
            let (_, expired) = window.entries.pop_front().expect("front exists");
            window.used = window.used.saturating_sub(expired);
        }
        if window.used.saturating_add(weight) > limit {
            return false;
        }
        window.used += weight;
        window.entries.push_back((now, weight));
        true
    }

    async fn wait_for_address_action(&self, cancel: bool) {
        if cancel {
            return;
        }
        loop {
            let wait = {
                let mut next = self.address_action_next.lock().await;
                let Some(at) = *next else {
                    return;
                };
                let now = Instant::now();
                if now >= at {
                    *next = Some(now + ADDRESS_ACTION_INTERVAL);
                    return;
                }
                at.duration_since(now)
            };
            tokio::time::sleep(wait).await;
        }
    }

    async fn address_action_ready(&self) -> bool {
        self.address_action_next
            .lock()
            .await
            .is_none_or(|at| Instant::now() >= at)
    }

    async fn try_claim_address_action(&self) -> bool {
        let mut next = self.address_action_next.lock().await;
        let Some(at) = *next else { return true };
        let now = Instant::now();
        if now < at {
            return false;
        }
        *next = Some(now + ADDRESS_ACTION_INTERVAL);
        true
    }

    async fn record_address_rate_limit(&self) {
        let mut next = self.address_action_next.lock().await;
        let paced = Instant::now() + ADDRESS_ACTION_INTERVAL;
        if next.is_none_or(|current| current < paced) {
            *next = Some(paced);
        }
    }
}

fn rate_limits() -> &'static RateLimits {
    static LIMITS: OnceLock<RateLimits> = OnceLock::new();
    LIMITS.get_or_init(RateLimits::new)
}

async fn acquire_info(weight: u32) {
    rate_limits().acquire(weight, false).await;
}

async fn acquire_action(cancel: bool, batch_len: usize) {
    rate_limits()
        .acquire(exchange_action_weight(batch_len), cancel)
        .await;
    rate_limits().wait_for_address_action(cancel).await;
}

async fn try_acquire_modify(batch_len: usize) -> bool {
    if !rate_limits().address_action_ready().await {
        return false;
    }
    if !rate_limits()
        .try_acquire(
            exchange_action_weight(batch_len),
            IP_WEIGHT_LIMIT - EXCHANGE_WEIGHT_RESERVE,
        )
        .await
    {
        return false;
    }
    rate_limits().try_claim_address_action().await
}

async fn record_action_error(message: &str) {
    if is_rate_limit_message(message) {
        rate_limits().record_address_rate_limit().await;
    }
}

fn exchange_action_weight(batch_len: usize) -> u32 {
    1 + (batch_len as u32 / 40)
}

fn response_weight(base: u32, items: usize, items_per_weight: usize) -> u32 {
    base + items.div_ceil(items_per_weight) as u32
}

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

/// Where the wall time of one signed action went.
///
/// The three phases have nothing in common except that they are serial, and
/// each has a different remedy: `gate` is a limit this process imposed on
/// itself and can be re-tuned, `sign` is local CPU, and `http` belongs to the
/// venue and can only be attacked by removing the request or moving closer to
/// it. Reporting them as one number invites optimising the wrong one.
#[derive(Clone, Copy, Debug, Default)]
pub struct SubmitTiming {
    /// Blocked in the client-side rate limiter.
    pub gate: Duration,
    /// msgpack + keccak256 + EIP-712 + secp256k1, on the calling task.
    pub sign: Duration,
    /// The `/exchange` request and response, including any TLS handshake.
    pub http: Duration,
}

/// Account-level margin snapshot from `clearinghouseState` (scoped to this
/// market's dex for HIP-3 markets — margin is segregated per dex).
#[derive(Clone, Copy, Debug, Default)]
pub struct AccountSummary {
    /// Total account equity in USDC.
    pub account_value: f64,
    /// Margin currently locked by open positions/orders.
    pub margin_used: f64,
    /// USDC available for withdrawal — the practical ceiling for new
    /// position margin.
    pub withdrawable: f64,
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
    /// Maximum allowed leverage for this market.
    max_leverage: u64,
    /// Valid price increments for this market.
    tick: PriceTick,
}

/// Public per-market trading constraints (price/size grids and order-value
/// bounds), as returned by [`fetch_market`] / [`Hyperliquid::market_info`].
#[derive(Clone, Debug)]
pub struct MarketInfo {
    /// Full coin name as the exchange knows it (e.g. `xyz:SKHY`).
    pub coin: String,
    /// Decimal places allowed in order sizes (lot grid).
    pub sz_decimals: i64,
    /// Maximum decimal places allowed in prices (`6 - sz_decimals` for
    /// perps; ticks additionally cap at 5 significant figures).
    pub px_decimals: i64,
    /// Minimum order value in USDC ([`MIN_NOTIONAL_USDC`], venue-wide).
    pub min_notional: f64,
    /// Maximum allowed leverage — the venue has no per-order max size;
    /// position size is bounded by margin × leverage.
    pub max_leverage: u64,
}

impl MarketInfo {
    /// Smallest size whose notional at `price` clears the minimum order
    /// value (not yet rounded onto the size grid).
    pub fn min_qty(&self, price: f64) -> f64 {
        self.min_notional / price
    }
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
                let key = if key.starts_with("0x") {
                    key
                } else {
                    format!("0x{key}")
                };
                Some(
                    key.parse::<PrivateKeySigner>()
                        .context("PRIVATE_KEY is not a valid hex key")?,
                )
            }
            false => None,
        };
        let market = resolve_market(&client, &normalize_coin(coin)).await?;
        Ok(Self {
            client,
            user,
            signer,
            nonces: NonceHandler::default(),
            market,
        })
    }

    /// Full market name this capsule trades (e.g. `xyz:SKHY`).
    pub fn coin(&self) -> &str {
        &self.market.coin
    }

    /// Does an exchange-reported coin name refer to this capsule's market?
    /// (Order/fill/position payloads may drop the HIP-3 `dex:` prefix.)
    pub fn is_market_coin(&self, candidate: &str) -> bool {
        coin_matches(candidate, &self.market.coin)
    }

    /// This market's trading constraints (cached from connect time; use
    /// [`fetch_market`] for a credential-free standalone lookup).
    pub fn market_info(&self) -> MarketInfo {
        market_info(&self.market)
    }

    fn signer(&self) -> anyhow::Result<&PrivateKeySigner> {
        self.signer.as_ref().with_context(|| {
            format!(
                "Hyperliquid {} is read-only: connect with connect_trading (PRIVATE_KEY)",
                self.market.coin
            )
        })
    }

    /// REST L2 book snapshot.
    pub async fn book(&self) -> anyhow::Result<OrderBook> {
        acquire_info(2).await;
        let book = self
            .client
            .l2_book(self.market.coin.clone(), None, None)
            .await?;
        let (bids, asks) = sdk_book_levels(&book)?;
        Ok(OrderBook::from_sides(bids, asks))
    }

    /// Mid of the current top of book.
    pub async fn mid_price(&self) -> anyhow::Result<f64> {
        self.book().await?.mid().with_context(|| {
            format!(
                "Hyperliquid {} book is one-sided or empty",
                self.market.coin
            )
        })
    }

    /// Place a GTC limit order. Price is rounded onto the market tick grid
    /// (conservatively: bids down, asks up) and size truncated onto the
    /// market's size decimals.
    pub async fn place_limit(
        &self,
        side: Side,
        qty: f64,
        price: f64,
        reduce_only: bool,
    ) -> anyhow::Result<ActiveOrder> {
        let px = round_price(&self.market.tick, side, price)?;
        let sz = round_size(self.market.sz_decimals, qty)?;
        Ok(self
            .submit(side, sz, px, reduce_only, TimeInForce::Gtc)
            .await?
            .0)
    }

    /// Place an add-liquidity-only limit order at the supplied BBO price.
    pub async fn place_post_only(
        &self,
        side: Side,
        qty: f64,
        price: f64,
        reduce_only: bool,
    ) -> anyhow::Result<ActiveOrder> {
        Ok(self
            .place_post_only_timed(side, qty, price, reduce_only)
            .await?
            .0)
    }

    /// As [`Hyperliquid::place_post_only`], also reporting where the wall time
    /// went.
    ///
    /// A single number for "the order took 412 ms" is not actionable: a rate
    /// gate we imposed, a signature we computed and a round trip the venue
    /// owns have completely different remedies, and only the split
    /// distinguishes them.
    pub async fn place_post_only_timed(
        &self,
        side: Side,
        qty: f64,
        price: f64,
        reduce_only: bool,
    ) -> anyhow::Result<(ActiveOrder, SubmitTiming)> {
        let px = round_price(&self.market.tick, side, price)?;
        let sz = round_size(self.market.sz_decimals, qty)?;
        self.submit(side, sz, px, reduce_only, TimeInForce::Alo)
            .await
    }

    /// Synthetic market order: IOC limit crossing the opposite top of book
    /// by [`MARKET_ORDER_SLIPPAGE`]. The unfilled remainder (if any) is
    /// canceled by the venue, never rests.
    ///
    /// Fetches the book over REST to price the cross, which puts a full round
    /// trip in front of the order. Callers that already stream the book should
    /// use [`Hyperliquid::place_market_at`] instead.
    pub async fn place_market(
        &self,
        side: Side,
        qty: f64,
        reduce_only: bool,
    ) -> anyhow::Result<ActiveOrder> {
        let book = self.book().await?;
        let opposite_top = match side {
            Side::Buy => book.best_ask(),
            Side::Sell => book.best_bid(),
        };
        Ok(self
            .market_at(side, qty, reduce_only, opposite_top)
            .await?
            .0)
    }

    /// Synthetic market order priced from a top of book the caller already
    /// has, crossing it by [`MARKET_ORDER_SLIPPAGE`].
    ///
    /// [`Hyperliquid::place_market`] cannot price the cross without first
    /// fetching the book, and on this venue that fetch is a ~50-90 ms round
    /// trip sitting *in front of* the order — on a hedge leg that is the
    /// dominant term in the whole fill-to-hedge path. A caller running a book
    /// stream already holds a price that is fresher than the one that fetch
    /// would return, so passing it in removes the round trip outright instead
    /// of merely overlapping it.
    ///
    /// `opposite_top` is the price the order has to cross: the best ask for a
    /// buy, the best bid for a sell. The 5% cross means a stale-by-a-tick
    /// input still fills; it only has to be on the right side of the book.
    pub async fn place_market_at(
        &self,
        side: Side,
        qty: f64,
        reduce_only: bool,
        opposite_top: f64,
    ) -> anyhow::Result<ActiveOrder> {
        Ok(self
            .place_market_at_timed(side, qty, reduce_only, opposite_top)
            .await?
            .0)
    }

    /// As [`Hyperliquid::place_market_at`], also reporting where the wall time
    /// went. See [`Hyperliquid::place_post_only_timed`].
    pub async fn place_market_at_timed(
        &self,
        side: Side,
        qty: f64,
        reduce_only: bool,
        opposite_top: f64,
    ) -> anyhow::Result<(ActiveOrder, SubmitTiming)> {
        self.market_at(side, qty, reduce_only, Some(opposite_top))
            .await
    }

    async fn market_at(
        &self,
        side: Side,
        qty: f64,
        reduce_only: bool,
        opposite_top: Option<f64>,
    ) -> anyhow::Result<(ActiveOrder, SubmitTiming)> {
        let px = crossing_price(&self.market.tick, side, opposite_top)?;
        let sz = round_size(self.market.sz_decimals, qty)?;
        self.submit(side, sz, px, reduce_only, TimeInForce::Ioc)
            .await
    }

    async fn submit(
        &self,
        side: Side,
        sz: Decimal,
        px: Decimal,
        reduce_only: bool,
        tif: TimeInForce,
    ) -> anyhow::Result<(ActiveOrder, SubmitTiming)> {
        check_min_notional(&self.market.coin, sz, px, reduce_only)?;
        let batch = BatchOrder {
            orders: vec![self.order_request(side, sz, px, reduce_only, tif)],
            grouping: OrderGrouping::Na,
            builder: None,
        };
        let started = Instant::now();
        acquire_action(false, 1).await;
        let gated = Instant::now();
        // `place` signs synchronously and returns the request future, so the
        // signature cost lands here rather than inside the await. That is what
        // makes the three phases separable at all — and it is a property of
        // the SDK's design, not an accident, so it is asserted by construction
        // rather than assumed: anything the future does is network time.
        let request = self
            .client
            .place(self.signer()?, batch, self.nonces.next(), None, None);
        let signed = Instant::now();
        let result = request.await;
        let timing = SubmitTiming {
            gate: gated.duration_since(started),
            sign: signed.duration_since(gated),
            http: signed.elapsed(),
        };
        if let Err(err) = &result {
            record_action_error(err.message()).await;
        }
        let statuses = result.map_err(|err| {
            anyhow::anyhow!(
                "Hyperliquid {} place failed: {}",
                self.market.coin,
                err.message()
            )
        })?;
        if let Some(OrderResponseStatus::Error(message)) = statuses.first() {
            record_action_error(message).await;
        }
        let order = active_from_status(
            statuses.into_iter().next(),
            side,
            decimal_to_f64(sz)?,
            &self.market.coin,
        )?;
        Ok((order, timing))
    }

    /// Re-price a live order with one native atomic `batchModify` action.
    /// Ambiguous race outcomes are resolved by the private `orderUpdates`
    /// stream rather than weighted `orderStatus` polling.
    pub async fn modify_order(
        &self,
        order: &ActiveOrder,
        price: f64,
        reduce_only: bool,
    ) -> anyhow::Result<ModifyOrderOutcome> {
        if order.order_id == 0 {
            return Ok(ModifyOrderOutcome::AwaitingOrderUpdate);
        }
        let px = round_price(&self.market.tick, order.side, price)?;
        let sz = round_size(self.market.sz_decimals, order.qty)?;
        if !try_acquire_modify(1).await {
            return Ok(ModifyOrderOutcome::Deferred);
        }
        let result = self
            .client
            .modify(
                self.signer()?,
                BatchModify {
                    // Do not place a replacement if the original filled while
                    // this cancel-and-replace request was in flight.
                    always_place: false,
                    modifies: vec![Modify {
                        oid: Either::Left(order.order_id),
                        order: self.order_request(
                            order.side,
                            sz,
                            px,
                            reduce_only,
                            TimeInForce::Alo,
                        ),
                    }],
                },
                self.nonces.next(),
                None,
                None,
            )
            .await;
        if let Err(err) = &result {
            record_action_error(err.message()).await;
        }
        let outcome = result
            .map(|statuses| statuses.into_iter().next())
            .map_err(|err| err.message().to_string());
        if let Ok(Some(OrderResponseStatus::Error(message))) = &outcome {
            record_action_error(message).await;
        }
        match classify_modify(outcome, &self.market.coin)? {
            ModifyOutcome::AwaitingOrderUpdate => Ok(ModifyOrderOutcome::AwaitingOrderUpdate),
            ModifyOutcome::Ambiguous(message) => Ok(ModifyOrderOutcome::Ambiguous(message)),
            ModifyOutcome::Live(oid) => Ok(ModifyOrderOutcome::Live(ActiveOrder {
                order_id: oid.unwrap_or(order.order_id),
                side: order.side,
                qty: order.qty,
            })),
        }
    }

    /// Cancel an order by oid. Cancels race fills: by the time the cancel
    /// lands the order may already be filled (or canceled), which the venue
    /// reports as an "already canceled or filled" rejection — that is a
    /// normal outcome ([`CancelOutcome::AlreadyGone`]), not an error, and
    /// callers must check fills before assuming nothing executed
    /// ([`Hyperliquid::fetch_order`] disambiguates).
    pub async fn cancel_order(&self, order_id: u64) -> anyhow::Result<CancelOutcome> {
        acquire_action(true, 1).await;
        let result = self
            .client
            .cancel(
                self.signer()?,
                BatchCancel {
                    cancels: vec![Cancel {
                        asset: self.market.asset,
                        oid: order_id,
                    }],
                },
                self.nonces.next(),
                None,
                None,
            )
            .await;
        if let Err(err) = &result {
            record_action_error(err.message()).await;
        }
        let outcome = result
            .map(|statuses| statuses.into_iter().next())
            .map_err(|err| err.message().to_string());
        if let Ok(Some(OrderResponseStatus::Error(message))) = &outcome {
            record_action_error(message).await;
        }
        classify_cancel(outcome, &self.market.coin)
    }

    /// `orderStatus` lookup by oid — works for closed orders too.
    /// `Ok(None)` when the exchange has never seen the id (polled briefly:
    /// the endpoint is eventually consistent right after a place).
    pub async fn fetch_order(&self, order_id: u64) -> anyhow::Result<Option<OrderInfo>> {
        for attempt in 0..FETCH_ATTEMPTS {
            if let Some(info) = self.fetch_order_once(order_id).await? {
                return Ok(Some(info));
            }
            if attempt + 1 < FETCH_ATTEMPTS {
                tokio::time::sleep(FETCH_DELAY).await;
            }
        }
        Ok(None)
    }

    /// One order-status request without the eventual-consistency retry loop.
    pub async fn fetch_order_once(&self, order_id: u64) -> anyhow::Result<Option<OrderInfo>> {
        acquire_info(2).await;
        self.client
            .order_status(self.user, Either::Left(order_id))
            .await?
            .map(|update| order_info_from_update(&update))
            .transpose()
    }

    /// Open orders on this capsule's market (`frontendOpenOrders` scan).
    pub async fn open_orders(&self) -> anyhow::Result<Vec<OrderInfo>> {
        acquire_info(20).await;
        let orders = self
            .client
            .open_orders(self.user, self.market.dex_name.clone())
            .await?;
        orders
            .iter()
            .filter(|order| coin_matches(&order.coin, &self.market.coin))
            .map(order_info_from_basic)
            .collect()
    }

    /// Recent account-wide fills from REST. Trade IDs match `userFills` WS,
    /// so callers can reconcile delayed events without double-counting.
    pub async fn recent_fills(&self) -> anyhow::Result<Vec<FillEvent>> {
        // `userFills` has base weight 20 plus one per 20 returned items. It
        // returns at most 2,000 fills, so reserve its worst-case 120 before
        // issuing the request rather than discovering response weight after
        // the IP budget has already been consumed.
        acquire_info(response_weight(20, MAX_USER_FILLS_ITEMS, 20)).await;
        let fills = self.client.user_fills(self.user).await?;
        // These arrived by REST, not over the private socket, but the stamp
        // still answers the question it exists to answer: when did this
        // process first learn about the fill.
        let recv_at = Instant::now();
        fills
            .iter()
            .map(|fill| fill_event(fill, recv_at))
            .collect()
    }

    /// Perp position on this capsule's market (zeroed default when flat).
    pub async fn position_snapshot(&self) -> anyhow::Result<PositionSnapshot> {
        acquire_info(2).await;
        let state = self
            .client
            .clearinghouse_state(self.user, self.market.dex_name.clone())
            .await?;
        snapshot_from_positions(&state.asset_positions, &self.market.coin)
    }

    /// Signed position size (long > 0, short < 0, flat = 0).
    pub async fn position_amt(&self) -> anyhow::Result<f64> {
        Ok(self.position_snapshot().await?.size)
    }

    /// Account margin summary (equity, margin used, withdrawable).
    ///
    /// Unified Account and Portfolio Margin expose the trading balance via
    /// `spotClearinghouseState`; the per-DEX `clearinghouseState` is not a
    /// meaningful balance source in those modes. Standard accounts continue
    /// to use the market's DEX-scoped clearinghouse state.
    pub async fn account_summary(&self) -> anyhow::Result<AccountSummary> {
        let abstraction = self.abstraction_mode_cached().await?;
        if abstraction.is_unified_account() || abstraction.is_portfolio_margin() {
            acquire_info(2).await;
            let available = self
                .client
                .user_balances(self.user)
                .await?
                .into_iter()
                .find(|balance| balance.coin == "USDC")
                .map(|balance| decimal_to_f64((balance.total - balance.hold).max(Decimal::ZERO)))
                .transpose()?
                .unwrap_or(0.0);
            return Ok(AccountSummary {
                account_value: available,
                margin_used: 0.0,
                withdrawable: available,
            });
        }

        acquire_info(2).await;
        let state = self
            .client
            .clearinghouse_state(self.user, self.market.dex_name.clone())
            .await?;
        Ok(AccountSummary {
            account_value: decimal_to_f64(state.margin_summary.account_value)?,
            margin_used: decimal_to_f64(state.margin_summary.total_margin_used)?,
            withdrawable: decimal_to_f64(state.withdrawable)?,
        })
    }

    /// Which balance source this account uses, re-read at most once per
    /// [`ABSTRACTION_MODE_TTL`].
    ///
    /// [`Hyperliquid::account_summary`] needs this before it knows which
    /// endpoint holds the balance, so an uncached read makes the margin
    /// pre-check two *sequential* round trips — and that pre-check sits
    /// directly in front of an order. The mode is an account setting that
    /// changes at human timescales, if ever, so re-reading it per order buys
    /// nothing. The TTL is what keeps "if ever" from meaning "never": a mode
    /// changed in the UI is picked up without a restart, and until then the
    /// venue's own margin check remains the authority that actually matters.
    async fn abstraction_mode_cached(&self) -> anyhow::Result<AbstractionMode> {
        static CACHE: OnceLock<Mutex<HashMap<Address, (Instant, AbstractionMode)>>> =
            OnceLock::new();
        let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
        if let Some((at, mode)) = cache.lock().await.get(&self.user)
            && at.elapsed() < ABSTRACTION_MODE_TTL
        {
            return Ok(*mode);
        }
        acquire_info(20).await;
        let mode = self.client.abstraction_mode(self.user).await?;
        cache.lock().await.insert(self.user, (Instant::now(), mode));
        Ok(mode)
    }

    fn order_request(
        &self,
        side: Side,
        sz: Decimal,
        px: Decimal,
        reduce_only: bool,
        tif: TimeInForce,
    ) -> OrderRequest {
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
        acquire_info(20).await;
        let dex = client
            .perp_dexes()
            .await?
            .into_iter()
            .find(|dex| dex.name() == dex_name)
            .with_context(|| format!("Hyperliquid perp DEX not found: {dex_name}"))?;
        acquire_info(20).await;
        client.perps_from(dex).await?
    } else {
        acquire_info(20).await;
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
        max_leverage: market.max_leverage,
        tick: market.table,
    })
}

/// Fetch a market's trading constraints (price/size decimals, min order
/// value, max leverage) without credentials. Honors the same coin aliases
/// (`HYPERLIQUID_COIN_<NAME>`, built-ins) as the capsule constructors.
pub async fn fetch_market(coin: &str) -> anyhow::Result<MarketInfo> {
    crate::load_dotenv();
    let client = hypercore::mainnet();
    let spec = resolve_market(&client, &normalize_coin(coin)).await?;
    Ok(market_info(&spec))
}

/// Hourly funding observation from `fundingHistory`.
///
/// `funding_rate` and `premium` are fractions of notional per hour, not
/// percent: `0.0000125` is the 0.00125 %/hr baseline.
#[derive(Clone, Copy, Debug, Default)]
pub struct FundingPoint {
    /// Funding payment timestamp, unix milliseconds.
    pub time_ms: u64,
    /// Realised hourly funding rate (fraction of notional).
    pub funding_rate: f64,
    /// Mark-vs-oracle premium component of that rate.
    pub premium: f64,
}

/// Hard stop on pagination so a venue that keeps handing back records without
/// advancing `time` cannot spin here forever. 200 pages ≈ 100k hourly points
/// ≈ 11 years, well past any window a caller has a use for; exceeding it is
/// reported as an error rather than as a silently truncated series.
const FUNDING_HISTORY_MAX_PAGES: usize = 200;

/// Historical hourly funding rates for a market, oldest first.
///
/// Credential-free like [`fetch_market`], and honors the same coin aliases
/// (`HYPERLIQUID_COIN_<NAME>`, built-ins), so `"SKHYNIX"` resolves to the
/// HIP-3 market `xyz:SKHX`. No market resolution round trip is needed —
/// `fundingHistory` keys off the coin name directly.
///
/// `start_ms`/`end_ms` are unix milliseconds and both inclusive; `end_ms`
/// defaults to now. The venue caps a response at 500 records, so longer
/// windows are paginated here and returned as one series.
///
/// An unresolvable coin is rejected by the venue (HTTP 500) rather than
/// answered with an empty series, so a mistyped market surfaces as an error.
/// An empty result therefore means the market simply paid no funding in the
/// window — e.g. it listed after `start_ms`.
pub async fn fetch_funding_history(
    coin: &str,
    start_ms: u64,
    end_ms: Option<u64>,
) -> anyhow::Result<Vec<FundingPoint>> {
    crate::load_dotenv();
    let client = hypercore::mainnet();
    let coin = normalize_coin(coin);
    let mut points: Vec<FundingPoint> = Vec::new();
    let mut cursor = start_ms;
    for _ in 0..FUNDING_HISTORY_MAX_PAGES {
        acquire_info(20).await;
        let page = client
            .funding_history(coin.clone(), cursor, end_ms)
            .await
            .with_context(|| format!("Hyperliquid funding history for {coin}"))?;
        let last_time = page.last().map(|rate| rate.time);
        // Records arrive ascending. The cursor is inclusive, so the first
        // record of every page after the first repeats the last record of the
        // previous one; anything not newer than what we already hold is that
        // duplicate (or, if the venue ever answered out of order, a record we
        // deliberately drop rather than splice into the middle of the series).
        for rate in page {
            if points.last().is_some_and(|last| last.time_ms >= rate.time) {
                continue;
            }
            points.push(FundingPoint {
                time_ms: rate.time,
                funding_rate: decimal_to_f64(rate.funding_rate)?,
                premium: decimal_to_f64(rate.premium)?,
            });
        }
        // Advancing the cursor is the only sound termination test: a page
        // whose last record is no newer than the cursor has nothing left to
        // give. Deliberately *not* "was the page full", which would make
        // correctness depend on the venue's 500-record cap staying 500 — if it
        // ever shrank, that test would end the loop early and return a
        // truncated series as `Ok`. The cost is one extra request per call.
        match last_time {
            Some(time) if time > cursor => cursor = time,
            _ => return Ok(points),
        }
    }
    bail!("Hyperliquid funding history for {coin} did not terminate within {FUNDING_HISTORY_MAX_PAGES} pages")
}

/// Derive the public [`MarketInfo`] view of a resolved market.
fn market_info(spec: &MarketSpec) -> MarketInfo {
    MarketInfo {
        coin: spec.coin.clone(),
        sz_decimals: spec.sz_decimals,
        px_decimals: (PERP_MAX_PX_DECIMALS - spec.sz_decimals).max(0),
        min_notional: MIN_NOTIONAL_USDC,
        max_leverage: spec.max_leverage,
    }
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
        Self {
            slow_overlay: false,
            rest_refresh: None,
            rest_gate: None,
            depth: 50,
        }
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
pub async fn stream_book(
    coin: String,
    config: BookStreamConfig,
    tx: mpsc::Sender<OrderBook>,
) -> anyhow::Result<()> {
    let coin = normalize_coin(&coin);
    let client = hypercore::mainnet();
    let mut fast_ws = hypercore::mainnet_ws();
    fast_ws.subscribe(Subscription::L2Book {
        coin: coin.clone(),
        n_sig_figs: None,
        mantissa: None,
        fast: true,
    });
    let mut slow_ws = config.slow_overlay.then(|| {
        let ws = hypercore::mainnet_ws();
        ws.subscribe(Subscription::L2Book {
            coin: coin.clone(),
            n_sig_figs: None,
            mantissa: None,
            fast: false,
        });
        ws
    });
    let mut rest_tick = config.rest_refresh.map(interval);

    let mut rest_book = SourceOverlay::default();
    let mut slow_book = SourceOverlay::default();
    let mut fast_book = SourceOverlay::default();

    if config.rest_allowed() {
        acquire_info(2).await;
        if let Ok(book) = client.l2_book(coin.clone(), None, None).await
            && let Ok((bids, asks)) = sdk_book_levels(&book)
        {
            rest_book.set(bids, asks);
            if send_combined(&tx, &rest_book, &slow_book, &fast_book, config.depth)
                .await
                .is_err()
            {
                return Ok(());
            }
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
                if config.rest_allowed() {
                    acquire_info(2).await;
                    if let Ok(book) = client.l2_book(coin.clone(), None, None).await
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
        bids.iter()
            .rev()
            .take(depth)
            .map(|(price, qty)| (crate::book::key_to_price(*price), *qty))
            .collect(),
        asks.iter()
            .take(depth)
            .map(|(price, qty)| (crate::book::key_to_price(*price), *qty))
            .collect(),
    );
    tx.send(book).await.map_err(|_| ())
}

// ---------------------------------------------------------------------------
// Private account streaming (userFills + orderUpdates WebSocket)
// ---------------------------------------------------------------------------

/// One of the account's fills, normalized from the `userFills` WS feed.
#[derive(Clone, Debug)]
pub struct FillEvent {
    /// Coin as reported by the exchange — HIP-3 fills may omit the `dex:`
    /// prefix, so match with [`Hyperliquid::is_market_coin`].
    pub coin: String,
    pub side: Side,
    pub px: f64,
    pub sz: f64,
    /// Order id the fill executed against.
    pub oid: u64,
    /// Unique trade identifier, used to deduplicate reconnect/retry events.
    pub tid: u64,
    /// True when the fill took liquidity (taker).
    pub crossed: bool,
    /// Exchange timestamp (ms).
    pub time: u64,
    /// Monotonic instant this fill was decoded off the private WebSocket.
    ///
    /// [`FillEvent::time`] is the venue's clock and cannot be differenced
    /// against a local one; this is the local arrival stamp, taken before the
    /// event is queued, so a consumer can separate "the venue was slow to
    /// tell us" from "we were slow to act on being told".
    pub recv_at: Instant,
}

/// One order lifecycle update from the private `orderUpdates` feed.
#[derive(Clone, Debug)]
pub struct OrderUpdateEvent {
    pub coin: String,
    pub info: OrderInfo,
    pub status_timestamp: u64,
    /// Monotonic instant this update was decoded off the private WebSocket.
    /// See [`FillEvent::recv_at`].
    pub recv_at: Instant,
}

#[derive(Clone, Debug)]
pub enum PrivateEvent {
    Fill(FillEvent),
    OrderUpdate(OrderUpdateEvent),
    Connected,
    Disconnected,
}

/// Stream account-wide fills and order lifecycle updates over one private
/// WebSocket. Fills remain the quantity authority; updates acknowledge
/// placement, modification, cancellation, and terminal order state.
pub async fn stream_user_events(tx: mpsc::Sender<PrivateEvent>) -> anyhow::Result<()> {
    crate::load_dotenv();
    let user: Address = env::var("WALLET_ADDRESS")
        .context("WALLET_ADDRESS is required")?
        .parse()
        .context("WALLET_ADDRESS is not a valid address")?;
    let mut ws = hypercore::mainnet_ws();
    ws.subscribe(Subscription::UserFills { user });
    ws.subscribe(Subscription::OrderUpdates { user });
    let mut fills_ready = false;
    let mut orders_ready = false;
    loop {
        let event = tokio::select! {
            _ = tx.closed() => return Ok(()),
            event = ws.next() => event,
        };
        let Some(event) = event else {
            return Ok(());
        };
        match event {
            Event::Connected => {
                fills_ready = false;
                orders_ready = false;
            }
            Event::Disconnected if tx.send(PrivateEvent::Disconnected).await.is_err() => {
                return Ok(());
            }
            Event::Disconnected => {}
            Event::Message(Incoming::SubscriptionResponse(
                hypersdk::hypercore::types::Outgoing::Subscribe { subscription },
            )) => {
                match subscription {
                    Subscription::UserFills { user: subscribed } if subscribed == user => {
                        fills_ready = true
                    }
                    Subscription::OrderUpdates { user: subscribed } if subscribed == user => {
                        orders_ready = true
                    }
                    _ => {}
                }
                if fills_ready && orders_ready && tx.send(PrivateEvent::Connected).await.is_err() {
                    return Ok(());
                }
            }
            Event::Message(Incoming::UserFills { fills, .. }) => {
                // Stamped once for the whole frame, before any queuing, so the
                // stamp measures the socket and not our own fan-out.
                let recv_at = Instant::now();
                for fill in &fills {
                    if tx
                        .send(PrivateEvent::Fill(fill_event(fill, recv_at)?))
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                }
            }
            Event::Message(Incoming::OrderUpdates(updates)) => {
                let recv_at = Instant::now();
                for update in &updates {
                    if tx
                        .send(PrivateEvent::OrderUpdate(order_update_event(
                            update, recv_at,
                        )?))
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                }
            }
            _ => {}
        }
    }
}

/// Stream the account's private fills into `tx` (`userFills` WS on
/// `WALLET_ADDRESS`, account-wide: fills for every market arrive here).
/// The initial snapshot batch is skipped — only fills that happen after
/// subscribing are forwarded. Returns when the socket or `tx` closes.
pub async fn stream_fills(tx: mpsc::Sender<FillEvent>) -> anyhow::Result<()> {
    crate::load_dotenv();
    let user: Address = env::var("WALLET_ADDRESS")
        .context("WALLET_ADDRESS is required")?
        .parse()
        .context("WALLET_ADDRESS is not a valid address")?;
    let mut ws = hypercore::mainnet_ws();
    ws.subscribe(Subscription::UserFills { user });
    while let Some(event) = ws.next().await {
        if let Event::Message(Incoming::UserFills {
            is_snapshot, fills, ..
        }) = event
        {
            if is_snapshot {
                continue;
            }
            let recv_at = Instant::now();
            for fill in &fills {
                if tx.send(fill_event(fill, recv_at)?).await.is_err() {
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}

/// Normalize an SDK [`Fill`] into a [`FillEvent`].
fn fill_event(fill: &Fill, recv_at: Instant) -> anyhow::Result<FillEvent> {
    Ok(FillEvent {
        recv_at,
        coin: fill.coin.clone(),
        side: if fill.side == BookSide::Bid {
            Side::Buy
        } else {
            Side::Sell
        },
        px: decimal_to_f64(fill.px)?,
        sz: decimal_to_f64(fill.sz)?,
        oid: fill.oid,
        tid: fill.tid,
        crossed: fill.crossed,
        time: fill.time,
    })
}

fn order_update_event(
    update: &OrderUpdate<WsBasicOrder>,
    recv_at: Instant,
) -> anyhow::Result<OrderUpdateEvent> {
    let order = &update.order;
    let filled = (order.orig_sz - order.sz).max(Decimal::ZERO);
    Ok(OrderUpdateEvent {
        recv_at,
        coin: order.coin.clone(),
        info: OrderInfo {
            order_id: order.oid,
            state: order_state(update.status),
            side: if order.side == BookSide::Bid {
                Side::Buy
            } else {
                Side::Sell
            },
            limit_px: decimal_to_f64(order.limit_px)?,
            filled_qty: decimal_to_f64(filled)?,
            remaining_qty: decimal_to_f64(order.sz)?,
        },
        status_timestamp: update.status_timestamp,
    })
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
    env::var(format!(
        "HYPERLIQUID_COIN_{}",
        coin.replace(':', "_").to_ascii_uppercase()
    ))
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
    coin.split_once(':')
        .map(|(dex_name, _)| dex_name.to_string())
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

/// Reject non-reduce-only orders below the venue's minimum order value
/// before they hit the wire (the venue would refuse them anyway).
/// Reduce-only closes are exempt: flattening a sub-$10 remainder is legal.
fn check_min_notional(
    coin: &str,
    sz: Decimal,
    px: Decimal,
    reduce_only: bool,
) -> anyhow::Result<()> {
    if reduce_only {
        return Ok(());
    }
    let notional = decimal_to_f64(sz * px)?;
    if notional < MIN_NOTIONAL_USDC {
        bail!(
            "Hyperliquid {coin} order notional {notional:.2} USDC is below the ${MIN_NOTIONAL_USDC} minimum order value"
        );
    }
    Ok(())
}

/// Synthetic market-order price: opposite top of book ± 5%, tick-rounded.
fn crossing_price(
    tick: &PriceTick,
    side: Side,
    opposite_top: Option<f64>,
) -> anyhow::Result<Decimal> {
    let top = opposite_top.context("market order with an empty opposite book side")?;
    let raw = match side {
        Side::Buy => top * (1.0 + MARKET_ORDER_SLIPPAGE),
        Side::Sell => top * (1.0 - MARKET_ORDER_SLIPPAGE),
    };
    round_price(tick, side, raw)
}

/// Normalize the first status of a place response into an [`ActiveOrder`].
fn active_from_status(
    status: Option<OrderResponseStatus>,
    side: Side,
    qty: f64,
    coin: &str,
) -> anyhow::Result<ActiveOrder> {
    match status {
        Some(OrderResponseStatus::Error(err)) => bail!("Hyperliquid {coin} order rejected: {err}"),
        Some(status) => Ok(ActiveOrder {
            order_id: status.oid().unwrap_or(0),
            side,
            qty,
        }),
        None => bail!("Hyperliquid {coin} place returned empty response"),
    }
}

/// What a cancel attempt actually did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelOutcome {
    /// The resting order was canceled.
    Canceled,
    /// The oid was no longer live — already filled or already canceled
    /// (the venue reports both identically). Check fills via
    /// [`Hyperliquid::fetch_order`] before assuming nothing executed.
    AlreadyGone,
}

/// Immediate result of a native modify request. `AwaitingOrderUpdate` means
/// the order raced the request; the private stream is the authority on
/// whether it filled, remained open, or was canceled.
#[derive(Clone, Debug)]
pub enum ModifyOrderOutcome {
    Live(ActiveOrder),
    AwaitingOrderUpdate,
    Ambiguous(String),
    Deferred,
}

/// Classify a cancel response (`Ok(first status)`) or transport/batch error
/// message into a [`CancelOutcome`].
fn classify_cancel(
    result: Result<Option<OrderResponseStatus>, String>,
    coin: &str,
) -> anyhow::Result<CancelOutcome> {
    match result {
        Ok(Some(OrderResponseStatus::Error(err))) if order_gone(&err) => {
            Ok(CancelOutcome::AlreadyGone)
        }
        Ok(Some(OrderResponseStatus::Error(err))) => {
            bail!("Hyperliquid {coin} cancel rejected: {err}")
        }
        Ok(Some(_)) => Ok(CancelOutcome::Canceled),
        Ok(None) => bail!("Hyperliquid {coin} cancel returned empty response"),
        Err(err) if order_gone(&err) => Ok(CancelOutcome::AlreadyGone),
        Err(err) => bail!("Hyperliquid {coin} cancel failed: {err}"),
    }
}

/// What a modify attempt means for the tracked order.
#[derive(Debug, PartialEq, Eq)]
enum ModifyOutcome {
    /// The request raced order state or the post-only BBO. Wait for WS.
    AwaitingOrderUpdate,
    Ambiguous(String),
    /// Still live; `Some(oid)` when the response carried the (new) oid.
    Live(Option<u64>),
}

/// Classify a modify response (`Ok(first status)`) or transport/batch error
/// message into a [`ModifyOutcome`].
fn classify_modify(
    result: Result<Option<OrderResponseStatus>, String>,
    coin: &str,
) -> anyhow::Result<ModifyOutcome> {
    match result {
        Ok(Some(OrderResponseStatus::Error(err)))
            if order_gone(&err) || post_only_race(&err).is_some() =>
        {
            Ok(ModifyOutcome::AwaitingOrderUpdate)
        }
        Ok(Some(OrderResponseStatus::Error(err))) => {
            bail!("Hyperliquid {coin} modify rejected: {err}")
        }
        Ok(Some(OrderResponseStatus::Filled { .. })) => Ok(ModifyOutcome::AwaitingOrderUpdate),
        Ok(Some(status)) => Ok(ModifyOutcome::Live(status.oid())),
        Ok(None) => bail!("Hyperliquid {coin} modify returned empty response"),
        Err(err) if order_gone(&err) || post_only_race(&err).is_some() => {
            Ok(ModifyOutcome::AwaitingOrderUpdate)
        }
        Err(err) if err.contains("No need") || err.contains("same") => {
            Ok(ModifyOutcome::Live(None))
        }
        Err(err) => Ok(ModifyOutcome::Ambiguous(format!(
            "Hyperliquid {coin} modify outcome is ambiguous: {err}"
        ))),
    }
}

/// The venue's view of the book at the instant it refused a post-only order.
///
/// Hyperliquid embeds the matching engine's BBO in the rejection text
/// (`... bbo was 1145.3@1145.4.`). That snapshot is strictly better than
/// anything we can fetch afterwards: it is the state that caused the
/// rejection, it costs no round-trip, and re-resting on the quoted side is
/// non-crossing by construction. `bbo` is `None` when the venue omits it,
/// which is the only case where refetching the book is justified.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PostOnlyRace {
    pub bbo: Option<(f64, f64)>,
}

impl PostOnlyRace {
    /// The price a passive order of `side` may rest at without crossing the
    /// quoted BBO. `Buy` joins the bid, `Sell` joins the ask.
    pub fn rest_price(&self, side: Side) -> Option<f64> {
        let (bid, ask) = self.bbo?;
        Some(match side {
            Side::Buy => bid,
            Side::Sell => ask,
        })
    }
}

/// Recognizes a post-only rejection and recovers the quoted BBO.
///
/// Returns `None` for every other error, so callers can use it as the single
/// test for "this order did not rest purely because it raced the book".
pub fn post_only_race(message: &str) -> Option<PostOnlyRace> {
    if !message.contains("Post only order would have immediately matched") {
        return None;
    }
    Some(PostOnlyRace {
        bbo: parse_quoted_bbo(message),
    })
}

/// Extracts `bid@ask` from the `bbo was <bid>@<ask>` clause.
///
/// Anything unparsable degrades to `None` rather than an error: a rejection
/// we cannot read is still a rejection, and the caller has a fallback.
fn parse_quoted_bbo(message: &str) -> Option<(f64, f64)> {
    let tail = message.split("bbo was ").nth(1)?;
    let quote = tail
        .split(|c: char| !(c.is_ascii_digit() || c == '.' || c == '@'))
        .next()?;
    let (bid, ask) = quote.split_once('@')?;
    let bid: f64 = bid.trim_end_matches('.').parse().ok()?;
    let ask: f64 = ask.trim_end_matches('.').parse().ok()?;
    // A crossed or non-positive quote would push the retry through the book;
    // treat it as unreadable so the caller refetches instead.
    (bid > 0.0 && ask > 0.0 && bid < ask).then_some((bid, ask))
}

fn is_rate_limit_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("rate limit")
        || message.contains("too many requests")
        || message.contains("http 429")
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
    order_info(order, order_state(update.status))
}

fn order_state(status: HlOrderStatus) -> OrderState {
    if status.is_filled() {
        OrderState::Filled
    } else if status.is_rejected() {
        OrderState::Rejected
    } else if matches!(status, HlOrderStatus::Open | HlOrderStatus::Triggered) {
        OrderState::Open
    } else {
        OrderState::Canceled
    }
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
        side: if order.side == BookSide::Bid {
            Side::Buy
        } else {
            Side::Sell
        },
        limit_px: decimal_to_f64(order.limit_px)?,
        filled_qty: decimal_to_f64(filled)?,
        remaining_qty: decimal_to_f64(order.sz)?,
    })
}

/// Find this market's perp position in a `clearinghouseState` (defaults to
/// a zeroed snapshot when flat).
fn snapshot_from_positions(
    positions: &[AssetPosition],
    coin: &str,
) -> anyhow::Result<PositionSnapshot> {
    let Some(pos) = positions
        .iter()
        .find(|pos| coin_matches(&pos.position.coin, coin))
    else {
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
        assert_eq!(
            round_price(&tick, Side::Buy, 25.13579).unwrap(),
            d("25.135")
        );
        assert_eq!(
            round_price(&tick, Side::Sell, 25.13571).unwrap(),
            d("25.136")
        );
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
    fn derives_market_info_from_the_resolved_spec() {
        let spec = MarketSpec {
            coin: "xyz:SKHY".to_string(),
            dex_name: Some("xyz".to_string()),
            asset: 7,
            sz_decimals: 2,
            max_leverage: 3,
            tick: PriceTick::for_perp(2),
        };
        let info = market_info(&spec);
        assert_eq!(info.coin, "xyz:SKHY");
        assert_eq!(info.sz_decimals, 2);
        assert_eq!(info.px_decimals, 4); // 6 - sz_decimals
        assert_eq!(info.min_notional, MIN_NOTIONAL_USDC);
        assert_eq!(info.max_leverage, 3);
        // Smallest size clearing the $10 minimum at a $25 price.
        assert_eq!(info.min_qty(25.0), 0.4);
        // sz_decimals can exceed the perp max; decimals never go negative.
        assert_eq!(
            market_info(&MarketSpec {
                sz_decimals: 6,
                ..spec
            })
            .px_decimals,
            0
        );
    }

    #[test]
    fn enforces_min_notional_for_opening_orders_only() {
        let coin = "xyz:SKHY";
        // $10+ opening orders pass; sub-$10 are refused with a clear error.
        assert!(check_min_notional(coin, d("0.4"), d("25.0"), false).is_ok());
        assert!(check_min_notional(coin, d("0.5"), d("20.0"), false).is_ok());
        let err = check_min_notional(coin, d("0.3"), d("25.0"), false).unwrap_err();
        assert!(err.to_string().contains("minimum"), "{err}");
        // Reduce-only closes are exempt (flattening dust is legal).
        assert!(check_min_notional(coin, d("0.01"), d("25.0"), true).is_ok());
    }

    #[test]
    fn crossing_prices_cross_by_five_percent() {
        let tick = PriceTick::for_perp(2);
        // Buy: ask 20.0 * 1.05 = 21.0; sell: bid 20.0 * 0.95 = 19.0.
        assert_eq!(
            crossing_price(&tick, Side::Buy, Some(20.0)).unwrap(),
            d("21")
        );
        assert_eq!(
            crossing_price(&tick, Side::Sell, Some(20.0)).unwrap(),
            d("19")
        );
        // Off-grid results round conservatively (still crossing).
        assert_eq!(
            crossing_price(&tick, Side::Buy, Some(25.137)).unwrap(),
            d("26.393")
        );
        // Empty opposite side → error.
        assert!(crossing_price(&tick, Side::Buy, None).is_err());
    }

    #[test]
    fn maps_place_response_statuses() {
        let resting = active_from_status(
            Some(OrderResponseStatus::Resting {
                oid: 7,
                cloid: None,
            }),
            Side::Buy,
            1.5,
            "xyz:SKHY",
        )
        .unwrap();
        assert_eq!(resting.order_id, 7);
        assert_eq!(resting.side, Side::Buy);
        assert_eq!(resting.qty, 1.5);

        let filled = active_from_status(
            Some(OrderResponseStatus::Filled {
                total_sz: d("1.5"),
                avg_px: d("25.1"),
                oid: 8,
            }),
            Side::Sell,
            1.5,
            "xyz:SKHY",
        )
        .unwrap();
        assert_eq!(filled.order_id, 8);

        let rejected = active_from_status(
            Some(OrderResponseStatus::Error(
                "Order must have minimum value of $10.".into(),
            )),
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
            classify_modify(
                Ok(Some(OrderResponseStatus::Resting {
                    oid: 42,
                    cloid: None
                })),
                coin
            )
            .unwrap(),
            ModifyOutcome::Live(Some(42)),
        );
        // Filled during modify is finalized by orderUpdates.
        assert_eq!(
            classify_modify(
                Ok(Some(OrderResponseStatus::Filled {
                    total_sz: d("1"),
                    avg_px: d("25"),
                    oid: 42
                })),
                coin
            )
            .unwrap(),
            ModifyOutcome::AwaitingOrderUpdate,
        );
        // Order vanished underneath us: wait for its terminal update.
        for gone in [
            "Order 42 does not exist",
            "oid not found",
            "Order was never placed, already canceled, or filled.",
            "Cannot modify canceled or filled order",
        ] {
            assert_eq!(
                classify_modify(Ok(Some(OrderResponseStatus::Error(gone.into()))), coin).unwrap(),
                ModifyOutcome::AwaitingOrderUpdate
            );
            assert_eq!(
                classify_modify(Err(gone.into()), coin).unwrap(),
                ModifyOutcome::AwaitingOrderUpdate
            );
        }
        assert_eq!(
            classify_modify(
                Err("Post only order would have immediately matched".into()),
                coin
            )
            .unwrap(),
            ModifyOutcome::AwaitingOrderUpdate,
        );
        // "No change needed" transport errors keep the original order.
        assert_eq!(
            classify_modify(Err("No need to modify".into()), coin).unwrap(),
            ModifyOutcome::Live(None)
        );
        // Real rejections propagate.
        assert!(
            classify_modify(
                Ok(Some(OrderResponseStatus::Error(
                    "Insufficient margin".into()
                ))),
                coin
            )
            .is_err()
        );
        assert!(matches!(
            classify_modify(Err("transport timeout".into()), coin).unwrap(),
            ModifyOutcome::Ambiguous(_)
        ));
        assert!(classify_modify(Ok(None), coin).is_err());
    }

    #[test]
    fn recovers_the_quoted_bbo_from_a_post_only_rejection() {
        // Verbatim venue text, including the trailing `asset=` clause that
        // must not be swallowed into the ask.
        let race = post_only_race(
            "Hyperliquid xyz:SKHX order rejected: Post only order would have \
             immediately matched, bbo was 1145.3@1145.4. asset=110022",
        )
        .expect("recognized as a post-only race");
        assert_eq!(race.bbo, Some((1145.3, 1145.4)));
        // A buy joins the bid and a sell the ask, so neither can cross the
        // book the venue just quoted.
        assert_eq!(race.rest_price(Side::Buy), Some(1145.3));
        assert_eq!(race.rest_price(Side::Sell), Some(1145.4));

        // Integral prices, and the modify wrapper's different prefix.
        let race = post_only_race(
            "Hyperliquid xyz:SKHX modify rejected: Error placing new order during \
             modify: Post only order would have immediately matched, bbo was 1145@1145.2.",
        )
        .expect("recognized as a post-only race");
        assert_eq!(race.bbo, Some((1145.0, 1145.2)));

        // Recognized but unquotable: the caller must fall back to the book
        // rather than reprice from nothing.
        let race = post_only_race("Post only order would have immediately matched")
            .expect("recognized as a post-only race");
        assert_eq!(race.bbo, None);
        assert_eq!(race.rest_price(Side::Buy), None);

        // A crossed or zero quote would reprice through the book; refuse it.
        for bad in [
            "Post only order would have immediately matched, bbo was 1145.4@1145.3.",
            "Post only order would have immediately matched, bbo was 0@1145.3.",
            "Post only order would have immediately matched, bbo was @.",
        ] {
            assert_eq!(post_only_race(bad).expect("recognized").bbo, None, "{bad}");
        }

        // Every other rejection stays a plain error.
        assert!(post_only_race("Insufficient margin").is_none());
        assert!(post_only_race("Order 42 does not exist").is_none());
    }

    #[test]
    fn calculates_documented_request_weights() {
        assert_eq!(exchange_action_weight(1), 1);
        assert_eq!(exchange_action_weight(39), 1);
        assert_eq!(exchange_action_weight(40), 2);
        assert_eq!(exchange_action_weight(79), 2);
        assert_eq!(response_weight(20, 0, 20), 20);
        assert_eq!(response_weight(20, 1, 20), 21);
        assert_eq!(response_weight(20, 20, 20), 21);
        assert_eq!(response_weight(20, 21, 20), 22);
        assert_eq!(response_weight(20, MAX_USER_FILLS_ITEMS, 20), 120);
    }

    #[test]
    fn classifies_cancel_outcomes() {
        let coin = "xyz:SKHY";
        // Clean cancel of a resting order.
        assert_eq!(
            classify_cancel(Ok(Some(OrderResponseStatus::Success)), coin).unwrap(),
            CancelOutcome::Canceled
        );
        // Cancel racing a fill (or an already-canceled/unknown oid) is a
        // normal outcome, whether reported in-band or as a batch error.
        for gone in [
            "Order 42 does not exist",
            "Order was never placed, already canceled, or filled.",
        ] {
            assert_eq!(
                classify_cancel(Ok(Some(OrderResponseStatus::Error(gone.into()))), coin).unwrap(),
                CancelOutcome::AlreadyGone
            );
            assert_eq!(
                classify_cancel(Err(gone.into()), coin).unwrap(),
                CancelOutcome::AlreadyGone
            );
        }
        // Real rejections and transport failures still propagate.
        assert!(
            classify_cancel(
                Ok(Some(OrderResponseStatus::Error(
                    "Vault not registered".into()
                ))),
                coin
            )
            .is_err()
        );
        assert!(classify_cancel(Err("transport timeout".into()), coin).is_err());
        assert!(classify_cancel(Ok(None), coin).is_err());
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

        let rejected =
            order_info_from_update(&update("minTradeNtlRejected", "1.5", "1.5")).unwrap();
        assert_eq!(rejected.state, OrderState::Rejected);
    }

    #[test]
    fn maps_user_fills() {
        let fill: Fill = serde_json::from_value(serde_json::json!({
            "coin": "SKHY",
            "px": "25.135",
            "sz": "1.5",
            "side": "B",
            "time": 1_753_200_000_000_u64,
            "startPosition": "0.0",
            "dir": "Open Long",
            "closedPnl": "0.0",
            "hash": "0xabc",
            "oid": 77,
            "crossed": false,
            "fee": "0.01",
            "tid": 900,
            "cloid": null,
            "feeToken": "USDC"
        }))
        .expect("fixture parses");
        let recv_at = Instant::now();
        let event = fill_event(&fill, recv_at).unwrap();
        assert_eq!(event.coin, "SKHY");
        assert_eq!(event.side, Side::Buy);
        assert_eq!(event.px, 25.135);
        assert_eq!(event.sz, 1.5);
        assert_eq!(event.oid, 77);
        assert!(!event.crossed);
        assert_eq!(event.time, 1_753_200_000_000);
        // The arrival stamp is the caller's, not derived from the payload:
        // it has to stay comparable with other local `Instant`s.
        assert_eq!(event.recv_at, recv_at);
    }

    #[test]
    fn maps_websocket_order_updates() {
        let update: OrderUpdate<WsBasicOrder> = serde_json::from_value(serde_json::json!({
            "status": "open",
            "statusTimestamp": 1_753_200_000_100_u64,
            "order": {
                "timestamp": 1_753_200_000_000_u64,
                "coin": "SKHY",
                "side": "A",
                "limitPx": "25.2",
                "sz": "0.6",
                "oid": 88,
                "origSz": "1.0",
                "cloid": null
            }
        }))
        .expect("fixture parses");
        let recv_at = Instant::now();
        let event = order_update_event(&update, recv_at).unwrap();
        assert_eq!(event.recv_at, recv_at);
        assert_eq!(event.coin, "SKHY");
        assert_eq!(event.info.order_id, 88);
        assert_eq!(event.info.state, OrderState::Open);
        assert_eq!(event.info.side, Side::Sell);
        assert_eq!(event.info.filled_qty, 0.4);
        assert_eq!(event.info.remaining_qty, 0.6);
        assert_eq!(event.status_timestamp, 1_753_200_000_100);
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
