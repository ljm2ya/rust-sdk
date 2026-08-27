//! Upbit spot venue capsule, ported from the kimp-arbitrager Upbit wrapper
//! onto the [`crate::binance`] / [`crate::lighter`] capsule surface.
//!
//! - [`Upbit`] — REST capsule for a single market (`KRW-BTC` style codes):
//!   place/cancel/fetch orders, open orders, balances, order fees,
//!   deposit/withdraw queries (JWT-signed) plus public quotation data
//!   (ticker / orderbook / trades / candles).
//! - [`stream_book`] — public WS orderbook stream. Upbit sends full
//!   top-of-book snapshots (no diffs), so every message becomes a fresh
//!   synced [`OrderBook`] via [`OrderBook::from_sides`].
//! - [`stream_user_events`] — private WS (`myOrder` / `myAsset` channels)
//!   emitting [`UserEvent`]s.
//!
//! ## Signing
//!
//! Upbit authenticates with a hand-rolled JWT (HS256): claims carry the
//! access key, a UUIDv4 nonce and — when the request has parameters — a
//! SHA512 hex hash of the URL-encoded, alphabetically sorted query string.
//! Quirk (verified against Upbit + the original Go implementation): the hash
//! is ALWAYS computed over the query-string form, even for POSTs whose body
//! is sent as JSON. The JSON body is built from a sorted map so it matches
//! the signed parameter set. A golden JWT test below pins byte-for-byte
//! compatibility with the original implementation.
//!
//! ## Environment keys
//!
//! | Key | Meaning |
//! |-----|---------|
//! | `UPBIT_ACCESS_KEY` | API access key (trading mode + private WS) |
//! | `UPBIT_SECRET_KEY` | API secret key (trading mode + private WS) |
//!
//! Upbit references orders by a venue-side UUID string, but the shared
//! [`ActiveOrder::order_id`] is numeric — so the capsule generates a numeric
//! client id per order, sends it as Upbit's `identifier`, and tracks the
//! `identifier -> uuid` mapping internally. Orders placed by this capsule
//! (or an earlier process of it, via the identifier fallback) can be
//! fetched/cancelled by that id; foreign orders show up in
//! [`Upbit::open_orders`] with `order_id == 0`.
//!
//! KRW-market limit prices are rounded onto Upbit's standard KRW tick table
//! ([`krw_tick_size`]; a few pairs have venue-specific exceptions) and KRW
//! orders below the venue's 5,000 KRW minimum are refused client-side.

use std::collections::{BTreeMap, HashMap};
use std::env;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256, Sha512};
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::book::{BookUpdate, OrderBook};
use crate::order::{ActiveOrder, OrderInfo, OrderState, Side};

const BASE_URL: &str = "https://api.upbit.com";
const WS_PUBLIC_URL: &str = "wss://api.upbit.com/websocket/v1";
const WS_PRIVATE_URL: &str = "wss://api.upbit.com/websocket/v1/private";

/// Minimum order value on KRW markets (checked client-side).
const MIN_KRW_ORDER: f64 = 5_000.0;
/// Upbit drops WS connections idle for 120s; send a text `PING` well within.
const WS_KEEPALIVE: Duration = Duration::from_secs(60);

static ORDER_SEQ: AtomicU64 = AtomicU64::new(1);

/// Sorted params: BTreeMap keeps the alphabetical order both the query-hash
/// and the deterministic JSON body require.
type Params = BTreeMap<String, String>;

// ---------------------------------------------------------------------------
// Request pacing (client-side floor under Upbit's per-group limits:
// orders 8/s, other exchange endpoints 30/s, quotation 10/s).
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Pace {
    /// `/v1/orders` create/cancel — venue limit 8 req/s.
    Order,
    /// Other JWT endpoints — venue limit 30 req/s.
    Exchange,
    /// Public quotation endpoints — venue limit 10 req/s per group.
    Quotation,
}

impl Pace {
    fn interval(self) -> Duration {
        match self {
            Pace::Order => Duration::from_millis(150),
            Pace::Exchange => Duration::from_millis(50),
            Pace::Quotation => Duration::from_millis(110),
        }
    }
}

#[derive(Default)]
struct Pacer(Mutex<HashMap<Pace, Instant>>);

impl Pacer {
    /// Reserve the next allowed send slot for `group` and sleep until it.
    async fn acquire(&self, group: Pace) {
        let wait = {
            let mut slots = self.0.lock().expect("pacer lock");
            let now = Instant::now();
            let slot = slots
                .get(&group)
                .map(|last| *last + group.interval())
                .filter(|slot| *slot > now)
                .unwrap_or(now);
            slots.insert(group, slot);
            slot - now
        };
        if wait > Duration::ZERO {
            tokio::time::sleep(wait).await;
        }
    }
}

// ---------------------------------------------------------------------------
// JWT signing
// ---------------------------------------------------------------------------

/// `Bearer <jwt>` for `params` (fresh UUIDv4 nonce).
fn jwt(access_key: &str, secret_key: &str, params: Option<&Params>) -> anyhow::Result<String> {
    jwt_with_nonce(access_key, secret_key, params, &uuid::Uuid::new_v4().to_string())
}

fn jwt_with_nonce(
    access_key: &str,
    secret_key: &str,
    params: Option<&Params>,
    nonce: &str,
) -> anyhow::Result<String> {
    // Claims in alphabetical key order (matches the original implementation
    // and serde_json's sorted map either way).
    let mut claims = serde_json::Map::new();
    claims.insert("access_key".to_string(), json!(access_key));
    claims.insert("nonce".to_string(), json!(nonce));
    if let Some(params) = params.filter(|params| !params.is_empty()) {
        claims.insert("query_hash".to_string(), json!(sha512_hex(&query_string(params))));
        claims.insert("query_hash_alg".to_string(), json!("SHA512"));
    }

    let mut header = serde_json::Map::new();
    header.insert("alg".to_string(), json!("HS256"));
    header.insert("typ".to_string(), json!("JWT"));

    let header_b64 = URL_SAFE_NO_PAD.encode(Value::Object(header).to_string());
    let payload_b64 = URL_SAFE_NO_PAD.encode(Value::Object(claims).to_string());
    let message = format!("{header_b64}.{payload_b64}");

    let mut mac = Hmac::<Sha256>::new_from_slice(secret_key.as_bytes())
        .context("Upbit JWT: bad HMAC key")?;
    mac.update(message.as_bytes());
    let signature_b64 = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());

    Ok(format!("Bearer {message}.{signature_b64}"))
}

/// URL-encoded query string in alphabetical key order — the exact bytes the
/// JWT `query_hash` covers.
fn query_string(params: &Params) -> String {
    params
        .iter()
        .map(|(key, value)| format!("{}={}", form_encode(key), form_encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

/// `application/x-www-form-urlencoded` byte encoding (space → `+`), matching
/// `url::form_urlencoded::byte_serialize` used by the original wrapper.
fn form_encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for &byte in text.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'*' => {
                out.push(byte as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn sha512_hex(data: &str) -> String {
    let mut hasher = Sha512::new();
    hasher.update(data.as_bytes());
    hex::encode(hasher.finalize())
}

// ---------------------------------------------------------------------------
// Public wire/response types
// ---------------------------------------------------------------------------

/// One account balance line (`/v1/accounts`).
#[derive(Debug, Clone)]
pub struct Balance {
    pub currency: String,
    pub balance: f64,
    pub locked: f64,
    pub avg_buy_price: f64,
    pub unit_currency: String,
}

/// `/v1/ticker` entry.
#[derive(Debug, Clone, Deserialize)]
pub struct Ticker {
    pub market: String,
    pub trade_price: f64,
    pub opening_price: f64,
    pub high_price: f64,
    pub low_price: f64,
    pub acc_trade_price_24h: f64,
    pub acc_trade_volume_24h: f64,
    pub signed_change_rate: f64,
    pub timestamp: i64,
}

/// `/v1/trades/ticks` entry.
#[derive(Debug, Clone, Deserialize)]
pub struct TradeTick {
    pub market: String,
    pub timestamp: i64,
    pub trade_price: f64,
    pub trade_volume: f64,
    /// `"ASK"` or `"BID"` (taker side).
    pub ask_bid: String,
}

/// `/v1/candles/minutes/{unit}` entry.
#[derive(Debug, Clone, Deserialize)]
pub struct Candle {
    pub market: String,
    pub candle_date_time_utc: String,
    pub opening_price: f64,
    pub high_price: f64,
    pub low_price: f64,
    pub trade_price: f64,
    pub candle_acc_trade_volume: f64,
    pub candle_acc_trade_price: f64,
    pub timestamp: i64,
}

/// `/v1/withdraws` entry (amounts kept as venue strings — transfer amounts
/// must not go through f64).
#[derive(Debug, Clone, Deserialize)]
pub struct Withdrawal {
    pub uuid: String,
    pub currency: String,
    pub state: String,
    pub created_at: String,
    pub done_at: Option<String>,
    pub amount: String,
    pub fee: String,
    pub txid: Option<String>,
    #[serde(rename = "type")]
    pub kind: Option<String>,
    pub transaction_type: Option<String>,
}

/// `/v1/deposits` entry.
#[derive(Debug, Clone, Deserialize)]
pub struct Deposit {
    pub uuid: String,
    pub currency: String,
    pub state: String,
    pub created_at: String,
    pub done_at: Option<String>,
    pub amount: String,
    pub txid: Option<String>,
    #[serde(rename = "type")]
    pub kind: Option<String>,
    pub transaction_type: Option<String>,
}

/// Private WS `myOrder` event (tolerant: every field defaults).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct WsOrder {
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub uuid: String,
    #[serde(default)]
    pub identifier: Option<String>,
    /// `"BID"` or `"ASK"`.
    #[serde(default)]
    pub ask_bid: String,
    #[serde(default)]
    pub order_type: String,
    #[serde(default)]
    pub state: String,
    pub trade_uuid: Option<String>,
    pub price: Option<f64>,
    pub avg_price: Option<f64>,
    pub volume: Option<f64>,
    pub remaining_volume: Option<f64>,
    pub executed_volume: Option<f64>,
    pub paid_fee: Option<f64>,
    #[serde(default)]
    pub timestamp: i64,
}

/// Private WS `myAsset` event.
#[derive(Debug, Clone, Deserialize)]
pub struct WsAsset {
    pub assets: Vec<WsAssetItem>,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WsAssetItem {
    pub currency: String,
    pub balance: f64,
    pub locked: f64,
}

/// An event from [`stream_user_events`].
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum UserEvent {
    Order(WsOrder),
    Asset(WsAsset),
}

// ---------------------------------------------------------------------------
// Private wire types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct WireError {
    error: WireErrorBody,
}

#[derive(Debug, Deserialize)]
struct WireErrorBody {
    /// String on most errors, numeric on some (e.g. 404) — keep raw.
    name: Value,
    message: String,
}

#[derive(Debug, Clone, Deserialize)]
struct WireOrder {
    uuid: String,
    side: String,
    state: String,
    price: Option<String>,
    remaining_volume: Option<String>,
    executed_volume: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WireBalance {
    currency: String,
    balance: String,
    locked: String,
    avg_buy_price: String,
    unit_currency: String,
}

#[derive(Debug, Deserialize)]
struct WireOrderbook {
    orderbook_units: Vec<WireOrderbookUnit>,
}

#[derive(Debug, Deserialize)]
struct WireOrderbookUnit {
    ask_price: f64,
    bid_price: f64,
    ask_size: f64,
    bid_size: f64,
}

#[derive(Debug, Deserialize)]
struct WireWsOrderbook {
    orderbook_units: Vec<WireOrderbookUnit>,
}

// ---------------------------------------------------------------------------
// Capsule
// ---------------------------------------------------------------------------

/// Upbit spot capsule for a single market.
pub struct Upbit {
    market: String,
    access_key: String,
    secret_key: String,
    trading: bool,
    http: reqwest::Client,
    pacer: Pacer,
    /// Client order id (our numeric identifier) → venue-side order UUID.
    orders: Mutex<HashMap<u64, String>>,
}

impl Upbit {
    /// Public-data-only capsule (no credentials required). `market` uses
    /// Upbit's `QUOTE-BASE` codes, e.g. `KRW-BTC`; validated with a live
    /// ticker fetch.
    pub async fn connect(market: &str) -> anyhow::Result<Self> {
        Self::build(market, false).await
    }

    /// Trading capsule. Needs `UPBIT_ACCESS_KEY` / `UPBIT_SECRET_KEY`.
    pub async fn connect_trading(market: &str) -> anyhow::Result<Self> {
        Self::build(market, true).await
    }

    async fn build(market: &str, trading: bool) -> anyhow::Result<Self> {
        crate::load_dotenv();
        let market = market.to_uppercase();
        if market.split('-').count() != 2 {
            bail!("Upbit market must be QUOTE-BASE (e.g. KRW-BTC), got {market}");
        }
        let (access_key, secret_key) = if trading {
            (
                env::var("UPBIT_ACCESS_KEY").context("UPBIT_ACCESS_KEY is required")?,
                env::var("UPBIT_SECRET_KEY").context("UPBIT_SECRET_KEY is required")?,
            )
        } else {
            (
                env::var("UPBIT_ACCESS_KEY").unwrap_or_default(),
                env::var("UPBIT_SECRET_KEY").unwrap_or_default(),
            )
        };
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context("Upbit HTTP client")?;
        let upbit = Self {
            market,
            access_key,
            secret_key,
            trading,
            http,
            pacer: Pacer::default(),
            orders: Mutex::new(HashMap::new()),
        };
        // Existence check: unknown markets must fail at connect, not on the
        // first order.
        upbit
            .ticker()
            .await
            .with_context(|| format!("Upbit market {} is not tradable", upbit.market))?;
        Ok(upbit)
    }

    /// Market code (`KRW-BTC` style).
    pub fn market(&self) -> &str {
        &self.market
    }

    /// Quote currency (`KRW` of `KRW-BTC`).
    pub fn quote(&self) -> &str {
        self.market.split('-').next().unwrap_or(&self.market)
    }

    /// Base asset (`BTC` of `KRW-BTC`).
    pub fn base(&self) -> &str {
        self.market.split('-').nth(1).unwrap_or(&self.market)
    }

    fn require_trading(&self) -> anyhow::Result<()> {
        if self.trading {
            Ok(())
        } else {
            bail!("Upbit capsule is read-only; use connect_trading for signed endpoints")
        }
    }

    // -- signed plumbing ----------------------------------------------------

    async fn signed_get<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        params: Option<Params>,
        pace: Pace,
    ) -> anyhow::Result<T> {
        self.require_trading()?;
        self.pacer.acquire(pace).await;
        let auth = jwt(&self.access_key, &self.secret_key, params.as_ref())?;
        let mut url = format!("{BASE_URL}{path}");
        if let Some(params) = &params {
            let query = query_string(params);
            if !query.is_empty() {
                url = format!("{url}?{query}");
            }
        }
        let resp = self.http.get(&url).header("Authorization", auth).send().await?;
        parse_response(resp, path).await
    }

    /// POST: the JWT hash covers the query-string form of `params`; the body
    /// is the same params as sorted JSON (see module docs).
    async fn signed_post<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        params: Params,
        pace: Pace,
    ) -> anyhow::Result<T> {
        self.require_trading()?;
        self.pacer.acquire(pace).await;
        let auth = jwt(&self.access_key, &self.secret_key, Some(&params))?;
        let body = serde_json::to_string(&params)?;
        let resp = self
            .http
            .post(format!("{BASE_URL}{path}"))
            .header("Authorization", auth)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await?;
        parse_response(resp, path).await
    }

    async fn signed_delete<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        params: Params,
        pace: Pace,
    ) -> anyhow::Result<T> {
        self.require_trading()?;
        self.pacer.acquire(pace).await;
        let auth = jwt(&self.access_key, &self.secret_key, Some(&params))?;
        let url = format!("{BASE_URL}{path}?{}", query_string(&params));
        let resp = self.http.delete(&url).header("Authorization", auth).send().await?;
        parse_response(resp, path).await
    }

    // -- public quotation data ---------------------------------------------

    /// Latest ticker for this market.
    pub async fn ticker(&self) -> anyhow::Result<Ticker> {
        self.pacer.acquire(Pace::Quotation).await;
        let resp = self
            .http
            .get(format!("{BASE_URL}/v1/ticker"))
            .query(&[("markets", self.market.as_str())])
            .send()
            .await?;
        let tickers: Vec<Ticker> = parse_response(resp, "/v1/ticker").await?;
        tickers.into_iter().next().context("Upbit /v1/ticker: empty response")
    }

    /// REST orderbook snapshot as a synced [`OrderBook`].
    pub async fn book(&self) -> anyhow::Result<OrderBook> {
        self.pacer.acquire(Pace::Quotation).await;
        let resp = self
            .http
            .get(format!("{BASE_URL}/v1/orderbook"))
            .query(&[("markets", self.market.as_str())])
            .send()
            .await?;
        let books: Vec<WireOrderbook> = parse_response(resp, "/v1/orderbook").await?;
        let book = books.into_iter().next().context("Upbit /v1/orderbook: empty response")?;
        Ok(book_from_units(&book.orderbook_units))
    }

    /// Mid of the REST book's best bid/ask.
    pub async fn mid_price(&self) -> anyhow::Result<f64> {
        let book = self.book().await?;
        book.mid().context("Upbit book has no mid (one side empty)")
    }

    /// Most recent trades (newest first).
    pub async fn recent_trades(&self, count: u32) -> anyhow::Result<Vec<TradeTick>> {
        self.pacer.acquire(Pace::Quotation).await;
        let resp = self
            .http
            .get(format!("{BASE_URL}/v1/trades/ticks"))
            .query(&[("market", self.market.as_str()), ("count", &count.to_string())])
            .send()
            .await?;
        parse_response(resp, "/v1/trades/ticks").await
    }

    /// Minute candles (`unit` ∈ 1,3,5,10,15,30,60,240; newest first).
    pub async fn minute_candles(&self, unit: u32, count: u32) -> anyhow::Result<Vec<Candle>> {
        self.pacer.acquire(Pace::Quotation).await;
        let resp = self
            .http
            .get(format!("{BASE_URL}/v1/candles/minutes/{unit}"))
            .query(&[("market", self.market.as_str()), ("count", &count.to_string())])
            .send()
            .await?;
        parse_response(resp, "/v1/candles/minutes").await
    }

    // -- account ------------------------------------------------------------

    /// All account balances.
    pub async fn balances(&self) -> anyhow::Result<Vec<Balance>> {
        let wire: Vec<WireBalance> =
            self.signed_get("/v1/accounts", None, Pace::Exchange).await?;
        Ok(wire
            .into_iter()
            .map(|b| Balance {
                balance: b.balance.parse().unwrap_or(0.0),
                locked: b.locked.parse().unwrap_or(0.0),
                avg_buy_price: b.avg_buy_price.parse().unwrap_or(0.0),
                currency: b.currency,
                unit_currency: b.unit_currency,
            })
            .collect())
    }

    /// Balance for one currency (`None` when the account never held it).
    pub async fn balance(&self, currency: &str) -> anyhow::Result<Option<Balance>> {
        let currency = currency.to_uppercase();
        Ok(self.balances().await?.into_iter().find(|b| b.currency == currency))
    }

    /// `(bid_fee, ask_fee)` maker/taker rate for this market.
    pub async fn order_chance_fees(&self) -> anyhow::Result<(f64, f64)> {
        let mut params = Params::new();
        params.insert("market".to_string(), self.market.clone());
        let value: Value = self
            .signed_get("/v1/orders/chance", Some(params), Pace::Exchange)
            .await?;
        let fee = |key: &str| match value.get(key) {
            Some(Value::String(s)) => s.parse().unwrap_or(0.0),
            Some(Value::Number(n)) => n.as_f64().unwrap_or(0.0),
            _ => 0.0,
        };
        Ok((fee("bid_fee"), fee("ask_fee")))
    }

    // -- orders --------------------------------------------------------------

    /// Place a resting limit order. KRW-market prices are rounded onto the
    /// standard KRW tick grid; sizes are truncated to Upbit's 8 decimals.
    pub async fn place_limit(&self, side: Side, qty: f64, price: f64) -> anyhow::Result<ActiveOrder> {
        self.limit_order(side, qty, price, None).await
    }

    /// Taker limit order (`time_in_force=ioc`): fills immediately against
    /// the book up to the limit price; the venue cancels any remainder.
    pub async fn place_limit_ioc(
        &self,
        side: Side,
        qty: f64,
        price: f64,
    ) -> anyhow::Result<ActiveOrder> {
        self.limit_order(side, qty, price, Some("ioc")).await
    }

    /// Limit order that only rests (`time_in_force=post_only`).
    pub async fn place_post_only(
        &self,
        side: Side,
        qty: f64,
        price: f64,
    ) -> anyhow::Result<ActiveOrder> {
        self.limit_order(side, qty, price, Some("post_only")).await
    }

    async fn limit_order(
        &self,
        side: Side,
        qty: f64,
        price: f64,
        time_in_force: Option<&str>,
    ) -> anyhow::Result<ActiveOrder> {
        self.require_trading()?;
        if price.is_nan() || price <= 0.0 {
            bail!("Upbit limit order price must be positive, got {price}");
        }
        let (price, price_str) = if self.quote() == "KRW" {
            let rounded = round_to_krw_tick(price);
            (rounded, format_krw_price(rounded))
        } else {
            (price, trim_fixed(price))
        };
        let qty = truncate_qty(qty);
        if qty.is_nan() || qty <= 0.0 {
            bail!("Upbit order size truncates to zero");
        }
        if self.quote() == "KRW" && price * qty < MIN_KRW_ORDER {
            bail!(
                "Upbit order value {:.0} KRW is below the venue minimum {MIN_KRW_ORDER} KRW",
                price * qty
            );
        }

        let mut params = Params::new();
        params.insert("ord_type".to_string(), "limit".to_string());
        params.insert("volume".to_string(), trim_fixed(qty));
        params.insert("price".to_string(), price_str);
        if let Some(tif) = time_in_force {
            params.insert("time_in_force".to_string(), tif.to_string());
        }
        self.submit_order(side, qty, params).await
    }

    /// Market buy: spends `quote_total` (e.g. KRW) at market. The filled base
    /// quantity is unknown until execution, so the returned
    /// [`ActiveOrder::qty`] is `0.0` — read it back with [`Self::fetch_order`].
    pub async fn market_buy(&self, quote_total: f64) -> anyhow::Result<ActiveOrder> {
        self.require_trading()?;
        if self.quote() == "KRW" && quote_total < MIN_KRW_ORDER {
            bail!("Upbit market buy of {quote_total} KRW is below the venue minimum {MIN_KRW_ORDER} KRW");
        }
        let mut params = Params::new();
        params.insert("ord_type".to_string(), "price".to_string());
        params.insert("price".to_string(), trim_fixed(quote_total));
        self.submit_order(Side::Buy, 0.0, params).await
    }

    /// Market sell of `qty` base units.
    pub async fn market_sell(&self, qty: f64) -> anyhow::Result<ActiveOrder> {
        self.require_trading()?;
        let qty = truncate_qty(qty);
        if qty.is_nan() || qty <= 0.0 {
            bail!("Upbit order size truncates to zero");
        }
        let mut params = Params::new();
        params.insert("ord_type".to_string(), "market".to_string());
        params.insert("volume".to_string(), trim_fixed(qty));
        self.submit_order(Side::Sell, qty, params).await
    }

    async fn submit_order(
        &self,
        side: Side,
        qty: f64,
        mut params: Params,
    ) -> anyhow::Result<ActiveOrder> {
        let order_id = next_order_id();
        params.insert("market".to_string(), self.market.clone());
        params.insert("side".to_string(), upbit_side(side).to_string());
        params.insert("identifier".to_string(), order_id.to_string());
        let order: WireOrder = self.signed_post("/v1/orders", params, Pace::Order).await?;
        self.orders.lock().expect("orders lock").insert(order_id, order.uuid);
        Ok(ActiveOrder { order_id, side, qty })
    }

    /// Query one order (`None` when the venue does not know it). Falls back
    /// to Upbit's `identifier` lookup for orders placed by an earlier
    /// process of this capsule.
    pub async fn fetch_order(&self, order_id: u64) -> anyhow::Result<Option<OrderInfo>> {
        match self
            .signed_get::<WireOrder>("/v1/order", Some(self.order_params(order_id)), Pace::Exchange)
            .await
        {
            Ok(order) => Ok(Some(order_info(&order, order_id))),
            Err(err) if is_not_found(&err) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Cancel one order.
    pub async fn cancel_order(&self, order_id: u64) -> anyhow::Result<()> {
        self.signed_delete::<WireOrder>("/v1/order", self.order_params(order_id), Pace::Order)
            .await?;
        Ok(())
    }

    /// Open (`wait`) orders on this market. Orders placed by this capsule
    /// carry their client id; foreign orders have `order_id == 0`.
    pub async fn open_orders(&self) -> anyhow::Result<Vec<OrderInfo>> {
        let orders = self.open_orders_wire().await?;
        let by_uuid: HashMap<String, u64> = self
            .orders
            .lock()
            .expect("orders lock")
            .iter()
            .map(|(id, uuid)| (uuid.clone(), *id))
            .collect();
        Ok(orders
            .iter()
            .map(|order| order_info(order, by_uuid.get(&order.uuid).copied().unwrap_or(0)))
            .collect())
    }

    /// Cancel every open order on this market; returns how many cancels
    /// succeeded.
    pub async fn cancel_all_orders(&self) -> anyhow::Result<u32> {
        let mut cancelled = 0;
        for order in self.open_orders_wire().await? {
            let mut params = Params::new();
            params.insert("uuid".to_string(), order.uuid.clone());
            if self.signed_delete::<WireOrder>("/v1/order", params, Pace::Order).await.is_ok() {
                cancelled += 1;
            }
        }
        Ok(cancelled)
    }

    async fn open_orders_wire(&self) -> anyhow::Result<Vec<WireOrder>> {
        let mut params = Params::new();
        params.insert("market".to_string(), self.market.clone());
        params.insert("state".to_string(), "wait".to_string());
        self.signed_get("/v1/orders", Some(params), Pace::Exchange).await
    }

    fn order_params(&self, order_id: u64) -> Params {
        let mut params = Params::new();
        match self.orders.lock().expect("orders lock").get(&order_id) {
            Some(uuid) => params.insert("uuid".to_string(), uuid.clone()),
            None => params.insert("identifier".to_string(), order_id.to_string()),
        };
        params
    }

    // -- transfers -----------------------------------------------------------

    /// Recent withdrawals (`currency = None` for all).
    pub async fn list_withdraws(
        &self,
        currency: Option<&str>,
        page: u32,
        limit: u32,
    ) -> anyhow::Result<Vec<Withdrawal>> {
        self.signed_get(
            "/v1/withdraws",
            Some(transfer_params(currency, page, limit)),
            Pace::Exchange,
        )
        .await
    }

    /// Recent deposits (`currency = None` for all).
    pub async fn list_deposits(
        &self,
        currency: Option<&str>,
        page: u32,
        limit: u32,
    ) -> anyhow::Result<Vec<Deposit>> {
        self.signed_get(
            "/v1/deposits",
            Some(transfer_params(currency, page, limit)),
            Pace::Exchange,
        )
        .await
    }

    /// Raw `/v1/deposits/coin_address` response for `currency` on `net_type`.
    pub async fn deposit_address_raw(&self, currency: &str, net_type: &str) -> anyhow::Result<Value> {
        let mut params = Params::new();
        params.insert("currency".to_string(), currency.to_uppercase());
        params.insert("net_type".to_string(), net_type.to_string());
        self.signed_get("/v1/deposits/coin_address", Some(params), Pace::Exchange).await
    }

    /// Raw `/v1/withdraws/chance` response (limits, fees, decimals).
    pub async fn withdraw_chance_raw(
        &self,
        currency: &str,
        net_type: Option<&str>,
    ) -> anyhow::Result<Value> {
        let mut params = Params::new();
        params.insert("currency".to_string(), currency.to_uppercase());
        if let Some(net_type) = net_type {
            params.insert("net_type".to_string(), net_type.to_string());
        }
        self.signed_get("/v1/withdraws/chance", Some(params), Pace::Exchange).await
    }

    /// Submit an on-chain withdrawal. `amount` is a decimal string —
    /// transfers must not round through f64.
    pub async fn withdraw_coin_raw(
        &self,
        currency: &str,
        net_type: &str,
        amount: &str,
        address: &str,
        secondary_address: Option<&str>,
    ) -> anyhow::Result<Value> {
        let mut params = Params::new();
        params.insert("currency".to_string(), currency.to_uppercase());
        params.insert("net_type".to_string(), net_type.to_string());
        params.insert("amount".to_string(), amount.to_string());
        params.insert("address".to_string(), address.to_string());
        if let Some(secondary) = secondary_address.filter(|s| !s.trim().is_empty()) {
            params.insert("secondary_address".to_string(), secondary.to_string());
        }
        params.insert("transaction_type".to_string(), "default".to_string());
        self.signed_post("/v1/withdraws/coin", params, Pace::Exchange).await
    }
}

impl std::fmt::Debug for Upbit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Upbit")
            .field("market", &self.market)
            .field("trading", &self.trading)
            .field("access_key", &"***")
            .field("secret_key", &"***")
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------

/// Stream managed orderbooks for `market` into `tx`.
///
/// Upbit's public WS sends full snapshots per message, so every update is an
/// independently synced [`OrderBook`] (no diff/resync bookkeeping). Answers
/// protocol pings and sends the venue's text `PING` keepalive. Returns
/// `Ok(())` when the receiver is dropped; reconnection is the caller's loop.
pub async fn stream_book(market: &str, tx: mpsc::Sender<BookUpdate>) -> anyhow::Result<()> {
    let (ws, _) = connect_async(WS_PUBLIC_URL).await?;
    let (mut write, mut read) = ws.split();
    let subscribe = json!([
        { "ticket": format!("rusk-{}", now_ms()) },
        { "type": "orderbook", "codes": [market.to_uppercase()] },
        { "format": "DEFAULT" },
    ]);
    write.send(Message::Text(subscribe.to_string().into())).await?;

    let mut keepalive = tokio::time::interval(WS_KEEPALIVE);
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = keepalive.tick() => write.send(Message::Text("PING".into())).await?,
            msg = read.next() => {
                let msg = msg.context("Upbit orderbook WebSocket ended")??;
                let data: tokio_tungstenite::tungstenite::Bytes = match msg {
                    Message::Binary(data) => data,
                    Message::Text(text) => text.into(),
                    Message::Ping(payload) => {
                        write.send(Message::Pong(payload)).await?;
                        continue;
                    }
                    Message::Close(frame) => bail!("Upbit orderbook WebSocket closed: {frame:?}"),
                    _ => continue,
                };
                if !forward_book(&data, &tx).await? {
                    return Ok(());
                }
            }
        }
    }
}

/// `false` when the receiver hung up (stream should end cleanly).
async fn forward_book(data: &[u8], tx: &mpsc::Sender<BookUpdate>) -> anyhow::Result<bool> {
    // Non-JSON and status/keepalive frames are ignored silently.
    let Ok(value) = serde_json::from_slice::<Value>(data) else {
        return Ok(true);
    };
    if value.get("type").and_then(Value::as_str) != Some("orderbook") {
        return Ok(true);
    }
    let wire: WireWsOrderbook =
        serde_json::from_value(value).context("Upbit WS orderbook parse")?;
    let book = book_from_units(&wire.orderbook_units);
    Ok(tx.send(BookUpdate::Book(book)).await.is_ok())
}

/// Stream private `myOrder` / `myAsset` events into `tx`.
///
/// Authenticates the WS handshake with a JWT `Authorization` header; needs
/// `UPBIT_ACCESS_KEY` / `UPBIT_SECRET_KEY`. Returns `Ok(())` when the
/// receiver is dropped; reconnection is the caller's loop.
pub async fn stream_user_events(tx: mpsc::Sender<UserEvent>) -> anyhow::Result<()> {
    crate::load_dotenv();
    let access_key = env::var("UPBIT_ACCESS_KEY").context("UPBIT_ACCESS_KEY is required")?;
    let secret_key = env::var("UPBIT_SECRET_KEY").context("UPBIT_SECRET_KEY is required")?;

    let mut request = WS_PRIVATE_URL.into_client_request()?;
    let token = jwt(&access_key, &secret_key, None)?;
    request.headers_mut().insert("Authorization", token.parse()?);
    let (ws, _) = connect_async(request).await?;
    let (mut write, mut read) = ws.split();

    let subscribe = json!([
        { "ticket": format!("rusk-private-{}", now_ms()) },
        { "type": "myOrder" },
        { "type": "myAsset" },
        { "format": "DEFAULT" },
    ]);
    write.send(Message::Text(subscribe.to_string().into())).await?;

    let mut keepalive = tokio::time::interval(WS_KEEPALIVE);
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = keepalive.tick() => write.send(Message::Text("PING".into())).await?,
            msg = read.next() => {
                let msg = msg.context("Upbit private WebSocket ended")??;
                let data: tokio_tungstenite::tungstenite::Bytes = match msg {
                    Message::Binary(data) => data,
                    Message::Text(text) => text.into(),
                    Message::Ping(payload) => {
                        write.send(Message::Pong(payload)).await?;
                        continue;
                    }
                    Message::Close(frame) => bail!("Upbit private WebSocket closed: {frame:?}"),
                    _ => continue,
                };
                if !forward_user_event(&data, &tx).await {
                    return Ok(());
                }
            }
        }
    }
}

async fn forward_user_event(data: &[u8], tx: &mpsc::Sender<UserEvent>) -> bool {
    let Ok(value) = serde_json::from_slice::<Value>(data) else {
        return true;
    };
    let event = match value.get("type").and_then(Value::as_str) {
        Some("myOrder") => serde_json::from_value::<WsOrder>(value).ok().map(UserEvent::Order),
        Some("myAsset") => serde_json::from_value::<WsAsset>(value).ok().map(UserEvent::Asset),
        // `{"status":"UP"}` keepalive responses and anything else: ignore.
        _ => None,
    };
    match event {
        Some(event) => tx.send(event).await.is_ok(),
        None => true,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Upbit's standard KRW-market price tick for `price`. A few pairs have
/// venue-specific finer-tick exceptions not modeled here.
pub fn krw_tick_size(price: f64) -> f64 {
    match price {
        p if p >= 2_000_000.0 => 1_000.0,
        p if p >= 1_000_000.0 => 500.0,
        p if p >= 500_000.0 => 100.0,
        p if p >= 100_000.0 => 50.0,
        p if p >= 10_000.0 => 10.0,
        p if p >= 1_000.0 => 1.0,
        p if p >= 100.0 => 0.1,
        p if p >= 10.0 => 0.01,
        p if p >= 1.0 => 0.001,
        p if p >= 0.1 => 0.000_1,
        p if p >= 0.01 => 0.000_01,
        p if p >= 0.001 => 0.000_001,
        p if p >= 0.000_1 => 0.000_000_1,
        _ => 0.000_000_01,
    }
}

fn round_to_krw_tick(price: f64) -> f64 {
    let tick = krw_tick_size(price);
    (price / tick).round() * tick
}

fn format_krw_price(price: f64) -> String {
    format!("{:.*}", tick_decimals(krw_tick_size(price)), price)
}

fn tick_decimals(tick: f64) -> usize {
    if tick >= 1.0 { 0 } else { (-tick.log10()).round() as usize }
}

/// Truncate onto Upbit's 8-decimal volume grid (never round a size up).
fn truncate_qty(qty: f64) -> f64 {
    (qty * 1e8).floor() / 1e8
}

/// 8-decimal fixed-point with trailing zeros trimmed (Upbit rejects
/// scientific notation).
fn trim_fixed(value: f64) -> String {
    let text = format!("{value:.8}");
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}

fn upbit_side(side: Side) -> &'static str {
    match side {
        Side::Buy => "bid",
        Side::Sell => "ask",
    }
}

fn order_info(order: &WireOrder, order_id: u64) -> OrderInfo {
    let parse = |field: &Option<String>| {
        field.as_deref().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0)
    };
    OrderInfo {
        order_id,
        state: match order.state.as_str() {
            "wait" | "watch" => OrderState::Open,
            "done" => OrderState::Filled,
            "cancel" => OrderState::Canceled,
            // Upbit has no explicit rejected state on this endpoint; treat
            // anything unknown as rejected rather than pretending it's live.
            _ => OrderState::Rejected,
        },
        side: if order.side == "bid" { Side::Buy } else { Side::Sell },
        limit_px: parse(&order.price),
        filled_qty: parse(&order.executed_volume),
        remaining_qty: parse(&order.remaining_volume),
    }
}

fn book_from_units(units: &[WireOrderbookUnit]) -> OrderBook {
    OrderBook::from_sides(
        units.iter().map(|u| (u.bid_price, u.bid_size)).collect(),
        units.iter().map(|u| (u.ask_price, u.ask_size)).collect(),
    )
}

fn transfer_params(currency: Option<&str>, page: u32, limit: u32) -> Params {
    let mut params = Params::new();
    if let Some(currency) = currency {
        params.insert("currency".to_string(), currency.to_uppercase());
    }
    params.insert("page".to_string(), page.to_string());
    params.insert("limit".to_string(), limit.to_string());
    params.insert("order_by".to_string(), "desc".to_string());
    params
}

async fn parse_response<T: serde::de::DeserializeOwned>(
    resp: reqwest::Response,
    what: &str,
) -> anyhow::Result<T> {
    let status = resp.status();
    let body = resp.text().await?;
    if status.as_u16() == 429 {
        bail!("Upbit {what} rate limited (HTTP 429)");
    }
    if !status.is_success() {
        if let Ok(err) = serde_json::from_str::<WireError>(&body) {
            bail!(
                "Upbit {what} failed (HTTP {}): {}: {}",
                status.as_u16(),
                error_name(&err.error.name),
                err.error.message
            );
        }
        bail!("Upbit {what} failed (HTTP {}): {body}", status.as_u16());
    }
    serde_json::from_str(&body).with_context(|| format!("Upbit {what}: unexpected response: {body}"))
}

fn error_name(name: &Value) -> String {
    name.as_str().map(str::to_string).unwrap_or_else(|| name.to_string())
}

fn is_not_found(err: &anyhow::Error) -> bool {
    let text = err.to_string();
    text.contains("order_not_found") || text.contains("(HTTP 404)")
}

fn next_order_id() -> u64 {
    now_ms() * 1_000 + ORDER_SEQ.fetch_add(1, Ordering::Relaxed) % 1_000
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_string_is_sorted_and_form_encoded() {
        let mut params = Params::new();
        params.insert("z_param".to_string(), "last".to_string());
        params.insert("a_param".to_string(), "value with spaces".to_string());
        params.insert("m_param".to_string(), "a&b=c".to_string());
        assert_eq!(
            query_string(&params),
            "a_param=value+with+spaces&m_param=a%26b%3Dc&z_param=last"
        );
        assert_eq!(form_encode("hello"), "hello");
        assert_eq!(form_encode("a=b&c=d"), "a%3Db%26c%3Dd");
        assert_eq!(form_encode("KRW-BTC"), "KRW-BTC");
    }

    /// Golden test carried over from the original wrapper: byte-for-byte JWT
    /// compatibility with the reference Go (jwt-go) implementation.
    #[test]
    fn jwt_matches_reference_implementation() {
        let mut params = Params::new();
        params.insert("market".to_string(), "KRW-USDT".to_string());
        params.insert("side".to_string(), "bid".to_string());
        params.insert("ord_type".to_string(), "limit".to_string());
        params.insert("price".to_string(), "1467".to_string());
        params.insert("volume".to_string(), "6.81663258".to_string());
        params.insert("identifier".to_string(), "test-identifier".to_string());

        assert_eq!(
            query_string(&params),
            "identifier=test-identifier&market=KRW-USDT&ord_type=limit&price=1467&side=bid&volume=6.81663258"
        );
        assert!(
            sha512_hex(&query_string(&params))
                .starts_with("d2a100e5e6845493233fc292e17bafeaf641c771a801770958e49ca05ac68939")
        );

        let token =
            jwt_with_nonce("test_access_key", "test_secret_key", Some(&params), "test-nonce-uuid")
                .unwrap();
        assert_eq!(
            token,
            "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3Nfa2V5IjoidGVzdF9hY2Nlc3Nfa2V5Iiwibm9uY2UiOiJ0ZXN0LW5vbmNlLXV1aWQiLCJxdWVyeV9oYXNoIjoiZDJhMTAwZTVlNjg0NTQ5MzIzM2ZjMjkyZTE3YmFmZWFmNjQxYzc3MWE4MDE3NzA5NThlNDljYTA1YWM2ODkzOWVkMWFjOTZmODczMjc2ODI1NjZhMWJhOWM5MDI0MGZmOTM4M2E2M2U5MGVkOGUxZjQwMTE2OWU1ZTg4MTgwZWQiLCJxdWVyeV9oYXNoX2FsZyI6IlNIQTUxMiJ9.yNl_FGBZ5tDsqUsE9zqwGwwzE2yuDtYVYxesyDWIfxw"
        );
    }

    #[test]
    fn jwt_without_params_skips_query_hash() {
        let token = jwt_with_nonce("ak", "sk", None, "nonce").unwrap();
        let jwt = token.strip_prefix("Bearer ").unwrap();
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3);
        let payload =
            String::from_utf8(URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
        let payload: Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(payload["access_key"], "ak");
        assert_eq!(payload["nonce"], "nonce");
        assert!(payload.get("query_hash").is_none());

        // Empty params behave like no params.
        let empty = Params::new();
        let token = jwt_with_nonce("ak", "sk", Some(&empty), "nonce").unwrap();
        assert!(!token.contains("query_hash"));
    }

    #[test]
    fn krw_ticks_follow_the_standard_table() {
        assert_eq!(krw_tick_size(150_000_000.0), 1_000.0);
        assert_eq!(krw_tick_size(1_500_000.0), 500.0);
        assert_eq!(krw_tick_size(700_000.0), 100.0);
        assert_eq!(krw_tick_size(150_000.0), 50.0);
        assert_eq!(krw_tick_size(50_000.0), 10.0);
        assert_eq!(krw_tick_size(1_467.0), 1.0);
        assert_eq!(krw_tick_size(500.0), 0.1);
        assert_eq!(krw_tick_size(50.0), 0.01);
        assert_eq!(krw_tick_size(5.0), 0.001);
        assert_eq!(krw_tick_size(0.000_05), 0.000_000_01);

        assert_eq!(round_to_krw_tick(150_000_123.0), 150_000_000.0);
        assert_eq!(format_krw_price(round_to_krw_tick(150_000_123.0)), "150000000");
        assert_eq!(format_krw_price(round_to_krw_tick(1_467.4)), "1467");
        assert_eq!(format_krw_price(round_to_krw_tick(523.456)), "523.5");
    }

    #[test]
    fn sizes_truncate_and_format_plainly() {
        assert_eq!(trim_fixed(truncate_qty(6.816632589)), "6.81663258");
        assert_eq!(trim_fixed(1.0), "1");
        assert_eq!(trim_fixed(0.100000004), "0.1");
        assert_eq!(truncate_qty(0.000000004), 0.0);
    }

    #[test]
    fn wire_orders_normalize_to_order_info() {
        let wire: WireOrder = serde_json::from_str(
            r#"{
                "uuid": "u-1",
                "side": "bid",
                "ord_type": "limit",
                "price": "1400",
                "state": "wait",
                "market": "KRW-USDT",
                "created_at": "2024-01-01T00:00:00+09:00",
                "volume": "100",
                "remaining_volume": "50",
                "executed_volume": "50"
            }"#,
        )
        .unwrap();
        let info = order_info(&wire, 42);
        assert_eq!(info.order_id, 42);
        assert_eq!(info.state, OrderState::Open);
        assert_eq!(info.side, Side::Buy);
        assert_eq!(info.limit_px, 1400.0);
        assert_eq!(info.filled_qty, 50.0);
        assert_eq!(info.remaining_qty, 50.0);

        for (state, expected) in [
            ("watch", OrderState::Open),
            ("done", OrderState::Filled),
            ("cancel", OrderState::Canceled),
            ("garbage", OrderState::Rejected),
        ] {
            let mut wire = wire.clone();
            wire.state = state.to_string();
            assert_eq!(order_info(&wire, 0).state, expected, "{state}");
        }
    }

    #[test]
    fn ws_orderbook_snapshots_become_synced_books() {
        let value: Value = serde_json::from_str(
            r#"{
                "type": "orderbook",
                "code": "KRW-BTC",
                "timestamp": 1704067200000,
                "total_ask_size": 1.5,
                "total_bid_size": 2.0,
                "orderbook_units": [
                    {"ask_price": 50001000.0, "bid_price": 50000000.0, "ask_size": 1.5, "bid_size": 2.0},
                    {"ask_price": 50002000.0, "bid_price": 49999000.0, "ask_size": 0.5, "bid_size": 1.0}
                ]
            }"#,
        )
        .unwrap();
        let wire: WireWsOrderbook = serde_json::from_value(value).unwrap();
        let book = book_from_units(&wire.orderbook_units);
        assert!(book.is_synced());
        assert_eq!(book.best_bid(), Some(50_000_000.0));
        assert_eq!(book.best_ask(), Some(50_001_000.0));
    }

    #[test]
    fn error_names_render_for_strings_and_numbers() {
        assert_eq!(error_name(&json!("order_not_found")), "order_not_found");
        assert_eq!(error_name(&json!(404)), "404");
        assert!(is_not_found(&anyhow::anyhow!(
            "Upbit /v1/order failed (HTTP 404): 404: not found"
        )));
        assert!(is_not_found(&anyhow::anyhow!(
            "Upbit /v1/order failed (HTTP 400): order_not_found: gone"
        )));
        assert!(!is_not_found(&anyhow::anyhow!("Upbit /v1/order failed (HTTP 401): bad jwt")));
    }
}
