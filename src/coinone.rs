//! Coinone spot venue capsule, ported from the kimp-arbitrager Coinone
//! wrapper onto the [`crate::upbit`] / [`crate::binance`] capsule surface.
//!
//! - [`Coinone`] — REST capsule for a single market (`KRW-BTC` style codes):
//!   place/cancel/fetch orders, open orders, balances, trade fees
//!   (HMAC-signed) plus public quotation data (ticker / orderbook / trades /
//!   candles). Market metadata (price/qty units, minimum order amount) is
//!   fetched at connect and used to round orders onto the venue grids.
//! - [`stream_book`] — public WS `ORDERBOOK` channel. Coinone sends full
//!   snapshots, so every message becomes a fresh synced [`OrderBook`].
//! - [`stream_user_events`] — private WS (`MYORDER` / `MYASSET` channels)
//!   emitting [`UserEvent`]s.
//!
//! ## Signing
//!
//! Private REST/WS requests carry the JSON payload twice: base64-encoded in
//! the `X-COINONE-PAYLOAD` header (with `access_token` and a UUIDv4 `nonce`
//! injected) and signed with HMAC-SHA512 (hex) in `X-COINONE-SIGNATURE`.
//! The POST body is the *exact* same JSON string that was base64-encoded.
//! Even 2xx responses embed `result`/`error_code`, which are checked on
//! every call.
//!
//! ## Environment keys
//!
//! | Key | Meaning |
//! |-----|---------|
//! | `COINONE_ACCESS_TOKEN` | API access token (trading mode + private WS) |
//! | `COINONE_SECRET_KEY` | API secret key (trading mode + private WS) |
//!
//! Coinone references orders by a venue-side UUID string, but the shared
//! [`ActiveOrder::order_id`] is numeric — so the capsule generates a numeric
//! client id per order, sends it as Coinone's `user_order_id`, and tracks
//! the `id -> uuid` mapping internally. The venue does **not** echo
//! `user_order_id` on the active-order list, so ids resolve through this
//! capsule instance's map: orders from other processes show up in
//! [`Coinone::open_orders`] with `order_id == 0` (cancel still works for
//! them via [`Coinone::cancel_all_orders`], and [`Coinone::cancel_order`]
//! falls back to sending `user_order_id` for unmapped ids).

use std::collections::HashMap;
use std::env;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Deserializer};
use serde_json::{Value, json};
use sha2::Sha512;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::book::{BookUpdate, OrderBook};
use crate::order::{ActiveOrder, OrderInfo, OrderState, Side};

const BASE_URL: &str = "https://api.coinone.co.kr";
const WS_PUBLIC_URL: &str = "wss://stream.coinone.co.kr";
const WS_PRIVATE_URL: &str = "wss://stream.coinone.co.kr/v1/private";

/// Coinone WS keepalive (`{"request_type":"PING"}` text frames).
const WS_KEEPALIVE: Duration = Duration::from_secs(20);
/// Orderbook depth requested from REST (venue-accepted size).
const BOOK_SIZE: u32 = 15;

static ORDER_SEQ: AtomicU64 = AtomicU64::new(1);

// ---------------------------------------------------------------------------
// Request pacing (client-side floor under Coinone's per-group limits:
// public 300/10s ≈ 30/s, private orders 60/s, other private 30/s).
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Pace {
    Public,
    Private,
    Order,
}

impl Pace {
    fn interval(self) -> Duration {
        match self {
            Pace::Public => Duration::from_millis(50),
            Pace::Private => Duration::from_millis(50),
            Pace::Order => Duration::from_millis(25),
        }
    }
}

#[derive(Default)]
struct Pacer(Mutex<HashMap<Pace, Instant>>);

impl Pacer {
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
// Signing
// ---------------------------------------------------------------------------

/// Base64 payload + hex HMAC-SHA512 signature for the `X-COINONE-*` headers.
fn sign_payload(secret_key: &str, payload_json: &str) -> anyhow::Result<(String, String)> {
    let encoded = BASE64.encode(payload_json.as_bytes());
    let mut mac = Hmac::<Sha512>::new_from_slice(secret_key.as_bytes())
        .context("Coinone signing: bad HMAC key")?;
    mac.update(encoded.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());
    Ok((encoded, signature))
}

// ---------------------------------------------------------------------------
// Public response types
// ---------------------------------------------------------------------------

/// Cached market metadata from `/public/v2/markets/{quote}/{target}`.
#[derive(Debug, Clone)]
pub struct MarketInfo {
    /// Price tick (e.g. `10000.0` on KRW-BTC).
    pub price_unit: f64,
    /// Size grid (e.g. `0.00000001`).
    pub qty_unit: f64,
    /// Minimum order value in quote currency (e.g. `5000.0` KRW).
    pub min_order_amount: f64,
    pub min_qty: f64,
}

/// One account balance line.
#[derive(Debug, Clone)]
pub struct Balance {
    pub currency: String,
    /// Free balance.
    pub available: f64,
    /// Amount locked in orders/withdrawals (Coinone calls this `limit`).
    pub locked: f64,
    pub average_price: f64,
}

/// `/public/v2/ticker_new/{quote}/{target}` entry.
#[derive(Debug, Clone)]
pub struct Ticker {
    pub market: String,
    pub last: f64,
    pub high: f64,
    pub low: f64,
    pub quote_volume: f64,
    pub target_volume: f64,
    pub timestamp: i64,
}

/// `/public/v2/trades/{quote}/{target}` entry.
#[derive(Debug, Clone)]
pub struct TradeTick {
    pub timestamp: i64,
    pub price: f64,
    pub qty: f64,
    pub is_seller_maker: bool,
}

/// `/public/v2/chart/{quote}/{target}` candle.
#[derive(Debug, Clone)]
pub struct Candle {
    pub timestamp: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub target_volume: f64,
    pub quote_volume: f64,
}

/// Private WS `MYORDER` event (tolerant: every field defaults).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct WsOrder {
    #[serde(default, deserialize_with = "de_opt_string")]
    pub order_id: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub user_order_id: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub trade_id: Option<String>,
    #[serde(default, rename = "type", deserialize_with = "de_opt_string")]
    pub order_type: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub status: Option<String>,
    /// `"BUY"` / `"SELL"` (REST style) or `"BID"` / `"ASK"` (WS style).
    #[serde(default, deserialize_with = "de_opt_string")]
    pub side: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub quote_currency: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub target_currency: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub executed_price: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub executed_qty: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub remain_qty: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub timestamp: Option<String>,
}

/// Private WS `MYASSET` event.
#[derive(Debug, Clone, Deserialize)]
pub struct WsAsset {
    #[serde(default, deserialize_with = "de_opt_string")]
    pub order_id: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    pub user_order_id: Option<String>,
    #[serde(default)]
    pub assets: Vec<WsAssetItem>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WsAssetItem {
    pub currency: String,
    #[serde(default, deserialize_with = "de_string")]
    pub available: String,
    /// Locked amount (Coinone calls this `limit`).
    #[serde(default, deserialize_with = "de_string", rename = "limit")]
    pub locked: String,
}

/// An event from [`stream_user_events`].
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum UserEvent {
    Order(WsOrder),
    Asset(WsAsset),
}

// ---------------------------------------------------------------------------
// Wire plumbing
// ---------------------------------------------------------------------------

/// Coinone mixes strings and numbers for the same fields across endpoints.
fn de_string<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    let value = Value::deserialize(deserializer)?;
    Ok(value_to_string(value))
}

fn de_opt_string<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<String>, D::Error> {
    let value = Option::<Value>::deserialize(deserializer)?;
    Ok(value.map(value_to_string))
}

fn value_to_string(value: Value) -> String {
    match value {
        Value::String(text) => text,
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

#[derive(Debug, Clone, Deserialize)]
struct WireOrder {
    #[serde(default, deserialize_with = "de_opt_string")]
    order_id: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    user_order_id: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    status: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    side: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    price: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    remain_qty: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    executed_qty: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    traded_qty: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WireLevel {
    #[serde(default, deserialize_with = "de_string")]
    price: String,
    #[serde(default, deserialize_with = "de_string")]
    qty: String,
}

#[derive(Debug, Deserialize)]
struct WireBookSides {
    #[serde(default)]
    bids: Vec<WireLevel>,
    #[serde(default)]
    asks: Vec<WireLevel>,
}

#[derive(Debug, Deserialize)]
struct WireEnvelope {
    #[serde(default, deserialize_with = "de_opt_string")]
    response_type: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string")]
    channel: Option<String>,
    #[serde(default)]
    data: Option<Value>,
}

// ---------------------------------------------------------------------------
// Capsule
// ---------------------------------------------------------------------------

/// Coinone spot capsule for a single market.
pub struct Coinone {
    market: String,
    quote: String,
    target: String,
    access_token: String,
    secret_key: String,
    trading: bool,
    http: reqwest::Client,
    pacer: Pacer,
    info: MarketInfo,
    /// Client order id (our numeric `user_order_id`) → venue-side order UUID.
    orders: Mutex<HashMap<u64, String>>,
}

impl Coinone {
    /// Public-data-only capsule (no credentials required). `market` uses
    /// `QUOTE-TARGET` codes, e.g. `KRW-BTC`; metadata is fetched (and the
    /// market validated) at connect.
    pub async fn connect(market: &str) -> anyhow::Result<Self> {
        Self::build(market, false).await
    }

    /// Trading capsule. Needs `COINONE_ACCESS_TOKEN` / `COINONE_SECRET_KEY`.
    pub async fn connect_trading(market: &str) -> anyhow::Result<Self> {
        Self::build(market, true).await
    }

    async fn build(market: &str, trading: bool) -> anyhow::Result<Self> {
        crate::load_dotenv();
        let market = market.to_uppercase();
        let (quote, target) = market
            .split_once('-')
            .with_context(|| format!("Coinone market must be QUOTE-TARGET (e.g. KRW-BTC), got {market}"))?;
        let (access_token, secret_key) = if trading {
            (
                env::var("COINONE_ACCESS_TOKEN").context("COINONE_ACCESS_TOKEN is required")?,
                env::var("COINONE_SECRET_KEY").context("COINONE_SECRET_KEY is required")?,
            )
        } else {
            (
                env::var("COINONE_ACCESS_TOKEN").unwrap_or_default(),
                env::var("COINONE_SECRET_KEY").unwrap_or_default(),
            )
        };
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context("Coinone HTTP client")?;
        let mut coinone = Self {
            quote: quote.to_string(),
            target: target.to_string(),
            market,
            access_token,
            secret_key,
            trading,
            http,
            pacer: Pacer::default(),
            info: MarketInfo {
                price_unit: 0.0,
                qty_unit: 0.0,
                min_order_amount: 0.0,
                min_qty: 0.0,
            },
            orders: Mutex::new(HashMap::new()),
        };
        coinone.info = coinone
            .fetch_market_info()
            .await
            .with_context(|| format!("Coinone market {} is not tradable", coinone.market))?;
        Ok(coinone)
    }

    /// Market code (`KRW-BTC` style).
    pub fn market(&self) -> &str {
        &self.market
    }

    /// Quote currency (`KRW` of `KRW-BTC`).
    pub fn quote(&self) -> &str {
        &self.quote
    }

    /// Target (base) asset (`BTC` of `KRW-BTC`).
    pub fn target(&self) -> &str {
        &self.target
    }

    /// Market grids/minimums cached at connect.
    pub fn market_info(&self) -> MarketInfo {
        self.info.clone()
    }

    fn require_trading(&self) -> anyhow::Result<()> {
        if self.trading {
            Ok(())
        } else {
            bail!("Coinone capsule is read-only; use connect_trading for signed endpoints")
        }
    }

    // -- plumbing ------------------------------------------------------------

    async fn get_public(&self, path: &str, query: &[(&str, String)]) -> anyhow::Result<Value> {
        self.pacer.acquire(Pace::Public).await;
        let resp = self
            .http
            .get(format!("{BASE_URL}{path}"))
            .query(query)
            .send()
            .await?;
        parse_response(resp, path).await
    }

    /// Signed POST: `access_token` + UUIDv4 `nonce` injected into `payload`;
    /// headers and body carry the identical JSON string (see module docs).
    async fn post_signed(&self, path: &str, mut payload: Value, pace: Pace) -> anyhow::Result<Value> {
        self.require_trading()?;
        self.pacer.acquire(pace).await;
        let object = payload.as_object_mut().context("Coinone payload must be a JSON object")?;
        object.insert("access_token".to_string(), json!(self.access_token));
        object.insert("nonce".to_string(), json!(uuid::Uuid::new_v4().to_string()));
        let body = payload.to_string();
        let (encoded, signature) = sign_payload(&self.secret_key, &body)?;
        let resp = self
            .http
            .post(format!("{BASE_URL}{path}"))
            .header("Content-Type", "application/json")
            .header("X-COINONE-PAYLOAD", encoded)
            .header("X-COINONE-SIGNATURE", signature)
            .body(body)
            .send()
            .await?;
        parse_response(resp, path).await
    }

    async fn fetch_market_info(&self) -> anyhow::Result<MarketInfo> {
        let value = self
            .get_public(&format!("/public/v2/markets/{}/{}", self.quote, self.target), &[])
            .await?;
        let market = value
            .get("markets")
            .and_then(Value::as_array)
            .and_then(|markets| markets.first())
            .with_context(|| format!("Coinone markets: no entry for {}", self.market))?;
        let field = |key: &str| {
            market
                .get(key)
                .map(|v| value_to_string(v.clone()))
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0)
        };
        if market.get("trade_status").and_then(Value::as_i64) == Some(0) {
            bail!("Coinone market {} trading is suspended", self.market);
        }
        Ok(MarketInfo {
            price_unit: field("price_unit"),
            qty_unit: field("qty_unit"),
            min_order_amount: field("min_order_amount"),
            min_qty: field("min_qty"),
        })
    }

    // -- public quotation data ---------------------------------------------

    /// Latest ticker for this market.
    pub async fn ticker(&self) -> anyhow::Result<Ticker> {
        let value = self
            .get_public(&format!("/public/v2/ticker_new/{}/{}", self.quote, self.target), &[])
            .await?;
        let ticker = value
            .get("tickers")
            .and_then(Value::as_array)
            .and_then(|tickers| tickers.first())
            .context("Coinone ticker_new: empty response")?;
        Ok(Ticker {
            market: self.market.clone(),
            last: str_field(ticker, "last"),
            high: str_field(ticker, "high"),
            low: str_field(ticker, "low"),
            quote_volume: str_field(ticker, "quote_volume"),
            target_volume: str_field(ticker, "target_volume"),
            timestamp: ticker.get("timestamp").and_then(Value::as_i64).unwrap_or_default(),
        })
    }

    /// REST orderbook snapshot as a synced [`OrderBook`].
    pub async fn book(&self) -> anyhow::Result<OrderBook> {
        let value = self
            .get_public(
                &format!("/public/v2/orderbook/{}/{}", self.quote, self.target),
                &[("size", BOOK_SIZE.to_string())],
            )
            .await?;
        let sides: WireBookSides = serde_json::from_value(value).context("Coinone orderbook parse")?;
        book_from_sides(&sides)
    }

    /// Mid of the REST book's best bid/ask.
    pub async fn mid_price(&self) -> anyhow::Result<f64> {
        let book = self.book().await?;
        book.mid().context("Coinone book has no mid (one side empty)")
    }

    /// Most recent trades (newest first). `size` must be a venue-accepted
    /// value (10 / 50 / 100 / 150 / 200).
    pub async fn recent_trades(&self, size: u32) -> anyhow::Result<Vec<TradeTick>> {
        let value = self
            .get_public(
                &format!("/public/v2/trades/{}/{}", self.quote, self.target),
                &[("size", size.to_string())],
            )
            .await?;
        let trades = value
            .get("transactions")
            .and_then(Value::as_array)
            .context("Coinone trades: missing transactions")?;
        Ok(trades
            .iter()
            .map(|t| TradeTick {
                timestamp: str_field(t, "timestamp") as i64,
                price: str_field(t, "price"),
                qty: str_field(t, "qty"),
                is_seller_maker: t.get("is_seller_maker").and_then(Value::as_bool).unwrap_or(false),
            })
            .collect())
    }

    /// Candles, newest first (`interval` like `1m`, `5m`, `1h`, `1d`).
    pub async fn candles(&self, interval: &str, size: u32) -> anyhow::Result<Vec<Candle>> {
        let value = self
            .get_public(
                &format!("/public/v2/chart/{}/{}", self.quote, self.target),
                &[("interval", interval.to_string()), ("size", size.to_string())],
            )
            .await?;
        let candles = value
            .get("chart")
            .and_then(Value::as_array)
            .context("Coinone chart: missing candles")?;
        Ok(candles
            .iter()
            .map(|c| Candle {
                timestamp: c.get("timestamp").and_then(Value::as_i64).unwrap_or_default(),
                open: str_field(c, "open"),
                high: str_field(c, "high"),
                low: str_field(c, "low"),
                close: str_field(c, "close"),
                target_volume: str_field(c, "target_volume"),
                quote_volume: str_field(c, "quote_volume"),
            })
            .collect())
    }

    // -- account ------------------------------------------------------------

    /// All account balances.
    pub async fn balances(&self) -> anyhow::Result<Vec<Balance>> {
        let value = self
            .post_signed("/v2.1/account/balance/all", json!({}), Pace::Private)
            .await?;
        let balances = value
            .get("balances")
            .and_then(Value::as_array)
            .context("Coinone balances: missing list")?;
        Ok(balances.iter().map(balance_from_wire).collect())
    }

    /// Balance for one currency (`None` when the account never held it).
    pub async fn balance(&self, currency: &str) -> anyhow::Result<Option<Balance>> {
        let value = self
            .post_signed(
                "/v2.1/account/balance",
                json!({ "currencies": [currency.to_uppercase()] }),
                Pace::Private,
            )
            .await?;
        Ok(value
            .get("balances")
            .and_then(Value::as_array)
            .and_then(|balances| balances.first())
            .map(balance_from_wire))
    }

    /// `(maker, taker)` fee rates for this market, normalized to fractions
    /// (Coinone reports percent-style values for some accounts).
    pub async fn trade_fees(&self) -> anyhow::Result<(f64, f64)> {
        let value = self
            .post_signed(
                &format!("/v2.1/account/trade_fee/{}/{}", self.quote, self.target),
                json!({}),
                Pace::Private,
            )
            .await?;
        let fee = |key: &str| {
            normalize_fee_rate(&value.get(key).map(|v| value_to_string(v.clone())).unwrap_or_default())
        };
        Ok((fee("maker"), fee("taker")))
    }

    // -- orders --------------------------------------------------------------

    /// Place a resting limit order. Prices are rounded onto the market's
    /// `price_unit`, sizes truncated onto `qty_unit`; orders below
    /// `min_order_amount` are refused client-side.
    pub async fn place_limit(&self, side: Side, qty: f64, price: f64) -> anyhow::Result<ActiveOrder> {
        self.limit_order(side, qty, price, false).await
    }

    /// Limit order that only rests (`post_only`).
    pub async fn place_post_only(&self, side: Side, qty: f64, price: f64) -> anyhow::Result<ActiveOrder> {
        self.limit_order(side, qty, price, true).await
    }

    async fn limit_order(
        &self,
        side: Side,
        qty: f64,
        price: f64,
        post_only: bool,
    ) -> anyhow::Result<ActiveOrder> {
        self.require_trading()?;
        if price.is_nan() || price <= 0.0 {
            bail!("Coinone limit order price must be positive, got {price}");
        }
        let price = round_to_unit(price, self.info.price_unit);
        let qty = truncate_to_unit(qty, self.info.qty_unit);
        if qty.is_nan() || qty <= 0.0 {
            bail!("Coinone order size truncates to zero");
        }
        if qty < self.info.min_qty {
            bail!("Coinone order size {qty} is below the market minimum {}", self.info.min_qty);
        }
        if price * qty < self.info.min_order_amount {
            bail!(
                "Coinone order value {:.0} {} is below the venue minimum {}",
                price * qty,
                self.quote,
                self.info.min_order_amount
            );
        }
        let payload = json!({
            "type": "LIMIT",
            "price": format_unit(price, self.info.price_unit),
            "qty": trim_fixed(qty),
            "post_only": post_only,
        });
        self.submit_order(side, qty, payload).await
    }

    /// Market buy: spends `quote_total` (e.g. KRW) at market. The filled
    /// quantity is unknown until execution, so the returned
    /// [`ActiveOrder::qty`] is `0.0` — read it back with [`Self::fetch_order`].
    pub async fn market_buy(&self, quote_total: f64) -> anyhow::Result<ActiveOrder> {
        self.require_trading()?;
        if quote_total < self.info.min_order_amount {
            bail!(
                "Coinone market buy of {quote_total} {} is below the venue minimum {}",
                self.quote,
                self.info.min_order_amount
            );
        }
        let payload = json!({ "type": "MARKET", "amount": trim_fixed(quote_total) });
        self.submit_order(Side::Buy, 0.0, payload).await
    }

    /// Market sell of `qty` base units.
    pub async fn market_sell(&self, qty: f64) -> anyhow::Result<ActiveOrder> {
        self.require_trading()?;
        let qty = truncate_to_unit(qty, self.info.qty_unit);
        if qty.is_nan() || qty <= 0.0 {
            bail!("Coinone order size truncates to zero");
        }
        let payload = json!({ "type": "MARKET", "qty": trim_fixed(qty) });
        self.submit_order(Side::Sell, qty, payload).await
    }

    async fn submit_order(&self, side: Side, qty: f64, mut payload: Value) -> anyhow::Result<ActiveOrder> {
        let order_id = next_order_id();
        let object = payload.as_object_mut().context("order payload must be an object")?;
        object.insert("side".to_string(), json!(coinone_side(side)));
        object.insert("quote_currency".to_string(), json!(self.quote));
        object.insert("target_currency".to_string(), json!(self.target));
        object.insert("user_order_id".to_string(), json!(order_id.to_string()));
        let value = self.post_signed("/v2.1/order", payload, Pace::Order).await?;
        let uuid = value
            .get("order_id")
            .map(|v| value_to_string(v.clone()))
            .filter(|s| !s.is_empty())
            .context("Coinone /v2.1/order: missing order_id")?;
        self.orders.lock().expect("orders lock").insert(order_id, uuid);
        Ok(ActiveOrder { order_id, side, qty })
    }

    /// Query one order (`None` when the venue does not know it). Orders from
    /// an earlier process are recovered from the open-order list via their
    /// echoed `user_order_id`.
    pub async fn fetch_order(&self, order_id: u64) -> anyhow::Result<Option<OrderInfo>> {
        let uuid = match self.uuid_for(order_id).await? {
            Some(uuid) => uuid,
            None => return Ok(None),
        };
        match self
            .post_signed(
                "/v2.1/order/detail",
                json!({
                    "order_id": uuid,
                    "quote_currency": self.quote,
                    "target_currency": self.target,
                }),
                Pace::Private,
            )
            .await
        {
            Ok(value) => {
                let wire: WireOrder = serde_json::from_value(
                    value.get("order").cloned().context("Coinone order detail: missing order")?,
                )
                .context("Coinone order detail parse")?;
                Ok(Some(order_info(&wire, order_id)))
            }
            Err(err) if is_not_found(&err) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Cancel one order.
    pub async fn cancel_order(&self, order_id: u64) -> anyhow::Result<()> {
        let mut payload = json!({
            "quote_currency": self.quote,
            "target_currency": self.target,
        });
        match self.uuid_for(order_id).await? {
            Some(uuid) => payload["order_id"] = json!(uuid),
            // The venue also cancels by the echoed client id.
            None => payload["user_order_id"] = json!(order_id.to_string()),
        }
        self.post_signed("/v2.1/order/cancel", payload, Pace::Order).await?;
        Ok(())
    }

    /// Open orders on this market. Orders placed by this capsule instance
    /// carry their numeric client id (resolved via the internal UUID map —
    /// the venue does not echo `user_order_id` here); foreign orders have
    /// `order_id == 0`.
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
            .map(|order| {
                let id = order
                    .order_id
                    .as_deref()
                    .and_then(|uuid| by_uuid.get(uuid).copied())
                    .unwrap_or_else(|| client_id_of(order));
                order_info(order, id)
            })
            .collect())
    }

    /// Cancel every open order on this market; returns how many the venue
    /// reported cancelled.
    pub async fn cancel_all_orders(&self) -> anyhow::Result<u32> {
        let value = self
            .post_signed(
                "/v2.1/order/cancel/all",
                json!({
                    "quote_currency": self.quote,
                    "target_currency": self.target,
                }),
                Pace::Order,
            )
            .await?;
        Ok(value
            .get("active_orders")
            .and_then(Value::as_array)
            .map(|orders| orders.len() as u32)
            .unwrap_or(0))
    }

    async fn open_orders_wire(&self) -> anyhow::Result<Vec<WireOrder>> {
        let value = self
            .post_signed(
                "/v2.1/order/active_orders",
                json!({
                    "quote_currency": self.quote,
                    "target_currency": self.target,
                }),
                Pace::Private,
            )
            .await?;
        serde_json::from_value(value.get("active_orders").cloned().unwrap_or_else(|| json!([])))
            .context("Coinone active orders parse")
    }

    /// Venue-side UUID for `order_id` from the internal map (the venue does
    /// not echo `user_order_id` on the active-order list, so unmapped ids
    /// cannot be recovered by listing).
    async fn uuid_for(&self, order_id: u64) -> anyhow::Result<Option<String>> {
        Ok(self.orders.lock().expect("orders lock").get(&order_id).cloned())
    }
}

impl std::fmt::Debug for Coinone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Coinone")
            .field("market", &self.market)
            .field("trading", &self.trading)
            .field("access_token", &"***")
            .field("secret_key", &"***")
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------

/// Stream managed orderbooks for `market` into `tx`.
///
/// Subscribes the public WS `ORDERBOOK` channel; Coinone sends full
/// snapshots, so every message becomes an independently synced
/// [`OrderBook`]. Sends the venue's `{"request_type":"PING"}` keepalive.
/// Returns `Ok(())` when the receiver is dropped; reconnection is the
/// caller's loop.
pub async fn stream_book(market: &str, tx: mpsc::Sender<BookUpdate>) -> anyhow::Result<()> {
    let (quote, target) = market
        .to_uppercase()
        .split_once('-')
        .map(|(q, t)| (q.to_string(), t.to_string()))
        .with_context(|| format!("Coinone market must be QUOTE-TARGET, got {market}"))?;
    let (ws, _) = connect_async(WS_PUBLIC_URL).await?;
    let (mut write, mut read) = ws.split();
    let subscribe = json!({
        "request_type": "SUBSCRIBE",
        "channel": "ORDERBOOK",
        "topic": { "quote_currency": quote, "target_currency": target },
    });
    write.send(Message::Text(subscribe.to_string().into())).await?;

    let mut keepalive = tokio::time::interval(WS_KEEPALIVE);
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = keepalive.tick() => {
                write.send(Message::Text(json!({"request_type": "PING"}).to_string().into())).await?
            }
            msg = read.next() => {
                let msg = msg.context("Coinone orderbook WebSocket ended")??;
                let data: tokio_tungstenite::tungstenite::Bytes = match msg {
                    Message::Binary(data) => data,
                    Message::Text(text) => text.into(),
                    Message::Ping(payload) => {
                        write.send(Message::Pong(payload)).await?;
                        continue;
                    }
                    Message::Close(frame) => bail!("Coinone orderbook WebSocket closed: {frame:?}"),
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
    let Ok(envelope) = serde_json::from_slice::<WireEnvelope>(data) else {
        return Ok(true);
    };
    // CONNECTED / PONG / ERROR frames are ignored silently.
    if envelope.channel.as_deref() != Some("ORDERBOOK")
        || envelope.response_type.as_deref() != Some("DATA")
    {
        return Ok(true);
    }
    let Some(data) = envelope.data else {
        return Ok(true);
    };
    let sides: WireBookSides = serde_json::from_value(data).context("Coinone WS orderbook parse")?;
    Ok(tx.send(BookUpdate::Book(book_from_sides(&sides)?)).await.is_ok())
}

/// Stream private `MYORDER` / `MYASSET` events into `tx`.
///
/// Authenticates the WS handshake with `X-COINONE-PAYLOAD` /
/// `X-COINONE-SIGNATURE` headers; needs `COINONE_ACCESS_TOKEN` /
/// `COINONE_SECRET_KEY`. Returns `Ok(())` when the receiver is dropped;
/// reconnection is the caller's loop.
pub async fn stream_user_events(tx: mpsc::Sender<UserEvent>) -> anyhow::Result<()> {
    crate::load_dotenv();
    let access_token = env::var("COINONE_ACCESS_TOKEN").context("COINONE_ACCESS_TOKEN is required")?;
    let secret_key = env::var("COINONE_SECRET_KEY").context("COINONE_SECRET_KEY is required")?;

    let payload = json!({
        "access_token": access_token,
        "nonce": uuid::Uuid::new_v4().to_string(),
        "timestamp": now_ms(),
    });
    let (encoded, signature) = sign_payload(&secret_key, &payload.to_string())?;
    let mut request = WS_PRIVATE_URL.into_client_request()?;
    request.headers_mut().insert("X-COINONE-PAYLOAD", encoded.parse()?);
    request.headers_mut().insert("X-COINONE-SIGNATURE", signature.parse()?);
    let (ws, _) = connect_async(request).await?;
    let (mut write, mut read) = ws.split();

    for channel in ["MYORDER", "MYASSET"] {
        let subscribe = json!({ "request_type": "SUBSCRIBE", "channel": channel });
        write.send(Message::Text(subscribe.to_string().into())).await?;
    }

    let mut keepalive = tokio::time::interval(WS_KEEPALIVE);
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = keepalive.tick() => {
                write.send(Message::Text(json!({"request_type": "PING"}).to_string().into())).await?
            }
            msg = read.next() => {
                let msg = msg.context("Coinone private WebSocket ended")??;
                let data: tokio_tungstenite::tungstenite::Bytes = match msg {
                    Message::Binary(data) => data,
                    Message::Text(text) => text.into(),
                    Message::Ping(payload) => {
                        write.send(Message::Pong(payload)).await?;
                        continue;
                    }
                    Message::Close(frame) => bail!("Coinone private WebSocket closed: {frame:?}"),
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
    let Ok(envelope) = serde_json::from_slice::<WireEnvelope>(data) else {
        return true;
    };
    if envelope.response_type.as_deref() != Some("DATA") {
        return true;
    }
    let event = match (envelope.channel.as_deref(), envelope.data) {
        (Some("MYORDER"), Some(data)) => {
            serde_json::from_value::<WsOrder>(data).ok().map(UserEvent::Order)
        }
        (Some("MYASSET"), Some(data)) => {
            serde_json::from_value::<WsAsset>(data).ok().map(UserEvent::Asset)
        }
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

/// Normalize Coinone fee rates: some accounts report percent-style values
/// (`0.2` meaning 0.2%), others fractions (`0.002`).
pub fn normalize_fee_rate(raw: &str) -> f64 {
    let Ok(parsed) = raw.trim().parse::<f64>() else {
        return 0.0;
    };
    if parsed >= 0.01 { parsed / 100.0 } else { parsed.max(0.0) }
}

fn round_to_unit(value: f64, unit: f64) -> f64 {
    if unit > 0.0 { (value / unit).round() * unit } else { value }
}

fn truncate_to_unit(value: f64, unit: f64) -> f64 {
    if unit > 0.0 {
        // Nudge by an epsilon so representation noise (0.29999999997) does
        // not lose a whole grid step.
        ((value / unit) + 1e-9).floor() * unit
    } else {
        value
    }
}

fn unit_decimals(unit: f64) -> usize {
    if unit >= 1.0 { 0 } else { (-(unit.log10()) - 1e-9).ceil() as usize }
}

fn format_unit(value: f64, unit: f64) -> String {
    format!("{:.*}", unit_decimals(unit), value)
}

/// 8-decimal fixed-point with trailing zeros trimmed (no scientific
/// notation on the wire).
fn trim_fixed(value: f64) -> String {
    let text = format!("{value:.8}");
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}

fn coinone_side(side: Side) -> &'static str {
    match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    }
}

fn str_field(value: &Value, key: &str) -> f64 {
    value
        .get(key)
        .map(|v| value_to_string(v.clone()))
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0)
}

fn balance_from_wire(value: &Value) -> Balance {
    Balance {
        currency: value
            .get("currency")
            .map(|v| value_to_string(v.clone()))
            .unwrap_or_default()
            .to_uppercase(),
        available: str_field(value, "available"),
        locked: str_field(value, "limit"),
        average_price: str_field(value, "average_price"),
    }
}

/// Numeric client id parsed from `user_order_id` when the venue includes it
/// (order detail does; the active-order list does not — 0 then).
fn client_id_of(order: &WireOrder) -> u64 {
    order
        .user_order_id
        .as_deref()
        .and_then(|id| id.parse::<u64>().ok())
        .unwrap_or(0)
}

fn order_info(order: &WireOrder, order_id: u64) -> OrderInfo {
    let parse = |field: &Option<String>| {
        field.as_deref().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0)
    };
    let filled_qty = parse(&order.executed_qty).max(parse(&order.traded_qty));
    let remaining_qty = parse(&order.remain_qty);
    let state = match order.status.as_deref().map(str::to_ascii_uppercase).as_deref() {
        Some("LIVE") | Some("WAIT") | Some("OPEN") | Some("PARTIALLY_FILLED") => OrderState::Open,
        Some("FILLED") => OrderState::Filled,
        Some(status) if status.contains("CANCEL") => OrderState::Canceled,
        // Unknown/absent status: infer from quantities (market orders often
        // omit a status on the fill path).
        _ if remaining_qty > 0.0 => OrderState::Open,
        _ if filled_qty > 0.0 => OrderState::Filled,
        _ => OrderState::Canceled,
    };
    OrderInfo {
        order_id,
        state,
        side: match order.side.as_deref().map(str::to_ascii_uppercase).as_deref() {
            Some("BUY") | Some("BID") => Side::Buy,
            _ => Side::Sell,
        },
        limit_px: parse(&order.price),
        filled_qty,
        remaining_qty,
    }
}

fn book_from_sides(sides: &WireBookSides) -> anyhow::Result<OrderBook> {
    let parse_side = |levels: &[WireLevel]| {
        levels
            .iter()
            .map(|level| {
                Ok((
                    level.price.parse::<f64>().with_context(|| format!("bad price {:?}", level.price))?,
                    level.qty.parse::<f64>().with_context(|| format!("bad qty {:?}", level.qty))?,
                ))
            })
            .collect::<anyhow::Result<Vec<(f64, f64)>>>()
    };
    Ok(OrderBook::from_sides(parse_side(&sides.bids)?, parse_side(&sides.asks)?))
}

async fn parse_response(resp: reqwest::Response, what: &str) -> anyhow::Result<Value> {
    let status = resp.status();
    let body = resp.text().await?;
    if status.as_u16() == 429 {
        bail!("Coinone {what} rate limited (HTTP 429)");
    }
    let value: Value = serde_json::from_str(&body)
        .with_context(|| format!("Coinone {what}: non-JSON response (HTTP {}): {body}", status.as_u16()))?;
    let error_code = value
        .get("error_code")
        .or_else(|| value.get("errorCode"))
        .map(|v| value_to_string(v.clone()))
        .unwrap_or_default();
    let result_ok = value.get("result").and_then(Value::as_str) == Some("success");
    let code_ok = error_code.is_empty() || error_code == "0" || error_code == "none";
    if !status.is_success() || (!result_ok && !code_ok) {
        let message = value
            .get("error_msg")
            .or_else(|| value.get("errorMsg"))
            .map(|v| value_to_string(v.clone()))
            .unwrap_or_else(|| body.clone());
        bail!(
            "Coinone {what} failed (HTTP {}): code {}: {message}",
            status.as_u16(),
            if error_code.is_empty() { "unknown".to_string() } else { error_code }
        );
    }
    Ok(value)
}

/// Coinone order-error codes 300..303 mean the order is gone/unknown.
fn is_not_found(err: &anyhow::Error) -> bool {
    let text = err.to_string();
    ["code 300:", "code 301:", "code 302:", "code 303:"]
        .iter()
        .any(|needle| text.contains(needle))
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

    /// Golden vector for the header signing scheme (base64 payload +
    /// HMAC-SHA512 hex), pinned against an independent implementation.
    #[test]
    fn signs_payload_like_the_reference() {
        let payload =
            r#"{"access_token":"test_access_token","nonce":"test-nonce-uuid","currency":"BTC"}"#;
        let (encoded, signature) = sign_payload("test_secret_key", payload).unwrap();
        assert_eq!(
            encoded,
            "eyJhY2Nlc3NfdG9rZW4iOiJ0ZXN0X2FjY2Vzc190b2tlbiIsIm5vbmNlIjoidGVzdC1ub25jZS11dWlkIiwiY3VycmVuY3kiOiJCVEMifQ=="
        );
        assert_eq!(
            signature,
            "6f8f9a8945d5d90cf3c5a2581fc694c64ff788c4825855e412ab15cde87b23dbcb619b91f126c605aeeef345f12b8d19a97553ad1c064a95f293a1662fbb8d82"
        );
    }

    #[test]
    fn prices_round_and_sizes_truncate_onto_market_units() {
        assert_eq!(round_to_unit(106_823_456.0, 10_000.0), 106_820_000.0);
        assert_eq!(round_to_unit(1_467.3, 0.5), 1_467.5);
        assert_eq!(round_to_unit(5.0, 0.0), 5.0); // degenerate unit passes through

        assert_eq!(truncate_to_unit(0.123_456_789, 0.000_000_01), 0.123_456_78);
        // Epsilon nudge keeps representation noise from losing a grid step.
        assert!((truncate_to_unit(0.3, 0.1) - 0.3).abs() < 1e-12);
        assert_eq!(truncate_to_unit(5.9, 1.0), 5.0);

        assert_eq!(format_unit(106_820_000.0, 10_000.0), "106820000");
        assert_eq!(format_unit(1_467.5, 0.5), "1467.5");
        assert_eq!(format_unit(0.005, 0.005), "0.005");
        assert_eq!(unit_decimals(0.05), 2);
        assert_eq!(unit_decimals(0.000_000_01), 8);

        assert_eq!(trim_fixed(6.816_632_58), "6.81663258");
        assert_eq!(trim_fixed(6_000.0), "6000");
    }

    #[test]
    fn normalizes_percent_style_fees() {
        assert!((normalize_fee_rate("0.2") - 0.002).abs() < f64::EPSILON);
        assert!((normalize_fee_rate("0.02") - 0.0002).abs() < f64::EPSILON);
        assert!((normalize_fee_rate("0.0002") - 0.0002).abs() < f64::EPSILON);
        assert_eq!(normalize_fee_rate("garbage"), 0.0);
    }

    #[test]
    fn wire_orders_normalize_to_order_info() {
        let wire: WireOrder = serde_json::from_str(
            r#"{
                "order_id": "uuid-1",
                "user_order_id": "1787569275131001",
                "status": "LIVE",
                "side": "BUY",
                "price": "53310000",
                "original_qty": "0.0001",
                "remain_qty": "0.0001",
                "executed_qty": "0",
                "fee": 0
            }"#,
        )
        .unwrap();
        assert_eq!(client_id_of(&wire), 1_787_569_275_131_001);
        let info = order_info(&wire, client_id_of(&wire));
        assert_eq!(info.state, OrderState::Open);
        assert_eq!(info.side, Side::Buy);
        assert_eq!(info.limit_px, 53_310_000.0);
        assert_eq!(info.remaining_qty, 0.0001);

        for (status, expected) in [
            (Some("PARTIALLY_FILLED"), OrderState::Open),
            (Some("FILLED"), OrderState::Filled),
            (Some("CANCELED"), OrderState::Canceled),
            (Some("PARTIALLY_CANCELED"), OrderState::Canceled),
        ] {
            let mut wire = wire.clone();
            wire.status = status.map(str::to_string);
            assert_eq!(order_info(&wire, 0).state, expected, "{status:?}");
        }

        // Missing status infers from quantities.
        let mut wire = wire.clone();
        wire.status = None;
        wire.remain_qty = Some("0".to_string());
        wire.executed_qty = Some("0.0001".to_string());
        assert_eq!(order_info(&wire, 0).state, OrderState::Filled);
    }

    #[test]
    fn ws_orderbook_envelopes_become_synced_books() {
        let envelope: WireEnvelope = serde_json::from_str(
            r#"{
                "response_type": "DATA",
                "channel": "ORDERBOOK",
                "topic": {"quote_currency": "KRW", "target_currency": "BTC"},
                "data": {
                    "quote_currency": "KRW",
                    "target_currency": "BTC",
                    "timestamp": 1787569804275,
                    "bids": [{"price": "106840000", "qty": "0.05770634"}],
                    "asks": [{"price": "106850000", "qty": 0.00009389}]
                }
            }"#,
        )
        .unwrap();
        assert_eq!(envelope.channel.as_deref(), Some("ORDERBOOK"));
        let sides: WireBookSides = serde_json::from_value(envelope.data.unwrap()).unwrap();
        let book = book_from_sides(&sides).unwrap();
        assert!(book.is_synced());
        assert_eq!(book.best_bid(), Some(106_840_000.0));
        assert_eq!(book.best_ask(), Some(106_850_000.0));
    }

    #[test]
    fn embedded_error_codes_are_detected() {
        // Even HTTP-200 bodies carry result/error_code; parse_response is
        // exercised live, here we pin the not-found matcher.
        assert!(is_not_found(&anyhow::anyhow!(
            "Coinone /v2.1/order/detail failed (HTTP 200): code 300: order not found"
        )));
        assert!(!is_not_found(&anyhow::anyhow!(
            "Coinone /v2.1/order/detail failed (HTTP 200): code 103: invalid token"
        )));
    }
}
