//! Binance USD-M futures adapter, extracted from mondayfuck.
//!
//! - [`BinanceFutures`] — trade WebSocket capsule (`ws-fapi`): signed
//!   `order.place` / `order.status` / `order.cancel` / `order.modify` /
//!   `v2/account.position`, with one automatic reconnect-and-retry per
//!   request.
//! - [`stream_book`] — depth streaming with the local orderbook managed
//!   internally: REST snapshot + `@depth@100ms` diffs applied per Binance's
//!   sync rules, automatic snapshot reload on sequence gaps, emitting
//!   ready-to-use [`OrderBook`] states (plus [`BookUpdate::Notice`] lines
//!   for resync events).
//!
//! ## Environment keys
//!
//! - `BINANCE_API_KEY` / `BINANCE_API_SECRET` — trade WS credentials.
//! - `PRICE_DECIMALS` / `QTY_DECIMALS` — [`format_price`] / [`format_qty`]
//!   precision (default 3), matching the original mondayfuck behavior.

use std::{
    collections::{BTreeMap as Params, HashMap, HashSet},
    env,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, bail};
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::Sha256;
use tokio::{
    net::TcpStream,
    sync::{Mutex, mpsc, watch},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::protocol::Message,
};

use crate::book::{BookUpdate, DepthEvent, DepthSnapshot, OrderBook, parse_levels};
use crate::order::{ActiveOrder, Side};

const TRADE_WS: &str = "wss://ws-fapi.binance.com/ws-fapi/v1";

static REQ_ID: AtomicU64 = AtomicU64::new(1);

/// Gate closure for REST snapshot fetches (e.g. skip near a market close).
pub type RestGate = Arc<dyn Fn() -> bool + Send + Sync>;

/// One account fill from `/fapi/v1/userTrades`.
#[derive(Clone, Debug)]
pub struct UserTrade {
    pub trade_id: u64,
    pub order_id: u64,
    pub side: Side,
    pub price: f64,
    pub qty: f64,
    /// `quoteQty` (USDT).
    pub quote_qty: f64,
    /// Commission charged, in `commission_asset` (USDT unless BNB fee
    /// discount is on).
    pub commission: f64,
    pub commission_asset: String,
    /// Venue-computed realized PnL of this fill (against the account's
    /// average entry, not per strategy).
    pub realized_pnl: f64,
    pub maker: bool,
    /// Exchange timestamp (ms).
    pub time: u64,
}

/// Per-symbol trading grids from `exchangeInfo` (price/quantity precision
/// and filters), resolved once at connect time.
#[derive(Clone, Debug)]
pub struct SymbolInfo {
    /// Max decimal places accepted in prices (`pricePrecision`).
    pub price_decimals: u32,
    /// Max decimal places accepted in quantities (`quantityPrecision`).
    pub qty_decimals: u32,
    /// PRICE_FILTER tick.
    pub tick_size: f64,
    /// LOT_SIZE step.
    pub step_size: f64,
    /// LOT_SIZE minimum quantity.
    pub min_qty: f64,
    /// MIN_NOTIONAL floor in quote units.
    pub min_notional: f64,
}

// ---------------------------------------------------------------------------
// REST budget (fapi): 2,400 weight/min per IP, 429 then 418 (IP ban) when
// exceeded. Every REST call in this module goes through `rest_get`, which
// meters weight against a conservative budget and pauses ALL calls while
// the venue says the IP is banned (`Retry-After`).
// ---------------------------------------------------------------------------

const REST_WEIGHT_LIMIT: u32 = 1_800;
const REST_WINDOW: Duration = Duration::from_secs(60);

struct RestBudget {
    used: std::collections::VecDeque<(Instant, u32)>,
    banned_until: Option<Instant>,
}

fn rest_budget() -> &'static Mutex<RestBudget> {
    static BUDGET: OnceLock<Mutex<RestBudget>> = OnceLock::new();
    BUDGET.get_or_init(|| Mutex::new(RestBudget { used: Default::default(), banned_until: None }))
}

/// Block until `weight` fits in the rolling window and no ban is active.
pub async fn acquire_rest(weight: u32) {
    loop {
        let wait = {
            let mut b = rest_budget().lock().await;
            let now = Instant::now();
            if let Some(until) = b.banned_until {
                if until > now {
                    Some(until - now)
                } else {
                    b.banned_until = None;
                    None
                }
            } else {
                while b.used.front().is_some_and(|(at, _)| now.duration_since(*at) >= REST_WINDOW) {
                    b.used.pop_front();
                }
                let used: u32 = b.used.iter().map(|(_, w)| *w).sum();
                if used + weight <= REST_WEIGHT_LIMIT {
                    b.used.push_back((now, weight));
                    None
                } else {
                    Some(b.used.front().map(|(at, _)| REST_WINDOW - now.duration_since(*at)).unwrap_or(Duration::from_secs(1)))
                }
            }
        };
        match wait {
            None => return,
            Some(d) => tokio::time::sleep(d.max(Duration::from_millis(50))).await,
        }
    }
}

/// Metered GET of a fapi endpoint. A 429/418 records the venue's
/// `Retry-After` (default 60s) as a process-wide pause.
pub async fn rest_get(url: &str, weight: u32) -> anyhow::Result<Value> {
    acquire_rest(weight).await;
    let resp = reqwest::get(url).await?;
    let status = resp.status();
    if status.as_u16() == 429 || status.as_u16() == 418 {
        let retry = resp
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(60)
            .max(5);
        rest_budget().lock().await.banned_until = Some(Instant::now() + Duration::from_secs(retry));
        bail!("Binance REST rate limited ({status}); pausing all REST calls for {retry}s");
    }
    Ok(resp.error_for_status()?.json().await?)
}

/// How long one `exchangeInfo` download (~1 MB) serves every
/// [`fetch_symbol_info`] call. Connecting many symbols at once would
/// otherwise download it once per symbol.
const EXCHANGE_INFO_TTL: Duration = Duration::from_secs(120);

async fn exchange_info() -> anyhow::Result<Arc<Value>> {
    static CACHE: OnceLock<Mutex<Option<(Instant, Arc<Value>)>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(None));
    let mut guard = cache.lock().await;
    if let Some((at, value)) = guard.as_ref()
        && at.elapsed() < EXCHANGE_INFO_TTL
    {
        return Ok(value.clone());
    }
    let value = rest_get("https://fapi.binance.com/fapi/v1/exchangeInfo", 1).await?;
    let value = Arc::new(value);
    *guard = Some((Instant::now(), value.clone()));
    Ok(value)
}

/// Fetch `exchangeInfo` grids for one symbol (shared, cached download).
pub async fn fetch_symbol_info(symbol: &str) -> anyhow::Result<SymbolInfo> {
    let value = exchange_info().await?;
    let info = value["symbols"]
        .as_array()
        .and_then(|symbols| {
            symbols
                .iter()
                .find(|s| s["symbol"].as_str() == Some(&symbol.to_uppercase()))
        })
        .context("symbol missing from exchangeInfo")?;
    let filter = |kind: &str, key: &str| -> f64 {
        info["filters"]
            .as_array()
            .and_then(|filters| filters.iter().find(|f| f["filterType"].as_str() == Some(kind)))
            .and_then(|f| f[key].as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0)
    };
    Ok(SymbolInfo {
        price_decimals: info["pricePrecision"].as_u64().unwrap_or(2) as u32,
        qty_decimals: info["quantityPrecision"].as_u64().unwrap_or(2) as u32,
        tick_size: filter("PRICE_FILTER", "tickSize"),
        step_size: filter("LOT_SIZE", "stepSize"),
        min_qty: filter("LOT_SIZE", "minQty"),
        min_notional: filter("MIN_NOTIONAL", "notional"),
    })
}

/// Truncate `value` onto a decimal grid and render with exactly that many
/// decimals (Binance rejects extra decimals with `-1111`).
fn format_on_grid(value: f64, decimals: u32) -> String {
    let scale = 10f64.powi(decimals as i32);
    format!("{:.*}", decimals as usize, (value * scale).round() / scale)
}

fn floor_on_grid(value: f64, decimals: u32) -> String {
    let scale = 10f64.powi(decimals as i32);
    format!("{:.*}", decimals as usize, (value * scale + 1e-9).floor() / scale)
}

/// Snap a price onto the PRICE_FILTER tick grid. The tick can be coarser
/// than the `pricePrecision` decimal grid (e.g. tickSize 0.10 with
/// pricePrecision 2), and off-tick prices are rejected with `-4014`
/// ("Price not increased by tick size").
fn snap_to_tick(price: f64, tick: f64) -> f64 {
    if tick > 0.0 { (price / tick).round() * tick } else { price }
}

type TradeSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// A trade WebSocket that several [`BinanceFutures`] capsules can share
/// (requests are serialized on it; one request is in flight at a time).
/// Connected lazily on first use, reconnected on failure.
#[derive(Clone, Default)]
pub struct SharedTradeWs(Arc<Mutex<Option<TradeSocket>>>);

impl SharedTradeWs {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Binance USD-M futures trade WebSocket capsule for a single symbol.
pub struct BinanceFutures {
    ws: SharedTradeWs,
    api_key: String,
    secret: String,
    symbol: String,
    info: SymbolInfo,
}

impl BinanceFutures {
    /// Connect the trade WebSocket. Needs `BINANCE_API_KEY` /
    /// `BINANCE_API_SECRET`. Also resolves the symbol's price/quantity
    /// grids from `exchangeInfo` so orders are formatted exactly.
    pub async fn connect(symbol: &str) -> anyhow::Result<Self> {
        let this = Self::connect_shared(symbol, SharedTradeWs::new()).await?;
        *this.ws.0.lock().await = Some(connect_trade_ws().await?);
        Ok(this)
    }

    /// Like [`connect`](Self::connect) but multiplexing requests over a
    /// [`SharedTradeWs`] — one socket for any number of symbols instead of
    /// one per symbol.
    pub async fn connect_shared(symbol: &str, ws: SharedTradeWs) -> anyhow::Result<Self> {
        crate::load_dotenv();
        let api_key = env::var("BINANCE_API_KEY").context("BINANCE_API_KEY is required")?;
        let secret = env::var("BINANCE_API_SECRET").context("BINANCE_API_SECRET is required")?;
        let info = fetch_symbol_info(symbol).await?;
        Ok(Self { ws, api_key, secret, symbol: symbol.to_uppercase(), info })
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// Trading grids resolved at connect time.
    pub fn symbol_info(&self) -> &SymbolInfo {
        &self.info
    }

    async fn signed_request(&mut self, method: &str, params: Params<String, Value>) -> anyhow::Result<Value> {
        // Hold the shared socket for the whole request/response exchange:
        // one request in flight per socket keeps id matching trivial.
        let ws = self.ws.clone();
        let mut socket = ws.0.lock().await;
        if socket.is_none() {
            *socket = Some(connect_trade_ws().await?);
        }
        match self.signed_request_once(socket.as_mut().expect("connected"), method, params.clone()).await {
            Ok(result) => Ok(result),
            // A definitive exchange response reached us. Reconnecting and
            // sending the same request again cannot fix it (and duplicates
            // deterministic rejects such as -2027).
            Err(err) if err.to_string().starts_with(&format!("Binance {method} failed:")) => Err(err),
            Err(first_err) => {
                *socket = Some(connect_trade_ws().await?);
                self.signed_request_once(socket.as_mut().expect("connected"), method, params)
                    .await
                    .with_context(|| {
                        format!("Binance {method} retry after reconnect failed; first error: {first_err}")
                    })
            }
        }
    }

    async fn signed_request_once(
        &self,
        ws: &mut TradeSocket,
        method: &str,
        mut params: Params<String, Value>,
    ) -> anyhow::Result<Value> {
        let id = next_id();
        params.insert("apiKey".to_string(), json!(self.api_key));
        params.insert("timestamp".to_string(), json!(now_ms()?));
        let signature = sign_params(&params, &self.secret)?;
        params.insert("signature".to_string(), json!(signature));

        ws.send(Message::Text(
            json!({ "id": id, "method": method, "params": params }).to_string().into(),
        ))
        .await?;

        while let Some(msg) = ws.next().await {
            match msg? {
                Message::Text(text) => {
                    let value: Value = serde_json::from_str(&text)?;
                    if value.get("id").and_then(Value::as_str) == Some(&id) {
                        if value.get("status").and_then(Value::as_i64) == Some(200) {
                            return Ok(value["result"].clone());
                        }
                        bail!("Binance {method} failed: {value}");
                    }
                }
                Message::Ping(payload) => ws.send(Message::Pong(payload)).await?,
                Message::Close(frame) => bail!("Binance trade WebSocket closed: {frame:?}"),
                _ => {}
            }
        }
        bail!("Binance trade WebSocket ended")
    }

    /// Place a GTC limit order. Prices and quantities are formatted onto the
    /// symbol's `exchangeInfo` grids (extra decimals are rejected by the
    /// venue with `-1111`).
    pub async fn place_limit(&mut self, side: Side, qty: f64, price: f64, reduce_only: bool) -> anyhow::Result<ActiveOrder> {
        self.place_limit_tif(side, qty, price, reduce_only, "GTC").await
    }

    /// Limit order with an explicit time-in-force. `"IOC"` + a crossing
    /// price is the synthetic market order on this venue: it fills against
    /// the book immediately and the remainder is canceled by the venue.
    /// Keep the cross small (~1%) — the PERCENT_PRICE filter rejects limit
    /// prices further than a few percent from mark price (`-4024`).
    pub async fn place_limit_tif(
        &mut self,
        side: Side,
        qty: f64,
        price: f64,
        reduce_only: bool,
        tif: &str,
    ) -> anyhow::Result<ActiveOrder> {
        let mut params = Params::new();
        params.insert("symbol".to_string(), json!(self.symbol));
        params.insert("side".to_string(), json!(side.as_str()));
        params.insert("type".to_string(), json!("LIMIT"));
        params.insert("timeInForce".to_string(), json!(tif));
        params.insert(
            "quantity".to_string(),
            json!(floor_on_grid(qty, self.info.qty_decimals)),
        );
        params.insert(
            "price".to_string(),
            json!(format_on_grid(
                snap_to_tick(price, self.info.tick_size),
                self.info.price_decimals
            )),
        );
        params.insert("positionSide".to_string(), json!("BOTH"));
        if reduce_only {
            params.insert("reduceOnly".to_string(), json!("true"));
        }
        let result = self.signed_request("order.place", params).await?;
        let order_id = result["orderId"].as_u64().context("missing orderId from order.place")?;
        Ok(ActiveOrder { order_id, side, qty })
    }

    /// Raw `order.status` result (`result.status` holds the state string).
    pub async fn order_status(&mut self, order_id: u64) -> anyhow::Result<Value> {
        let mut params = Params::new();
        params.insert("symbol".to_string(), json!(self.symbol));
        params.insert("orderId".to_string(), json!(order_id));
        self.signed_request("order.status", params).await
    }

    pub async fn cancel_order(&mut self, order_id: u64) -> anyhow::Result<()> {
        let mut params = Params::new();
        params.insert("symbol".to_string(), json!(self.symbol));
        params.insert("orderId".to_string(), json!(order_id));
        self.signed_request("order.cancel", params).await?;
        Ok(())
    }

    /// Signed position size for this symbol (long > 0, short < 0).
    pub async fn position_amt(&mut self) -> anyhow::Result<f64> {
        let mut params = Params::new();
        params.insert("symbol".to_string(), json!(self.symbol));
        let result = self.signed_request("v2/account.position", params).await?;
        let positions = result.as_array().context("position result is not an array")?;
        let pos = positions
            .iter()
            .find(|p| p["symbol"].as_str() == Some(self.symbol.as_str()))
            .and_then(|p| p["positionAmt"].as_str())
            .unwrap_or("0");
        Ok(pos.parse()?)
    }

    /// Signed position sizes of EVERY symbol in one request (long > 0),
    /// keyed by symbol — lets many capsules poll positions with one call.
    pub async fn all_positions(&mut self) -> anyhow::Result<HashMap<String, f64>> {
        let result = self.signed_request("v2/account.position", Params::new()).await?;
        let positions = result.as_array().context("position result is not an array")?;
        let mut out = HashMap::new();
        for p in positions {
            let (Some(symbol), Some(amt)) = (p["symbol"].as_str(), p["positionAmt"].as_str()) else { continue };
            out.insert(symbol.to_string(), amt.parse::<f64>().unwrap_or(0.0));
        }
        Ok(out)
    }

    /// Available USDT margin balance for placing new orders
    /// (`v2/account.balance` -> `availableBalance`).
    pub async fn available_balance(&mut self) -> anyhow::Result<f64> {
        let result = self.signed_request("v2/account.balance", Params::new()).await?;
        let balances = result.as_array().context("balance result is not an array")?;
        let avail = balances
            .iter()
            .find(|b| b["asset"].as_str() == Some("USDT"))
            .and_then(|b| b["availableBalance"].as_str())
            .unwrap_or("0");
        Ok(avail.parse()?)
    }

    /// Leverage currently configured for this symbol. Uses the signed REST
    /// `/fapi/v2/positionRisk` endpoint, which reports `leverage` even for a
    /// flat symbol (the WS `v2/account.position` result omits it there).
    pub async fn position_leverage(&mut self) -> anyhow::Result<f64> {
        let mut params = Params::new();
        params.insert("symbol".to_string(), json!(self.symbol));
        let result = self.signed_rest_get("/fapi/v2/positionRisk", params, 5).await?;
        let positions = result.as_array().context("positionRisk result is not an array")?;
        let leverage = positions
            .iter()
            .find(|p| p["symbol"].as_str() == Some(self.symbol.as_str()))
            .and_then(|p| p["leverage"].as_str())
            .context("positionRisk: symbol/leverage missing")?
            .parse::<f64>()
            .context("positionRisk: invalid leverage")?;
        Ok(leverage.max(1.0))
    }

    /// Account-specific notional/leverage brackets for this symbol.
    pub async fn leverage_brackets(&self) -> anyhow::Result<Value> {
        let mut params = Params::new();
        params.insert("symbol".to_string(), json!(self.symbol));
        self.signed_rest_get("/fapi/v1/leverageBracket", params, 1).await
    }

    /// Maximum initial leverage allowed by this account's first notional
    /// bracket for the symbol.
    pub async fn max_initial_leverage(&self) -> anyhow::Result<f64> {
        let value = self.leverage_brackets().await?;
        value
            .as_array()
            .and_then(|symbols| symbols.iter().find(|item| item["symbol"].as_str() == Some(self.symbol.as_str())))
            .and_then(|symbol| symbol["brackets"].as_array())
            .and_then(|brackets| {
                brackets.iter().find(|bracket| {
                    bracket["bracket"].as_u64() == Some(1)
                        && bracket["notionalFloor"].as_f64() == Some(0.0)
                })
            })
            .and_then(|bracket| bracket["initialLeverage"].as_f64())
            .context("leverageBracket: initialLeverage missing")
    }

    /// Fills of one order (`/fapi/v1/userTrades?orderId=`): actual prices,
    /// quote amounts and commissions. Empty when nothing executed.
    pub async fn order_trades(&self, order_id: u64) -> anyhow::Result<Vec<UserTrade>> {
        let mut params = Params::new();
        params.insert("symbol".to_string(), json!(self.symbol));
        params.insert("orderId".to_string(), json!(order_id));
        let result = self.signed_rest_get("/fapi/v1/userTrades", params, 5).await?;
        let trades = result.as_array().context("userTrades result is not an array")?;
        trades
            .iter()
            .map(|t| {
                let num = |key: &str| -> anyhow::Result<f64> {
                    t[key]
                        .as_str()
                        .with_context(|| format!("userTrades: missing {key}"))?
                        .parse::<f64>()
                        .with_context(|| format!("userTrades: bad {key}"))
                };
                Ok(UserTrade {
                    trade_id: t["id"].as_u64().unwrap_or(0),
                    order_id: t["orderId"].as_u64().unwrap_or(order_id),
                    side: if t["side"].as_str() == Some("BUY") { Side::Buy } else { Side::Sell },
                    price: num("price")?,
                    qty: num("qty")?,
                    quote_qty: num("quoteQty")?,
                    commission: num("commission")?,
                    commission_asset: t["commissionAsset"].as_str().unwrap_or("").to_string(),
                    realized_pnl: num("realizedPnl").unwrap_or(0.0),
                    maker: t["maker"].as_bool().unwrap_or(false),
                    time: t["time"].as_u64().unwrap_or(0),
                })
            })
            .collect()
    }

    /// Signed REST GET against `fapi.binance.com` (endpoints the trade
    /// WebSocket does not expose).
    async fn signed_rest_get(&self, path: &str, mut params: Params<String, Value>, weight: u32) -> anyhow::Result<Value> {
        params.insert("timestamp".to_string(), json!(now_ms()?));
        let signature = sign_params(&params, &self.secret)?;
        let query = params
            .iter()
            .map(|(key, value)| format!("{key}={}", param_value(value)))
            .collect::<Vec<_>>()
            .join("&");
        let url = format!("https://fapi.binance.com{path}?{query}&signature={signature}");
        acquire_rest(weight).await;
        let client = reqwest::Client::new();
        let response = client.get(url).header("X-MBX-APIKEY", &self.api_key).send().await?;
        let status = response.status();
        let body: Value = response.json().await.with_context(|| format!("Binance {path}: non-JSON response ({status})"))?;
        if !status.is_success() {
            bail!("Binance {path} failed ({status}): {body}");
        }
        Ok(body)
    }

    /// Re-price a live order. `Ok(true)` — still live (or no change needed,
    /// `-5027`); `Ok(false)` — the order no longer exists (`-2013`).
    pub async fn modify_order(&mut self, order: &ActiveOrder, price: f64) -> anyhow::Result<bool> {
        let mut params = Params::new();
        params.insert("symbol".to_string(), json!(self.symbol));
        params.insert("orderId".to_string(), json!(order.order_id));
        params.insert("side".to_string(), json!(order.side.as_str()));
        params.insert("quantity".to_string(), json!(floor_on_grid(order.qty, self.info.qty_decimals)));
        params.insert(
            "price".to_string(),
            json!(format_on_grid(
                snap_to_tick(price, self.info.tick_size),
                self.info.price_decimals
            )),
        );
        match self.signed_request("order.modify", params).await {
            Ok(_) => Ok(true),
            Err(err) if err.to_string().contains("-2013") => Ok(false),
            Err(err) if err.to_string().contains("-5027") => Ok(true),
            Err(err) => Err(err),
        }
    }
}

/// REST depth snapshot (1000 levels) for `symbol`.
pub async fn fetch_depth_snapshot(symbol: &str) -> anyhow::Result<DepthSnapshot> {
    fetch_depth_snapshot_limit(symbol, 1000).await
}

/// Stream managed depth books for `symbol` into `tx`.
///
/// The local [`OrderBook`] lives inside this method: it is seeded with a
/// REST snapshot, kept in sync with `@depth@100ms` diffs, and reloaded
/// automatically on sequence gaps. `rest_gate` (when given) can veto REST
/// snapshot fetches — on a vetoed/failed resync the book is marked unsynced
/// and a [`BookUpdate::Notice`] is emitted, mirroring the original
/// mondayfuck behavior.
pub async fn stream_book(symbol: &str, rest_gate: Option<RestGate>, tx: mpsc::Sender<BookUpdate>) -> anyhow::Result<()> {
    let stream_url = format!(
        "wss://fstream.binance.com/public/stream?streams={}@depth@100ms",
        symbol.to_lowercase()
    );
    let (mut ws, _) = connect_async(&stream_url).await?;
    let gate_allows = |gate: &Option<RestGate>| gate.as_ref().is_none_or(|gate| gate());

    let mut book = OrderBook::default();
    if gate_allows(&rest_gate) {
        fetch_depth_snapshot(symbol).await?.load_into(&mut book)?;
        if tx.send(BookUpdate::Book(book.clone())).await.is_err() {
            return Ok(());
        }
    }

    while let Some(msg) = ws.next().await {
        match msg? {
            Message::Text(text) => {
                let raw: CombinedDepthEvent = serde_json::from_str(&text)?;
                let raw = raw.data;
                let event = DepthEvent {
                    first_id: raw.first_id,
                    final_id: raw.final_id,
                    prev_final_id: raw.prev_final_id,
                    bids: parse_levels(raw.b)?,
                    asks: parse_levels(raw.a)?,
                };
                if !book.apply(event) {
                    let notice_tx = tx
                        .send(BookUpdate::Notice("depth gap detected; reloading snapshot".to_string()))
                        .await;
                    if notice_tx.is_err() {
                        return Ok(());
                    }
                    let reload = if gate_allows(&rest_gate) {
                        match fetch_depth_snapshot(symbol).await {
                            Ok(snapshot) => snapshot.load_into(&mut book).map_err(|err| err.to_string()),
                            Err(err) => Err(err.to_string()),
                        }
                    } else {
                        Err("REST snapshot gated".to_string())
                    };
                    if let Err(err) = reload {
                        book.mark_unsynced();
                        if tx
                            .send(BookUpdate::Notice(format!(
                                "depth resync failed; trading disabled until resync: {err}"
                            )))
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                }
                if tx.send(BookUpdate::Book(book.clone())).await.is_err() {
                    return Ok(());
                }
            }
            Message::Ping(payload) => ws.send(Message::Pong(payload)).await?,
            Message::Close(frame) => bail!("Binance depth WebSocket closed: {frame:?}"),
            _ => {}
        }
    }
    Ok(())
}

/// Depth snapshot with a caller-chosen level count (weight 5 for ≤100,
/// 10 for 500, 20 for 1000).
pub async fn fetch_depth_snapshot_limit(symbol: &str, limit: u32) -> anyhow::Result<DepthSnapshot> {
    let url = format!(
        "https://fapi.binance.com/fapi/v1/depth?symbol={}&limit={limit}",
        symbol.to_uppercase()
    );
    let weight = match limit {
        0..=100 => 5,
        101..=500 => 10,
        501..=1000 => 20,
        _ => 50,
    };
    Ok(serde_json::from_value(rest_get(&url, weight).await?)?)
}

/// Levels kept per side in the books emitted by [`stream_books`].
const MULTI_BOOK_DEPTH: usize = 20;
/// Snapshot size for [`stream_books`] resyncs: top-of-book consumers only
/// need the first levels, and 100 levels cost weight 5 instead of 20.
const MULTI_SNAPSHOT_LIMIT: u32 = 100;
/// Concurrent snapshot fetches (startup of many symbols).
const MULTI_SNAPSHOT_PARALLEL: usize = 4;
/// Spread boot-time subscription messages and REST snapshots across time.
const MULTI_REQUEST_PAUSE: Duration = Duration::from_millis(250);
const MULTI_SUBSCRIBE_BATCH: usize = 20;

struct MultiBook {
    book: OrderBook,
    /// Events buffered while a snapshot is in flight; replayed on arrival
    /// (Binance sync rules: drop those before the snapshot, apply the rest).
    pending: Option<Vec<DepthEvent>>,
    /// Snapshot requested and not yet answered. The request is only issued
    /// once the FIRST diff of the symbol has arrived: a snapshot taken
    /// before the subscription is live is older than the first diff, which
    /// reads as a gap and triggers another snapshot — a reload storm that
    /// blew the REST budget (429 -> 418 IP ban).
    snapshot_in_flight: bool,
    /// Consecutive snapshot failures/gaps, for exponential backoff.
    retries: u32,
}

impl MultiBook {
    fn fresh() -> Self {
        Self { book: OrderBook::default(), pending: Some(Vec::new()), snapshot_in_flight: false, retries: 0 }
    }
}

fn snapshot_backoff(retries: u32) -> Duration {
    (Duration::from_secs(2) * (1u32 << retries.min(5))).min(Duration::from_secs(120))
}

/// Stream managed depth books for a DYNAMIC set of symbols over ONE
/// combined-stream connection (`SUBSCRIBE`/`UNSUBSCRIBE` on change). Each
/// symbol keeps its own locally synced book; gaps reload that symbol's
/// snapshot only. Emits `(SYMBOL, BookUpdate)` with the top
/// [`MULTI_BOOK_DEPTH`] levels. Returns on socket close (caller
/// reconnects) or when `tx` / `symbols_rx` close.
pub async fn stream_books(
    mut symbols_rx: watch::Receiver<Vec<String>>,
    tx: mpsc::Sender<(String, BookUpdate)>,
) -> anyhow::Result<()> {
    // The combined endpoint needs at least one stream in the URL.
    let mut initial: Vec<String> = symbols_rx.borrow().iter().map(|s| s.to_uppercase()).collect();
    while initial.is_empty() {
        if symbols_rx.changed().await.is_err() {
            return Ok(());
        }
        initial = symbols_rx.borrow().iter().map(|s| s.to_uppercase()).collect();
    }
    let stream_name = |s: &str| format!("{}@depth@100ms", s.to_lowercase());
    let url = format!(
        "wss://fstream.binance.com/public/stream?streams={}",
        stream_name(&initial[0])
    );
    let (mut ws, _) = connect_async(&url).await?;

    let (snap_tx, mut snap_rx) = mpsc::channel::<(String, anyhow::Result<DepthSnapshot>)>(64);
    let gate = Arc::new(tokio::sync::Semaphore::new(MULTI_SNAPSHOT_PARALLEL));
    let snapshot_pace = Arc::new(Mutex::new(None::<Instant>));
    let request_snapshot = |symbol: String, delay: Duration| {
        let snap_tx = snap_tx.clone();
        let gate = gate.clone();
        let snapshot_pace = snapshot_pace.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let Ok(_permit) = gate.acquire().await else { return };
            let mut last = snapshot_pace.lock().await;
            if let Some(at) = *last {
                tokio::time::sleep_until((at + MULTI_REQUEST_PAUSE).into()).await;
            }
            *last = Some(Instant::now());
            drop(last);
            let result = fetch_depth_snapshot_limit(&symbol, MULTI_SNAPSHOT_LIMIT).await;
            let _ = snap_tx.send((symbol, result)).await;
        });
    };

    let mut books: HashMap<String, MultiBook> = HashMap::new();
    let mut current: HashSet<String> = HashSet::new();
    current.insert(initial[0].clone());
    books.insert(initial[0].clone(), MultiBook::fresh());
    if initial.len() > 1 {
        symbols_rx.mark_changed();
    }
    let mut next_ws_request = Instant::now();

    loop {
        tokio::select! {
            changed = symbols_rx.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
                let wanted: HashSet<String> = symbols_rx.borrow().iter().map(|s| s.to_uppercase()).collect();
                let removed: Vec<String> = current.difference(&wanted).cloned().collect();
                let added: Vec<String> = wanted.difference(&current).cloned().collect();
                if !removed.is_empty() {
                    let params: Vec<String> = removed.iter().map(|s| stream_name(s)).collect();
                    tokio::time::sleep_until(next_ws_request.into()).await;
                    ws.send(Message::Text(json!({ "method": "UNSUBSCRIBE", "params": params, "id": next_id() }).to_string().into())).await?;
                    next_ws_request = Instant::now() + MULTI_REQUEST_PAUSE;
                    for s in &removed {
                        books.remove(s);
                    }
                }
                if !added.is_empty() {
                    for batch in added.chunks(MULTI_SUBSCRIBE_BATCH) {
                        let params: Vec<String> = batch.iter().map(|s| stream_name(s)).collect();
                        tokio::time::sleep_until(next_ws_request.into()).await;
                        ws.send(Message::Text(json!({ "method": "SUBSCRIBE", "params": params, "id": next_id() }).to_string().into())).await?;
                        next_ws_request = Instant::now() + MULTI_REQUEST_PAUSE;
                        for s in batch {
                            books.insert(s.clone(), MultiBook::fresh());
                        }
                    }
                }
                current = wanted;
            }
            Some((symbol, result)) = snap_rx.recv() => {
                let Some(entry) = books.get_mut(&symbol) else { continue };
                entry.snapshot_in_flight = false;
                match result {
                    Ok(snapshot) => {
                        if let Err(err) = snapshot.load_into(&mut entry.book) {
                            let _ = tx.send((symbol.clone(), BookUpdate::Notice(format!("bad snapshot: {err}")))).await;
                            entry.retries += 1;
                            entry.snapshot_in_flight = true;
                            request_snapshot(symbol, snapshot_backoff(entry.retries));
                            continue;
                        }
                        let buffered = entry.pending.take().unwrap_or_default();
                        let mut gap = false;
                        for event in buffered {
                            if !entry.book.apply(event) {
                                gap = true;
                                break;
                            }
                        }
                        if gap {
                            entry.pending = Some(Vec::new());
                            entry.retries += 1;
                            let _ = tx.send((symbol.clone(), BookUpdate::Notice(format!("snapshot/diff gap (retry {})", entry.retries)))).await;
                            // Re-armed by the next diff (see `snapshot_in_flight`).
                            continue;
                        }
                        entry.retries = 0;
                        if entry.book.is_synced() {
                            let (bids, asks) = entry.book.levels(MULTI_BOOK_DEPTH);
                            if tx.send((symbol, BookUpdate::Book(OrderBook::from_sides(bids, asks)))).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                    Err(err) => {
                        entry.retries += 1;
                        let delay = snapshot_backoff(entry.retries);
                        let _ = tx.send((symbol.clone(), BookUpdate::Notice(format!("snapshot failed, retry in {}s: {err}", delay.as_secs())))).await;
                        entry.snapshot_in_flight = true;
                        request_snapshot(symbol, delay);
                    }
                }
            }
            msg = ws.next() => {
                let Some(msg) = msg else { bail!("Binance combined depth WebSocket ended") };
                match msg? {
                    Message::Text(text) => {
                        let Ok(raw) = serde_json::from_str::<CombinedDepthEvent>(&text) else {
                            continue; // SUBSCRIBE acks etc.
                        };
                        let symbol = raw.stream.split('@').next().unwrap_or_default().to_uppercase();
                        let Some(entry) = books.get_mut(&symbol) else { continue };
                        let raw = raw.data;
                        let event = DepthEvent {
                            first_id: raw.first_id,
                            final_id: raw.final_id,
                            prev_final_id: raw.prev_final_id,
                            bids: parse_levels(raw.b)?,
                            asks: parse_levels(raw.a)?,
                        };
                        if let Some(buffer) = entry.pending.as_mut() {
                            buffer.push(event);
                            if buffer.len() > 5_000 {
                                buffer.drain(..2_500);
                            }
                            // First diff seen: the subscription is live, a
                            // snapshot taken now overlaps the buffer.
                            if !entry.snapshot_in_flight {
                                entry.snapshot_in_flight = true;
                                request_snapshot(symbol, snapshot_backoff(entry.retries).saturating_sub(Duration::from_secs(2)));
                            }
                            continue;
                        }
                        let (first_id, final_id, prev) = (event.first_id, event.final_id, event.prev_final_id);
                        let was_synced = entry.book.is_synced();
                        let seq = entry.book.sequence();
                        if !entry.book.apply(event) {
                            entry.book.mark_unsynced();
                            entry.pending = Some(Vec::new());
                            entry.snapshot_in_flight = true;
                            let _ = tx.send((symbol.clone(), BookUpdate::Notice(format!(
                                "depth gap detected; reloading snapshot (event U={first_id} u={final_id} pu={prev:?}, book synced={was_synced} lastUpdateId={} last_u={:?})",
                                seq.0, seq.1
                            )))).await;
                            request_snapshot(symbol, snapshot_backoff(entry.retries));
                            continue;
                        }
                        if entry.book.is_synced() {
                            let (bids, asks) = entry.book.levels(MULTI_BOOK_DEPTH);
                            if tx.send((symbol, BookUpdate::Book(OrderBook::from_sides(bids, asks)))).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                    Message::Ping(payload) => ws.send(Message::Pong(payload)).await?,
                    Message::Close(frame) => bail!("Binance depth WebSocket closed: {frame:?}"),
                    _ => {}
                }
            }
            _ = tx.closed() => return Ok(()),
        }
    }
}

#[derive(Debug, Deserialize)]
struct CombinedDepthEvent {
    #[serde(default)]
    stream: String,
    data: RawDepthEvent,
}

#[derive(Debug, Deserialize)]
struct RawDepthEvent {
    #[serde(rename = "U")]
    first_id: u64,
    #[serde(rename = "u")]
    final_id: u64,
    #[serde(rename = "pu")]
    prev_final_id: Option<u64>,
    b: Vec<Vec<String>>,
    a: Vec<Vec<String>>,
}

async fn connect_trade_ws() -> anyhow::Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
    let (ws, _) = connect_async(TRADE_WS).await?;
    Ok(ws)
}

/// `PRICE_DECIMALS` (default 3) fixed-point formatting for logs and
/// modify payloads.
pub fn format_price(value: f64) -> String {
    format!("{:.*}", env_u64("PRICE_DECIMALS", 3) as usize, value)
}

/// `QTY_DECIMALS` (default 3) fixed-point formatting for logs and
/// modify payloads.
pub fn format_qty(value: f64) -> String {
    format!("{:.*}", env_u64("QTY_DECIMALS", 3) as usize, value)
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn sign_params(params: &Params<String, Value>, secret: &str) -> anyhow::Result<String> {
    let payload = params
        .iter()
        .map(|(key, value)| format!("{key}={}", param_value(value)))
        .collect::<Vec<_>>()
        .join("&");
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

fn param_value(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => value.to_string(),
    }
}

fn next_id() -> String {
    format!("arb-{}-{}", now_ms().unwrap_or_default(), REQ_ID.fetch_add(1, Ordering::Relaxed))
}

fn now_ms() -> anyhow::Result<u128> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signs_params_hmac_sha256_over_sorted_query() {
        // Params is a BTreeMap, so the signed payload is key-sorted.
        let mut params = Params::new();
        params.insert("symbol".to_string(), json!("CLUSDT"));
        params.insert("orderId".to_string(), json!(42_u64));
        params.insert("apiKey".to_string(), json!("key"));
        let signature = sign_params(&params, "secret").unwrap();
        // Stable reference produced by this exact payload:
        // "apiKey=key&orderId=42&symbol=CLUSDT".
        let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
        mac.update(b"apiKey=key&orderId=42&symbol=CLUSDT");
        assert_eq!(signature, hex::encode(mac.finalize().into_bytes()));
    }

    #[test]
    fn param_values_serialize_like_query_strings() {
        assert_eq!(param_value(&json!("abc")), "abc");
        assert_eq!(param_value(&json!(42)), "42");
        assert_eq!(param_value(&json!(true)), "true");
    }
}
