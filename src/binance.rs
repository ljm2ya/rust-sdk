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
    collections::BTreeMap as Params,
    env,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, bail};
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::Sha256;
use tokio::{net::TcpStream, sync::mpsc};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::protocol::Message,
};

use crate::book::{BookUpdate, DepthEvent, DepthSnapshot, OrderBook, parse_levels};
use crate::order::{ActiveOrder, Side};

const TRADE_WS: &str = "wss://ws-fapi.binance.com/ws-fapi/v1";

static REQ_ID: AtomicU64 = AtomicU64::new(1);

/// Gate closure for REST snapshot fetches (e.g. skip near a market close).
pub type RestGate = Arc<dyn Fn() -> bool + Send + Sync>;

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

/// Fetch `exchangeInfo` grids for one symbol.
pub async fn fetch_symbol_info(symbol: &str) -> anyhow::Result<SymbolInfo> {
    let url = format!(
        "https://fapi.binance.com/fapi/v1/exchangeInfo?symbol={}",
        symbol.to_uppercase()
    );
    let value: Value = reqwest::get(url).await?.error_for_status()?.json().await?;
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

/// Binance USD-M futures trade WebSocket capsule for a single symbol.
pub struct BinanceFutures {
    ws: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
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
        crate::load_dotenv();
        let api_key = env::var("BINANCE_API_KEY").context("BINANCE_API_KEY is required")?;
        let secret = env::var("BINANCE_API_SECRET").context("BINANCE_API_SECRET is required")?;
        let info = fetch_symbol_info(symbol).await?;
        Ok(Self {
            ws: Some(connect_trade_ws().await?),
            api_key,
            secret,
            symbol: symbol.to_uppercase(),
            info,
        })
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// Trading grids resolved at connect time.
    pub fn symbol_info(&self) -> &SymbolInfo {
        &self.info
    }

    async fn signed_request(&mut self, method: &str, params: Params<String, Value>) -> anyhow::Result<Value> {
        match self.signed_request_once(method, params.clone()).await {
            Ok(result) => Ok(result),
            Err(first_err) => {
                self.ws = Some(connect_trade_ws().await?);
                self.signed_request_once(method, params).await.with_context(|| {
                    format!("Binance {method} retry after reconnect failed; first error: {first_err}")
                })
            }
        }
    }

    async fn signed_request_once(&mut self, method: &str, mut params: Params<String, Value>) -> anyhow::Result<Value> {
        let id = next_id();
        params.insert("apiKey".to_string(), json!(self.api_key));
        params.insert("timestamp".to_string(), json!(now_ms()?));
        let signature = sign_params(&params, &self.secret)?;
        params.insert("signature".to_string(), json!(signature));

        let ws = self.ws.as_mut().context("Binance trade WebSocket missing")?;
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
        params.insert("timestamp".to_string(), json!(now_ms()?));
        let signature = sign_params(&params, &self.secret)?;
        let query = params
            .iter()
            .map(|(key, value)| format!("{key}={}", param_value(value)))
            .collect::<Vec<_>>()
            .join("&");
        let url = format!("https://fapi.binance.com/fapi/v2/positionRisk?{query}&signature={signature}");
        let client = reqwest::Client::new();
        let result: Value = client
            .get(url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let positions = result.as_array().context("positionRisk result is not an array")?;
        let leverage = positions
            .iter()
            .find(|p| p["symbol"].as_str() == Some(self.symbol.as_str()))
            .and_then(|p| p["leverage"].as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(1.0);
        Ok(leverage.max(1.0))
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
    let url = format!(
        "https://fapi.binance.com/fapi/v1/depth?symbol={}&limit=1000",
        symbol.to_uppercase()
    );
    Ok(reqwest::get(url).await?.error_for_status()?.json().await?)
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

#[derive(Debug, Deserialize)]
struct CombinedDepthEvent {
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
