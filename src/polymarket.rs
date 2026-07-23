//! Polymarket venue adapter, built on the official `polymarket_client_sdk_v2`
//! crate (rs-clob-client-v2).
//!
//! Environment keys:
//! - `POLYMARKET_PRIVATE_KEY` — EOA private key (the SDK's `PRIVATE_KEY_VAR`).
//! - `POLYMARKET_HOST` — optional CLOB host, defaults to
//!   `https://clob-v2.polymarket.com`.
//! - `POLYMARKET_SIGNATURE_TYPE` — optional: `eoa` (default), `proxy`,
//!   `gnosis_safe`, or `poly1271` (numeric `0`-`3` also accepted). Must match
//!   how your account was onboarded — see [`SignatureType`] docs below.
//! - `POLYMARKET_FUNDER` — optional explicit funder/deposit-wallet address;
//!   normally left unset so the SDK auto-derives it for `proxy`/`gnosis_safe`.

use async_trait::async_trait;
use futures_util::StreamExt as _;
use polymarket_client_sdk_v2::auth::state::{Authenticated, Unauthenticated};
use polymarket_client_sdk_v2::auth::{LocalSigner, Normal, Signer as _};
use polymarket_client_sdk_v2::clob::types::response::{OpenOrderResponse, PostOrderResponse};
use polymarket_client_sdk_v2::clob::types::{
    Amount, OrderStatusType, OrderType as PmOrderType, Side as PmSide,
};
use polymarket_client_sdk_v2::clob::ws::types::response::BookUpdate;
use polymarket_client_sdk_v2::clob::ws::types::response::PriceChange;
use polymarket_client_sdk_v2::clob::{Client as ClobClient, Config as ClobConfig, ws};
use polymarket_client_sdk_v2::data::types::request::PositionsRequest;
use polymarket_client_sdk_v2::data::{self};
use polymarket_client_sdk_v2::gamma;
use polymarket_client_sdk_v2::gamma::types::request::EventsRequest;
use polymarket_client_sdk_v2::gamma::types::response::Event;
use polymarket_client_sdk_v2::types::{Address, DateTime, U256, Utc};
use polymarket_client_sdk_v2::{POLYGON, PRIVATE_KEY_VAR, derive_proxy_wallet, derive_safe_wallet};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::str::FromStr as _;
use tokio::sync::mpsc;

use crate::error::{Error, Result};
use crate::types::{
    Level, MarketId, Order, Orderbook, OrderStatus, OrderType, PlaceOrderRequest, Position, Side,
    TimeInForce, status_from_fills,
};
use crate::venue::{MarketSpec, OrderbookTrader};

/// The EOA signer type used for order signing.
pub type EoaSigner = LocalSigner<k256::ecdsa::SigningKey>;

/// Re-exported so callers (and tests) can construct API credentials without
/// depending on the SDK crate directly.
pub use polymarket_client_sdk_v2::auth::{Credentials, Uuid};
/// Re-exported: which wallet-onboarding mode a Polymarket account uses.
///
/// | Variant | Who uses it | Funder |
/// |---|---|---|
/// | `Eoa` (default) | raw MetaMask/hardware wallet with on-chain token allowances set directly | the EOA itself |
/// | `Proxy` | email/Magic wallet login on app.polymarket.com | auto-derived (CREATE2) |
/// | `GnosisSafe` | browser wallet (MetaMask, Phantom, etc.) connected via app.polymarket.com | auto-derived (CREATE2) |
/// | `Poly1271` | institutional/deposit-wallet flow | explicit — set [`WalletConfig::funder`] |
///
/// Using the wrong type fails order placement with `maker address not
/// allowed, please use the deposit wallet flow` (server-side rejection).
pub use polymarket_client_sdk_v2::clob::types::SignatureType;

/// Wallet-onboarding override for [`PolymarketVenue::connect_with_wallet`].
/// `Default` (`signature_type: None, funder: None`) matches [`PolymarketVenue::connect`]
/// — plain EOA, no funder.
#[derive(Clone, Debug, Default)]
pub struct WalletConfig {
    pub signature_type: Option<SignatureType>,
    /// Explicit funder/deposit-wallet address. Leave unset for `Proxy`/`GnosisSafe`
    /// (auto-derived via CREATE2); required for `Poly1271`.
    pub funder: Option<Address>,
}

type AuthedClob = ClobClient<Authenticated<Normal>>;

const DEFAULT_HOST: &str = "https://clob-v2.polymarket.com";

/// Authenticated trading state (absent in market-data-only mode).
struct Trading {
    clob: AuthedClob,
    signer: EoaSigner,
    /// The actual maker/asset-holding address: the explicit `WalletConfig`
    /// funder if set, else the CREATE2-derived Proxy/Safe address, else the
    /// raw EOA (`Eoa` signature type).
    funder: Address,
}

/// Resolve the address that actually holds funds/positions for this account,
/// mirroring the SDK's own funder resolution (`WalletConfig` funder, else
/// CREATE2-derive for `Proxy`/`GnosisSafe`, else the raw EOA for `Eoa`).
///
/// Needed because the SDK does not expose the resolved funder on the
/// authenticated client, but the Data API's `user` positions/balance
/// parameter must be this address, not the raw signer address.
fn resolve_funder(wallet: &WalletConfig, eoa: Address) -> Address {
    if let Some(funder) = wallet.funder {
        return funder;
    }
    match wallet.signature_type {
        Some(SignatureType::Proxy) => derive_proxy_wallet(eoa, POLYGON).unwrap_or(eoa),
        Some(SignatureType::GnosisSafe) => derive_safe_wallet(eoa, POLYGON).unwrap_or(eoa),
        _ => eoa,
    }
}

/// Polymarket venue adapter implementing [`OrderbookTrader`].
///
/// Construct with [`PolymarketVenue::connect`] / [`PolymarketVenue::from_env`]
/// for trading, or [`PolymarketVenue::market_data`] for credential-free
/// orderbook subscriptions (trading/account methods then return
/// [`Error::Config`]).
pub struct PolymarketVenue {
    trading: Option<Trading>,
    ws: ws::Client<Unauthenticated>,
    data: data::Client,
    gamma: gamma::Client,
}

/// One market of a Gamma event, resolved to tradable outcome tokens.
///
/// Produced by [`PolymarketVenue::resolve_event_markets`]. Each entry in
/// `outcomes` pairs the outcome label (e.g. `"Yes"`) with the [`MarketId`]
/// (outcome token id) accepted by every [`OrderbookTrader`] method.
#[derive(Clone, Debug, PartialEq)]
pub struct EventMarket {
    /// e.g. `"Will Spain win on 2026-07-19?"`
    pub question: String,
    /// Gamma market slug, e.g. `"fifwc-esp-arg-2026-07-19-esp"`.
    pub slug: String,
    /// `(outcome label, token id)` pairs, e.g. `("Yes", ..), ("No", ..)`.
    pub outcomes: Vec<(String, MarketId)>,
}

impl PolymarketVenue {
    /// Authenticate against `host` with an EOA private key, using the plain
    /// `Eoa` signature type (no funder). If your account was onboarded
    /// through the Polymarket web app (email or browser wallet), use
    /// [`PolymarketVenue::connect_with_wallet`] instead — see [`SignatureType`].
    pub async fn connect(host: &str, private_key: &str) -> Result<Self> {
        Self::connect_full(host, private_key, None, WalletConfig::default()).await
    }

    /// Authenticate with an explicit [`WalletConfig`] (signature type / funder).
    ///
    /// Required whenever the account isn't a plain EOA — e.g. a browser
    /// wallet connected via app.polymarket.com needs `SignatureType::GnosisSafe`:
    ///
    /// ```no_run
    /// # async fn run() -> rusk_sdk::Result<()> {
    /// use rusk_sdk::polymarket::{PolymarketVenue, SignatureType, WalletConfig};
    /// let venue = PolymarketVenue::connect_with_wallet(
    ///     "https://clob.polymarket.com",
    ///     "0x...",
    ///     WalletConfig { signature_type: Some(SignatureType::GnosisSafe), funder: None },
    /// ).await?;
    /// # Ok(()) }
    /// ```
    pub async fn connect_with_wallet(
        host: &str,
        private_key: &str,
        wallet: WalletConfig,
    ) -> Result<Self> {
        Self::connect_full(host, private_key, None, wallet).await
    }

    /// Authenticate with pre-existing API [`Credentials`], skipping the
    /// create/derive-api-key network round trip entirely (useful to reuse
    /// long-lived credentials).
    pub async fn connect_with_credentials(
        host: &str,
        private_key: &str,
        credentials: Credentials,
    ) -> Result<Self> {
        Self::connect_full(host, private_key, Some(credentials), WalletConfig::default()).await
    }

    async fn connect_full(
        host: &str,
        private_key: &str,
        credentials: Option<Credentials>,
        wallet: WalletConfig,
    ) -> Result<Self> {
        let signer = EoaSigner::from_str(private_key)
            .map_err(|e| Error::Config(format!("invalid Polymarket private key: {e}")))?
            .with_chain_id(Some(POLYGON));
        let mut builder = ClobClient::new(host, ClobConfig::default())?
            .authentication_builder(&signer);
        if let Some(credentials) = credentials {
            builder = builder.credentials(credentials);
        }
        if let Some(signature_type) = wallet.signature_type {
            builder = builder.signature_type(signature_type);
        }
        if let Some(funder) = wallet.funder {
            builder = builder.funder(funder);
        }
        let clob = builder.authenticate().await?;
        let funder = resolve_funder(&wallet, clob.address());
        Ok(Self {
            trading: Some(Trading { clob, signer, funder }),
            ws: ws::Client::default(),
            data: data::Client::default(),
            gamma: gamma::Client::default(),
        })
    }

    /// Build a credential-free client for public market data
    /// (`subscribe_orderbook` and `resolve_event_markets` only).
    pub fn market_data() -> Self {
        Self {
            trading: None,
            ws: ws::Client::default(),
            data: data::Client::default(),
            gamma: gamma::Client::default(),
        }
    }

    /// Resolve a `polymarket.com` **event slug** (the tail of an event URL,
    /// e.g. `fifwc-esp-arg-2026-07-19`) to its markets and outcome token ids.
    ///
    /// Works without credentials.
    pub async fn resolve_event_markets(&self, event_slug: &str) -> Result<Vec<EventMarket>> {
        let request = EventsRequest::builder()
            .slug(vec![event_slug.to_owned()])
            .build();
        let events = self.gamma.events(&request).await?;
        let markets = event_markets(&events);
        if markets.is_empty() {
            return Err(Error::InvalidInput(format!(
                "no Polymarket markets found for event slug {event_slug:?}"
            )));
        }
        Ok(markets)
    }

    /// Build from `POLYMARKET_PRIVATE_KEY` (and optional `POLYMARKET_HOST`,
    /// `POLYMARKET_SIGNATURE_TYPE`, `POLYMARKET_FUNDER`) — read from the
    /// process environment, falling back to `.env` (auto-loaded).
    pub async fn from_env() -> Result<Self> {
        crate::load_dotenv();
        let private_key = std::env::var(PRIVATE_KEY_VAR)
            .map_err(|_| Error::Config(format!("{PRIVATE_KEY_VAR} is not set")))?;
        let host =
            std::env::var("POLYMARKET_HOST").unwrap_or_else(|_| DEFAULT_HOST.to_owned());
        let wallet = wallet_config_from_env()?;
        Self::connect_full(&host, &private_key, None, wallet).await
    }

    fn trading(&self) -> Result<&Trading> {
        self.trading.as_ref().ok_or_else(|| {
            Error::Config(
                "Polymarket credentials required: construct with PolymarketVenue::connect \
                 or from_env"
                    .into(),
            )
        })
    }

    /// The resolved maker/asset-holding address for this account: the
    /// explicit `WalletConfig` funder if set, else the CREATE2-derived
    /// Proxy/Safe address, else the raw EOA (`Eoa` signature type). This is
    /// what actually holds positions/collateral — not necessarily the
    /// signer's own address.
    pub fn funder(&self) -> Result<Address> {
        Ok(self.trading()?.funder)
    }

    async fn place_limit(
        &self,
        token: U256,
        request: &PlaceOrderRequest,
    ) -> Result<PostOrderResponse> {
        let trading = self.trading()?;
        let price = request
            .price
            .ok_or_else(|| Error::InvalidInput("limit orders require a price".into()))?;
        validate_limit_inputs(price, request.size)?;
        let (order_type, expiration) = limit_order_type(request.tif)?;
        let mut builder = trading
            .clob
            .limit_order()
            .token_id(token)
            .side(to_pm_side(request.side))
            .price(price)
            .size(request.size)
            .order_type(order_type)
            .post_only(request.post_only);
        if let Some(expiration) = expiration {
            builder = builder.expiration(expiration);
        }
        let order = builder.build().await?;
        let signed = trading.clob.sign(&trading.signer, order).await?;
        Ok(trading.clob.post_order(signed).await?)
    }

    async fn place_market(
        &self,
        token: U256,
        request: &PlaceOrderRequest,
    ) -> Result<PostOrderResponse> {
        let trading = self.trading()?;
        validate_size(request.size)?;
        let order_type = market_order_type(request.tif);
        let order = trading
            .clob
            .market_order()
            .token_id(token)
            .side(to_pm_side(request.side))
            .amount(Amount::shares(request.size)?)
            .order_type(order_type)
            .build()
            .await?;
        let signed = trading.clob.sign(&trading.signer, order).await?;
        Ok(trading.clob.post_order(signed).await?)
    }
}

/// Flatten Gamma events into [`EventMarket`]s, pairing each outcome label
/// with its CLOB token id. Markets without token ids are skipped.
fn event_markets(events: &[Event]) -> Vec<EventMarket> {
    events
        .iter()
        .flat_map(|event| event.markets.iter().flatten())
        .filter_map(|market| {
            let tokens = market.clob_token_ids.as_ref()?;
            let outcomes = market
                .outcomes
                .clone()
                .unwrap_or_else(|| (1..=tokens.len()).map(|i| format!("outcome {i}")).collect());
            Some(EventMarket {
                question: market.question.clone().unwrap_or_default(),
                slug: market.slug.clone().unwrap_or_default(),
                outcomes: outcomes
                    .into_iter()
                    .zip(tokens.iter())
                    .map(|(label, token)| (label, MarketId::new(token.to_string())))
                    .collect(),
            })
        })
        .collect()
}

/// Pre-validate limit order inputs before hitting the SDK/venue.
///
/// Prices live strictly inside (0, 1); sizes are positive with at most
/// 2 decimal places (the CLOB lot size). Per-market tick alignment and
/// minimum order size are still enforced server-side.
fn validate_limit_inputs(price: Decimal, size: Decimal) -> Result<()> {
    if price <= Decimal::ZERO || price >= Decimal::ONE {
        return Err(Error::InvalidInput(format!(
            "limit price must be strictly between 0 and 1, got {price}"
        )));
    }
    validate_size(size)
}

/// Sizes must be positive with at most 2 decimal places.
fn validate_size(size: Decimal) -> Result<()> {
    if size <= Decimal::ZERO {
        return Err(Error::InvalidInput(format!(
            "size must be positive, got {size}"
        )));
    }
    if size.normalize().scale() > 2 {
        return Err(Error::InvalidInput(format!(
            "Polymarket sizes allow at most 2 decimal places, got {size}"
        )));
    }
    Ok(())
}

/// `POLYMARKET_SIGNATURE_TYPE` / `POLYMARKET_FUNDER` -> [`WalletConfig`].
fn wallet_config_from_env() -> Result<WalletConfig> {
    let signature_type = std::env::var("POLYMARKET_SIGNATURE_TYPE")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map(|v| parse_signature_type(&v))
        .transpose()?;
    let funder = std::env::var("POLYMARKET_FUNDER")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map(|v| {
            Address::from_str(v.trim())
                .map_err(|e| Error::Config(format!("invalid POLYMARKET_FUNDER: {e}")))
        })
        .transpose()?;
    Ok(WalletConfig { signature_type, funder })
}

/// Accepts the numeric discriminant (`"0"`-`"3"`) or a case-insensitive name
/// (`eoa`, `proxy`, `gnosis_safe`/`gnosissafe`/`safe`, `poly1271`).
fn parse_signature_type(raw: &str) -> Result<SignatureType> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "0" | "eoa" => Ok(SignatureType::Eoa),
        "1" | "proxy" => Ok(SignatureType::Proxy),
        "2" | "gnosis_safe" | "gnosissafe" | "safe" => Ok(SignatureType::GnosisSafe),
        "3" | "poly1271" => Ok(SignatureType::Poly1271),
        other => Err(Error::Config(format!(
            "unknown POLYMARKET_SIGNATURE_TYPE {other:?}; \
             expected eoa|proxy|gnosis_safe|poly1271 (or 0-3)"
        ))),
    }
}

fn parse_token(market: &MarketId) -> Result<U256> {
    U256::from_str(market.as_str()).map_err(|e| {
        Error::InvalidInput(format!(
            "Polymarket market id must be a token id (U256): {market}: {e}"
        ))
    })
}

fn to_pm_side(side: Side) -> PmSide {
    match side {
        Side::Buy => PmSide::Buy,
        Side::Sell => PmSide::Sell,
    }
}

/// Map abstract TIF to Polymarket limit order type + optional GTD expiration.
fn limit_order_type(tif: TimeInForce) -> Result<(PmOrderType, Option<DateTime<Utc>>)> {
    match tif {
        TimeInForce::Gtc => Ok((PmOrderType::GTC, None)),
        TimeInForce::Gtd { expires_at_ms } => {
            let expiration = DateTime::<Utc>::from_timestamp_millis(expires_at_ms)
                .ok_or_else(|| Error::InvalidInput("invalid GTD expiry".into()))?;
            Ok((PmOrderType::GTD, Some(expiration)))
        }
        TimeInForce::Ioc => Ok((PmOrderType::FAK, None)),
        TimeInForce::Fok => Ok((PmOrderType::FOK, None)),
    }
}

/// Market orders are immediate: FOK stays FOK, everything else is FAK.
fn market_order_type(tif: TimeInForce) -> PmOrderType {
    match tif {
        TimeInForce::Fok => PmOrderType::FOK,
        _ => PmOrderType::FAK,
    }
}

/// Map a Polymarket order status plus fill accounting to the normalized status.
fn map_status(status: &OrderStatusType, filled: Decimal, original: Decimal) -> OrderStatus {
    match status {
        OrderStatusType::Live | OrderStatusType::Delayed => {
            status_from_fills(true, filled, original)
        }
        OrderStatusType::Matched => OrderStatus::Filled,
        OrderStatusType::Canceled => OrderStatus::Canceled,
        OrderStatusType::Unmatched => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

/// Normalize a `POST /order` response. `taking_amount`/`making_amount` are the
/// immediately-executed amounts; the share leg depends on the order side.
fn order_from_post_response(
    response: &PostOrderResponse,
    side: Side,
    requested_size: Decimal,
) -> Result<Order> {
    if !response.success {
        return Err(Error::Venue {
            code: format!("{:?}", response.status),
            message: response
                .error_msg
                .clone()
                .unwrap_or_else(|| "order rejected".into()),
        });
    }
    let filled = match side {
        Side::Buy => response.taking_amount,
        Side::Sell => response.making_amount,
    };
    let remaining = (requested_size - filled).max(Decimal::ZERO);
    Ok(Order {
        order_id: response.order_id.clone(),
        status: map_status(&response.status, filled, requested_size),
        filled_size: filled,
        remaining_size: remaining,
    })
}

/// Normalize a fetched open order.
fn order_from_open_order(response: &OpenOrderResponse) -> Order {
    let filled = response.size_matched;
    let original = response.original_size;
    let remaining = (original - filled).max(Decimal::ZERO);
    Order {
        order_id: response.id.clone(),
        status: map_status(&response.status, filled, original),
        filled_size: filled,
        remaining_size: remaining,
    }
}

/// Locally-maintained full L2 book for a single asset, fed by *both* of the
/// SDK's market-data streams so callers only need [`PolymarketVenue::subscribe_orderbook`]:
///
/// - `book` (`ws::Client::subscribe_orderbook`) — the authoritative full
///   snapshot, resent by the venue on every subscribe and whenever a trade
///   affects the book. Slow-ish, but always complete: fully replaces local
///   state (`apply_book_update`).
/// - `price_change` (`ws::Client::subscribe_prices`) — fast, low-latency
///   per-level deltas emitted on every new order/cancel. Applied on top of
///   the last snapshot (`apply_price_change`): a `size` of zero (or absent)
///   removes that price level, otherwise it's an absolute size (not a
///   delta-of-delta) that upserts the level.
///
/// Both streams are subscribed for the same `asset_id`/`MarketId`, so every
/// update — from either feed — is merged into one always-current full-book
/// view via [`MergedBookState::to_orderbook`].
#[derive(Default)]
struct MergedBookState {
    /// price -> size
    bids: std::collections::BTreeMap<Decimal, Decimal>,
    asks: std::collections::BTreeMap<Decimal, Decimal>,
    timestamp_ms: i64,
    /// Whether at least one `book` snapshot has been applied. `price_change`
    /// deltas arriving before the first snapshot are not meaningful on their
    /// own (nothing to apply them on top of) and must not be surfaced as a
    /// "full" orderbook, so callers gate emission on this.
    has_snapshot: bool,
}

impl MergedBookState {
    /// Replace the whole book from a `book` (full snapshot) message.
    fn apply_book_update(&mut self, update: &BookUpdate) {
        self.bids.clear();
        self.asks.clear();
        for level in &update.bids {
            self.bids.insert(level.price, level.size);
        }
        for level in &update.asks {
            self.asks.insert(level.price, level.size);
        }
        self.timestamp_ms = update.timestamp;
        self.has_snapshot = true;
    }

    /// Apply every `price_changes` entry matching `token` (a batch can carry
    /// more than one entry for the same asset, e.g. a bid and an ask level
    /// changing together — all matching entries are applied, not just the
    /// first). Returns `true` if the merged book actually changed *and*
    /// there's a base snapshot to have changed on top of, i.e. the caller
    /// should re-emit.
    fn apply_price_change(&mut self, update: &PriceChange, token: U256) -> bool {
        let mut changed = false;
        for entry in update.price_changes.iter().filter(|e| e.asset_id == token) {
            changed = true;
            let book = match entry.side {
                PmSide::Buy => &mut self.bids,
                PmSide::Sell => &mut self.asks,
                _ => continue, // Unknown side variant: nothing sensible to apply.
            };
            match entry.size {
                Some(size) if size > Decimal::ZERO => {
                    book.insert(entry.price, size);
                }
                _ => {
                    book.remove(&entry.price);
                }
            }
        }
        if changed {
            self.timestamp_ms = update.timestamp;
        }
        changed && self.has_snapshot
    }

    /// Materialize a full [`Orderbook`] snapshot (bids best-first, asks best-first).
    fn to_orderbook(&self, market: &MarketId, received_at_ms: i64) -> Orderbook {
        Orderbook {
            market: market.clone(),
            bids: self
                .bids
                .iter()
                .rev()
                .map(|(&price, &size)| Level { price, size })
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(&price, &size)| Level { price, size })
                .collect(),
            timestamp_ms: self.timestamp_ms,
            received_at_ms,
        }
    }
}

fn convert_prices(update: &PriceChange, market: &MarketId, received_at_ms: i64) -> Orderbook {
    let entry = update
        .price_changes
        .iter()
        .find(|e| e.asset_id.to_string() == market.0)
        .expect("asset_id not found in price_changes");

    let bids = entry
        .best_bid
        .map(|price| {
            vec![Level {
                price,
                size: entry.size.unwrap_or_default(),
            }]
        })
        .unwrap_or_default();

    let asks = entry
        .best_ask
        .map(|price| {
            vec![Level {
                price,
                size: entry.size.unwrap_or_default(),
            }]
        })
        .unwrap_or_default();

    Orderbook {
        market: market.clone(),
        bids,
        asks,
        timestamp_ms: update.timestamp,
        received_at_ms,
    }
}

#[async_trait]
impl OrderbookTrader for PolymarketVenue {
    async fn place_order(&self, request: PlaceOrderRequest) -> Result<Order> {
        let token = parse_token(&request.market)?;
        let response = match request.order_type {
            OrderType::Limit => self.place_limit(token, &request).await?,
            OrderType::Market => self.place_market(token, &request).await?,
        };
        order_from_post_response(&response, request.side, request.size)
    }

    async fn cancel_order(&self, order_id: &str) -> Result<()> {
        let response = self.trading()?.clob.cancel_order(order_id).await?;
        if response.canceled.iter().any(|id| id == order_id) {
            return Ok(());
        }
        match response.not_canceled.get(order_id) {
            Some(reason) => Err(Error::Venue {
                code: "NOT_CANCELED".into(),
                message: reason.clone(),
            }),
            None => Err(Error::OrderNotFound(order_id.to_owned())),
        }
    }

    async fn fetch_order(&self, order_id: &str) -> Result<Order> {
        let response = self.trading()?.clob.order(order_id).await?;
        Ok(order_from_open_order(&response))
    }

    async fn modify_order(
        &self,
        order_id: &str,
        new_price: Decimal,
        new_size: Decimal,
    ) -> Result<Order> {
        // Polymarket has no native modify: cancel, then re-place with the
        // original token/side. The returned order has a new id.
        let existing = self.trading()?.clob.order(order_id).await?;
        self.cancel_order(order_id).await?;

        let side = match existing.side {
            PmSide::Buy => Side::Buy,
            PmSide::Sell => Side::Sell,
            _ => return Err(Error::Serde("unknown side on existing order".into())),
        };
        let request = PlaceOrderRequest {
            market: MarketId::new(existing.asset_id.to_string()),
            side,
            order_type: OrderType::Limit,
            price: Some(new_price),
            size: new_size,
            // GTD expirations are not carried over; the replacement rests GTC.
            tif: TimeInForce::Gtc,
            post_only: false,
        };
        let response = self.place_limit(existing.asset_id, &request).await?;
        order_from_post_response(&response, side, new_size)
    }

    async fn fetch_position(&self, market: &MarketId) -> Result<Position> {
        let token = parse_token(market)?;
        let request = PositionsRequest::builder()
            .user(self.trading()?.funder)
            .build();
        let positions = self.data.positions(&request).await?;
        let position = positions
            .iter()
            .find(|position| position.asset == token)
            .map(|position| Position {
                market: market.clone(),
                size: position.size,
                avg_price: Some(position.avg_price),
            })
            .unwrap_or_else(|| Position {
                market: market.clone(),
                size: Decimal::ZERO,
                avg_price: None,
            });
        Ok(position)
    }

    /// Polymarket's tick size is fixed per market, so it's fetched once here
    /// and the returned closure just ignores its `price` argument and hands
    /// back the cached constant (no per-call I/O). Polymarket also has no
    /// maker-rebate program, so `maker_rebate_share` is always zero.
    async fn fetch_market(&self, market: &MarketId) -> Result<MarketSpec> {
        let token = parse_token(market)?;
        let clob = &self.trading()?.clob;
        let tick_size = clob.tick_size(token).await?.minimum_tick_size.as_decimal();
        let base_fee = clob.fee_rate_bps(token).await?.base_fee;
        Ok(MarketSpec {
            taker_fee_rate: Decimal::from(base_fee) / dec!(10_000),
            maker_rebate_share: Decimal::ZERO,
            tick_size: std::sync::Arc::new(move |_price| Ok(tick_size)),
        })
    }

    /// Subscribes to *both* underlying SDK feeds for `market` and merges them
    /// into one always-current full-book stream (see [`MergedBookState`]):
    /// the slow-but-authoritative `book` snapshots and the fast `price_change`
    /// deltas. Every message from either feed updates local state and, once
    /// an initial snapshot has been seen, emits a freshly-materialized full
    /// [`Orderbook`] — callers get full-book accuracy at price-change speed
    /// without subscribing to `subscribe_prices` themselves.
    async fn subscribe_orderbook(&self, market: &MarketId) -> Result<mpsc::Receiver<Orderbook>> {
        let token = parse_token(market)?;
        let book_stream = self.ws.subscribe_orderbook(vec![token])?;
        let price_stream = self.ws.subscribe_prices(vec![token])?;
        let (tx, rx) = mpsc::channel(64);
        let market = market.clone();
        tokio::spawn(async move {
            let mut book_stream = Box::pin(book_stream);
            let mut price_stream = Box::pin(price_stream);
            let mut state = MergedBookState::default();
            loop {
                tokio::select! {
                    update = book_stream.next() => {
                        let Some(Ok(update)) = update else { break };
                        state.apply_book_update(&update);
                        let book = state.to_orderbook(&market, crate::types::unix_now_ms());
                        if tx.send(book).await.is_err() {
                            break; // receiver dropped
                        }
                    }
                    update = price_stream.next() => {
                        let Some(Ok(update)) = update else { break };
                        if state.apply_price_change(&update, token) {
                            let book = state.to_orderbook(&market, crate::types::unix_now_ms());
                            if tx.send(book).await.is_err() {
                                break; // receiver dropped
                            }
                        }
                    }
                }
            }
        });
        Ok(rx)
    }

    async fn subscribe_prices(&self, market: &MarketId) -> Result<mpsc::Receiver<Orderbook>> {
        let token = parse_token(market)?;
        let stream = self.ws.subscribe_prices(vec![token])?;
        let (tx, rx) = mpsc::channel(64);
        let market = market.clone();
        tokio::spawn(async move {
            let mut stream = Box::pin(stream);
            while let Some(update) = stream.next().await {
                let Ok(update) = update else { break };
                let book = convert_prices(&update, &market, crate::types::unix_now_ms());
                if tx.send(book).await.is_err() {
                    break; // receiver dropped
                }
            }
        });
        Ok(rx)
    }
}

#[cfg(test)]
mod tests {
    use polymarket_client_sdk_v2::clob::ws::types::response::{OrderBookLevel, PriceChangeBatchEntry};
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn token_parsing() {
        assert!(parse_token(&MarketId::from("123456789")).is_ok());
        assert!(parse_token(&MarketId::from("not-a-token")).is_err());
    }

    #[test]
    fn signature_type_parses_names_and_numbers_case_insensitively() {
        for (raw, expected) in [
            ("eoa", SignatureType::Eoa),
            ("0", SignatureType::Eoa),
            ("Proxy", SignatureType::Proxy),
            ("1", SignatureType::Proxy),
            ("gnosis_safe", SignatureType::GnosisSafe),
            ("GnosisSafe", SignatureType::GnosisSafe),
            ("safe", SignatureType::GnosisSafe),
            ("2", SignatureType::GnosisSafe), // the case reported against a Phantom/browser account
            ("poly1271", SignatureType::Poly1271),
            ("3", SignatureType::Poly1271),
        ] {
            assert_eq!(
                parse_signature_type(raw).unwrap(),
                expected,
                "input {raw:?}"
            );
        }
        assert!(matches!(
            parse_signature_type("bogus"),
            Err(Error::Config(_))
        ));
    }

    #[test]
    fn wallet_config_default_is_plain_eoa_no_funder() {
        let wallet = WalletConfig::default();
        assert!(wallet.signature_type.is_none());
        assert!(wallet.funder.is_none());
    }

    /// Regression test: `fetch_position`/balance lookups must use the
    /// resolved funder (asset-holding address), not the raw signer address —
    /// a real bug found via live testing where `Proxy`/`GnosisSafe`/`Poly1271`
    /// accounts always read back a flat position because the wrong address
    /// was queried.
    #[test]
    fn resolve_funder_prefers_explicit_then_derives_then_falls_back_to_eoa() {
        let eoa = Address::from([0x11; 20]);
        let explicit = Address::from([0x22; 20]);

        // Explicit funder always wins, regardless of signature_type.
        let wallet = WalletConfig {
            signature_type: Some(SignatureType::Eoa),
            funder: Some(explicit),
        };
        assert_eq!(resolve_funder(&wallet, eoa), explicit);
        let wallet = WalletConfig {
            signature_type: Some(SignatureType::Poly1271),
            funder: Some(explicit),
        };
        assert_eq!(resolve_funder(&wallet, eoa), explicit);

        // No funder + Eoa (or unset) -> the raw EOA itself.
        let wallet = WalletConfig::default();
        assert_eq!(resolve_funder(&wallet, eoa), eoa);
        let wallet = WalletConfig {
            signature_type: Some(SignatureType::Eoa),
            funder: None,
        };
        assert_eq!(resolve_funder(&wallet, eoa), eoa);

        // No funder + Proxy/GnosisSafe -> CREATE2-derived, and NOT the EOA
        // itself (this is exactly the bug: using the EOA here reads the
        // wrong account's positions).
        let wallet = WalletConfig {
            signature_type: Some(SignatureType::Proxy),
            funder: None,
        };
        let derived_proxy = resolve_funder(&wallet, eoa);
        assert_ne!(derived_proxy, eoa);
        assert_eq!(derived_proxy, derive_proxy_wallet(eoa, POLYGON).unwrap());

        let wallet = WalletConfig {
            signature_type: Some(SignatureType::GnosisSafe),
            funder: None,
        };
        let derived_safe = resolve_funder(&wallet, eoa);
        assert_ne!(derived_safe, eoa);
        assert_ne!(derived_safe, derived_proxy, "Proxy and Safe derive different addresses");
        assert_eq!(derived_safe, derive_safe_wallet(eoa, POLYGON).unwrap());
    }

    #[test]
    fn tif_mapping_for_limit_orders() {
        assert!(matches!(
            limit_order_type(TimeInForce::Gtc).unwrap(),
            (PmOrderType::GTC, None)
        ));
        let (order_type, expiration) =
            limit_order_type(TimeInForce::Gtd { expires_at_ms: 1_731_536_000_000 }).unwrap();
        assert!(matches!(order_type, PmOrderType::GTD));
        assert_eq!(expiration.unwrap().timestamp_millis(), 1_731_536_000_000);
        assert!(matches!(
            limit_order_type(TimeInForce::Ioc).unwrap(),
            (PmOrderType::FAK, None)
        ));
        assert!(matches!(
            limit_order_type(TimeInForce::Fok).unwrap(),
            (PmOrderType::FOK, None)
        ));
    }

    #[test]
    fn tif_mapping_for_market_orders() {
        assert!(matches!(
            market_order_type(TimeInForce::Fok),
            PmOrderType::FOK
        ));
        assert!(matches!(
            market_order_type(TimeInForce::Gtc),
            PmOrderType::FAK
        ));
        assert!(matches!(
            market_order_type(TimeInForce::Ioc),
            PmOrderType::FAK
        ));
    }

    #[test]
    fn status_mapping() {
        assert_eq!(
            map_status(&OrderStatusType::Live, dec!(0), dec!(10)),
            OrderStatus::Open
        );
        assert_eq!(
            map_status(&OrderStatusType::Live, dec!(4), dec!(10)),
            OrderStatus::PartiallyFilled
        );
        assert_eq!(
            map_status(&OrderStatusType::Matched, dec!(10), dec!(10)),
            OrderStatus::Filled
        );
        assert_eq!(
            map_status(&OrderStatusType::Canceled, dec!(0), dec!(10)),
            OrderStatus::Canceled
        );
        assert_eq!(
            map_status(&OrderStatusType::Unmatched, dec!(0), dec!(10)),
            OrderStatus::Rejected
        );
        assert_eq!(
            map_status(&OrderStatusType::Unknown("weird".into()), dec!(0), dec!(1)),
            OrderStatus::Unknown
        );
    }

    #[test]
    fn post_response_mapping_uses_side_for_filled_leg() {
        // PostOrderResponse is #[non_exhaustive]; deserialize like the wire does.
        let response: PostOrderResponse = serde_json::from_str(
            r#"{
                "errorMsg": null,
                "makingAmount": "5.5",
                "takingAmount": "10",
                "orderID": "0xabc",
                "status": "MATCHED",
                "success": true,
                "transactionsHashes": [],
                "tradeIds": []
            }"#,
        )
        .unwrap();

        let buy = order_from_post_response(&response, Side::Buy, dec!(10)).unwrap();
        assert_eq!(buy.order_id, "0xabc");
        assert_eq!(buy.status, OrderStatus::Filled);
        assert_eq!(buy.filled_size, dec!(10)); // taking leg
        assert_eq!(buy.remaining_size, dec!(0));

        let sell = order_from_post_response(&response, Side::Sell, dec!(10)).unwrap();
        assert_eq!(sell.filled_size, dec!(5.5)); // making leg
        assert_eq!(sell.remaining_size, dec!(4.5));
    }

    #[test]
    fn failed_post_response_becomes_venue_error() {
        let response: PostOrderResponse = serde_json::from_str(
            r#"{
                "errorMsg": "not enough balance",
                "makingAmount": "",
                "takingAmount": "",
                "orderID": "",
                "status": "UNMATCHED",
                "success": false,
                "transactionsHashes": [],
                "tradeIds": []
            }"#,
        )
        .unwrap();
        match order_from_post_response(&response, Side::Buy, dec!(1)).unwrap_err() {
            Error::Venue { message, .. } => assert_eq!(message, "not enough balance"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    /// "$1-size" trading edge cases: tiny orders around $1 notional must be
    /// accepted, while malformed prices/sizes fail fast with `InvalidInput`.
    #[test]
    fn dollar_size_orders_validate() {
        // $1 notional at 50c = 2 shares; at 42.5c ≈ 2.35 shares (2 dp) — OK.
        validate_limit_inputs(dec!(0.50), dec!(2)).unwrap();
        validate_limit_inputs(dec!(0.425), dec!(2.35)).unwrap();
        // 1 share and fractional 2-dp shares — OK.
        validate_limit_inputs(dec!(0.999), dec!(1)).unwrap();
        validate_limit_inputs(dec!(0.001), dec!(0.01)).unwrap();
        // Trailing zeros don't count as extra precision.
        validate_limit_inputs(dec!(0.50), dec!(1.500)).unwrap();
    }

    #[test]
    fn price_out_of_range_rejected() {
        for price in [dec!(0), dec!(1), dec!(1.01), dec!(-0.5)] {
            assert!(
                matches!(
                    validate_limit_inputs(price, dec!(1)),
                    Err(Error::InvalidInput(_))
                ),
                "price {price} must be rejected"
            );
        }
    }

    #[test]
    fn too_small_or_too_precise_size_rejected() {
        // Zero / negative.
        assert!(matches!(validate_size(dec!(0)), Err(Error::InvalidInput(_))));
        assert!(matches!(validate_size(dec!(-1)), Err(Error::InvalidInput(_))));
        // More than 2 decimal places (sub-lot size).
        assert!(matches!(
            validate_size(dec!(1.001)),
            Err(Error::InvalidInput(_))
        ));
        assert!(matches!(
            validate_size(dec!(0.005)),
            Err(Error::InvalidInput(_))
        ));
    }

    #[test]
    fn market_data_mode_rejects_trading_calls() {
        let venue = PolymarketVenue::market_data();
        assert!(matches!(venue.trading(), Err(Error::Config(_))));
    }

    #[test]
    fn sdk_validation_errors_map_to_polymarket_variant() {
        // 7 decimal places exceeds USDC precision -> SDK validation error.
        let sdk_err = Amount::usdc(dec!(0.00000001)).unwrap_err();
        let mapped: Error = sdk_err.into();
        assert!(matches!(mapped, Error::Polymarket(_)));
    }

    #[test]
    fn event_markets_pair_outcomes_with_token_ids() {
        // Shape of GET gamma-api.polymarket.com/events?slug=..: outcomes and
        // clobTokenIds are JSON-encoded *strings* inside the JSON document.
        let event: Event = serde_json::from_str(
            r#"{
                "id": "708597",
                "slug": "fifwc-esp-arg-2026-07-19",
                "markets": [
                    {
                        "id": "2941974",
                        "question": "Will Spain win on 2026-07-19?",
                        "slug": "fifwc-esp-arg-2026-07-19-esp",
                        "outcomes": "[\"Yes\", \"No\"]",
                        "clobTokenIds": "[\"105\", \"269\"]"
                    },
                    {
                        "id": "2941975",
                        "question": "No tokens yet",
                        "slug": "skipped-market"
                    }
                ]
            }"#,
        )
        .unwrap();

        let markets = event_markets(&[event]);
        assert_eq!(markets.len(), 1, "markets without token ids are skipped");
        assert_eq!(markets[0].question, "Will Spain win on 2026-07-19?");
        assert_eq!(markets[0].slug, "fifwc-esp-arg-2026-07-19-esp");
        assert_eq!(
            markets[0].outcomes,
            vec![
                ("Yes".to_owned(), MarketId::from("105")),
                ("No".to_owned(), MarketId::from("269")),
            ]
        );
    }

    #[test]
    fn book_update_converts_and_sorts_best_first() {
        // The live feed delivers worst-first (best at the end); the merged
        // state must re-sort into best-first on both sides.
        let update = BookUpdate::builder()
            .asset_id(U256::from(42u64))
            .market(polymarket_client_sdk_v2::types::B256::ZERO)
            .timestamp(1_731_536_000_000)
            .bids(vec![
                OrderBookLevel::builder().price(dec!(0.01)).size(dec!(999)).build(),
                OrderBookLevel::builder().price(dec!(0.54)).size(dec!(15)).build(),
            ])
            .asks(vec![
                OrderBookLevel::builder().price(dec!(0.99)).size(dec!(999)).build(),
                OrderBookLevel::builder().price(dec!(0.56)).size(dec!(12)).build(),
            ])
            .build();
        let market = MarketId::from("42");
        let mut state = MergedBookState::default();
        state.apply_book_update(&update);
        let book = state.to_orderbook(&market, 1_731_536_000_123);
        assert_eq!(book.market, market);
        assert_eq!(book.timestamp_ms, 1_731_536_000_000);
        assert_eq!(book.received_at_ms, 1_731_536_000_123);
        assert_eq!(book.bids[0], Level { price: dec!(0.54), size: dec!(15) });
        assert_eq!(book.bids[1], Level { price: dec!(0.01), size: dec!(999) });
        assert_eq!(book.asks[0], Level { price: dec!(0.56), size: dec!(12) });
        assert_eq!(book.asks[1], Level { price: dec!(0.99), size: dec!(999) });
    }

    /// `price_change` deltas that arrive before any `book` snapshot must not
    /// be surfaced as a "full" orderbook — there's nothing to apply them on
    /// top of yet.
    #[test]
    fn price_change_before_snapshot_does_not_signal_emit() {
        let token = U256::from(42u64);
        let mut state = MergedBookState::default();
        let update = PriceChange::builder()
            .market(polymarket_client_sdk_v2::types::B256::ZERO)
            .timestamp(1)
            .price_changes(vec![
                PriceChangeBatchEntry::builder()
                    .asset_id(token)
                    .price(dec!(0.5))
                    .side(PmSide::Buy)
                    .size(dec!(10))
                    .build(),
            ])
            .build();
        assert!(!state.apply_price_change(&update, token));
    }

    /// Once a snapshot exists, a batch can carry multiple entries for the
    /// same asset (e.g. a bid and an ask level moving together) — every
    /// matching entry must be applied, not just the first.
    #[test]
    fn price_change_applies_all_matching_entries_in_a_batch() {
        let token = U256::from(42u64);
        let other_token = U256::from(7u64);
        let mut state = MergedBookState::default();
        state.apply_book_update(
            &BookUpdate::builder()
                .asset_id(token)
                .market(polymarket_client_sdk_v2::types::B256::ZERO)
                .timestamp(1)
                .bids(vec![OrderBookLevel::builder().price(dec!(0.48)).size(dec!(10)).build()])
                .asks(vec![OrderBookLevel::builder().price(dec!(0.52)).size(dec!(10)).build()])
                .build(),
        );

        let update = PriceChange::builder()
            .market(polymarket_client_sdk_v2::types::B256::ZERO)
            .timestamp(2)
            .price_changes(vec![
                // New bid level for our asset.
                PriceChangeBatchEntry::builder()
                    .asset_id(token)
                    .price(dec!(0.49))
                    .side(PmSide::Buy)
                    .size(dec!(25))
                    .build(),
                // Existing ask level removed (size "0") for our asset.
                PriceChangeBatchEntry::builder()
                    .asset_id(token)
                    .price(dec!(0.52))
                    .side(PmSide::Sell)
                    .size(dec!(0))
                    .build(),
                // A different asset in the same batch must be ignored.
                PriceChangeBatchEntry::builder()
                    .asset_id(other_token)
                    .price(dec!(0.10))
                    .side(PmSide::Buy)
                    .size(dec!(999))
                    .build(),
            ])
            .build();

        assert!(state.apply_price_change(&update, token));
        let book = state.to_orderbook(&MarketId::from("42"), 0);
        assert_eq!(book.bids.len(), 2);
        assert_eq!(book.bids[0], Level { price: dec!(0.49), size: dec!(25) });
        assert_eq!(book.bids[1], Level { price: dec!(0.48), size: dec!(10) });
        assert!(book.asks.is_empty(), "the only ask level was removed");
        assert_eq!(book.timestamp_ms, 2);
    }
}
