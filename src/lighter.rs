//! Lighter (zkLighter) perp venue capsule, mirroring the [`crate::hyperliquid`]
//! surface: place/modify/cancel/fetch orders, positions, account summary,
//! REST book snapshots and a managed WS book stream.
//!
//! ## Signing
//!
//! Lighter transactions are signed with a Schnorr-style scheme over a custom
//! zk-friendly curve (Poseidon hash). No independent Rust implementation
//! exists, so — exactly like the official Python SDK — this module drives the
//! vendor's canonical Go signer built as a C shared library
//! (`vendor/lighter-signer/lighter-signer-linux-amd64.so`, from
//! <https://github.com/elliottech/lighter-python>), loaded at runtime via
//! `libloading`. Override the library path with `LIGHTER_SIGNER_LIB` on other
//! platforms.
//!
//! ## Environment keys
//!
//! | Key | Meaning |
//! |-----|---------|
//! | `LIGHTER_PRIVATE_KEY` | API-key private key (hex, 40 bytes) — trading mode |
//! | `LIGHTER_PUBLIC_KEY` | matching public key (optional, informational) |
//! | `LIGHTER_API_KEY_INDEX` | API-key slot (2..=254, default 2) |
//! | `LIGHTER_ACCOUNT_INDEX` | account index (auto-discovered from `WALLET_ADDRESS` when unset) |
//! | `LIGHTER_BASE_URL` | REST base (default `https://mainnet.zklighter.elliot.ai`) |
//! | `LIGHTER_SIGNER_LIB` | path to the signer shared library (optional) |
//!
//! Orders are referenced by their **client order index** (a u48 the SDK
//! generates): Lighter accepts it for cancels and modifies, so
//! [`ActiveOrder::order_id`] carries it.

use std::collections::HashMap;
use std::ffi::{CStr, CString, c_char, c_int, c_longlong, c_void};
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::book::{BookUpdate, OrderBook};
use crate::order::{ActiveOrder, OrderInfo, OrderState, Side};

pub const MAINNET_BASE_URL: &str = "https://mainnet.zklighter.elliot.ai";
const MAINNET_CHAIN_ID: c_int = 304;
const TESTNET_CHAIN_ID: c_int = 300;

// Order types (wire constants, see docs/trading).
const ORDER_TYPE_LIMIT: c_int = 0;
const ORDER_TYPE_MARKET: c_int = 1;
// Time in force.
const TIF_IOC: c_int = 0;
const TIF_GOOD_TILL_TIME: c_int = 1;
const TIF_POST_ONLY: c_int = 2;
// Expiry sentinels understood by the signer.
const DEFAULT_28_DAY_ORDER_EXPIRY: c_longlong = -1;
const DEFAULT_IOC_EXPIRY: c_longlong = 0;
const NIL_TRIGGER_PRICE: c_int = 0;
/// Client order indexes are uint48 on the wire; stay below 2^47.
const MAX_CLIENT_ORDER_INDEX: u64 = (1 << 47) - 1;
/// Worst-price slack applied by [`Lighter::place_market`] (crosses the book).
const MARKET_SLIPPAGE: f64 = 0.05;

// ---------------------------------------------------------------------------
// FFI: vendor Go signer shared library (see vendor/lighter-signer/*.h)
// ---------------------------------------------------------------------------

#[repr(C)]
struct FfiStrOrErr {
    str_: *mut c_char,
    err: *mut c_char,
}

#[repr(C)]
struct FfiSignedTx {
    tx_type: u8,
    tx_info: *mut c_char,
    tx_hash: *mut c_char,
    message_to_sign: *mut c_char,
    err: *mut c_char,
}

#[repr(C)]
struct FfiApiKey {
    private_key: *mut c_char,
    public_key: *mut c_char,
    err: *mut c_char,
}

type GenerateApiKeyFn = unsafe extern "C" fn() -> FfiApiKey;
type CreateClientFn =
    unsafe extern "C" fn(*mut c_char, *mut c_char, c_int, c_int, c_longlong) -> *mut c_char;
type CheckClientFn = unsafe extern "C" fn(c_int, c_longlong) -> *mut c_char;
type SignChangePubKeyFn =
    unsafe extern "C" fn(*mut c_char, u8, c_longlong, c_int, c_longlong) -> FfiSignedTx;
#[allow(clippy::type_complexity)]
type SignCreateOrderFn = unsafe extern "C" fn(
    c_int,      // market index
    c_longlong, // client order index
    c_longlong, // base amount
    c_int,      // price
    c_int,      // is ask
    c_int,      // order type
    c_int,      // time in force
    c_int,      // reduce only
    c_int,      // trigger price
    c_longlong, // order expiry
    c_longlong, // integrator account index
    c_int,      // integrator taker fee
    c_int,      // integrator maker fee
    u8,         // self trade behavior mode
    u8,         // self trade equality mode
    u8,         // skip nonce
    c_longlong, // nonce
    c_int,      // api key index
    c_longlong, // account index
) -> FfiSignedTx;
type SignCancelOrderFn =
    unsafe extern "C" fn(c_int, c_longlong, u8, c_longlong, c_int, c_longlong) -> FfiSignedTx;
#[allow(clippy::type_complexity)]
type SignModifyOrderFn = unsafe extern "C" fn(
    c_int,      // market index
    c_longlong, // order index (client order index works)
    c_longlong, // base amount
    c_longlong, // price
    c_longlong, // trigger price
    c_longlong, // integrator account index
    c_int,      // integrator taker fee
    c_int,      // integrator maker fee
    u8,         // self trade behavior mode
    u8,         // self trade equality mode
    u8,         // skip nonce
    c_longlong, // nonce
    c_int,      // api key index
    c_longlong, // account index
) -> FfiSignedTx;
type SignCancelAllFn = unsafe extern "C" fn(
    c_int,
    c_longlong,
    c_int,
    u8,
    c_longlong,
    c_int,
    c_longlong,
) -> FfiSignedTx;
type SignUpdateAccountAssetConfigFn =
    unsafe extern "C" fn(i16, u8, u8, c_longlong, c_int, c_longlong) -> FfiSignedTx;
type CreateAuthTokenFn = unsafe extern "C" fn(c_longlong, c_int, c_longlong) -> FfiStrOrErr;
type FreeFn = unsafe extern "C" fn(*mut c_void);

/// A signed transaction ready for `sendTx`.
#[derive(Clone, Debug)]
pub struct SignedTx {
    pub tx_type: u8,
    pub tx_info: String,
    pub tx_hash: String,
    /// EIP-191 message the L1 wallet must co-sign (`ChangePubKey` only).
    pub message_to_sign: Option<String>,
}

/// Runtime binding to the vendor signer shared library. One per process.
struct SignerLib {
    // Field order matters for drop order (symbols hold references into
    // `_lib`), but we only store raw fn pointers, so the library must simply
    // outlive them: keep it alive forever via the process-wide singleton.
    generate_api_key: GenerateApiKeyFn,
    create_client: CreateClientFn,
    check_client: CheckClientFn,
    sign_change_pub_key: SignChangePubKeyFn,
    sign_create_order: SignCreateOrderFn,
    sign_cancel_order: SignCancelOrderFn,
    sign_modify_order: SignModifyOrderFn,
    #[allow(dead_code)]
    sign_cancel_all: SignCancelAllFn,
    sign_update_account_asset_config: SignUpdateAccountAssetConfigFn,
    create_auth_token: CreateAuthTokenFn,
    free: FreeFn,
    _lib: &'static libloading::Library,
}

fn signer_lib_candidates() -> Vec<std::path::PathBuf> {
    let mut paths = Vec::new();
    if let Ok(explicit) = std::env::var("LIGHTER_SIGNER_LIB") {
        if !explicit.trim().is_empty() {
            paths.push(explicit.into());
        }
    }
    let file = format!(
        "lighter-signer-{os}-{arch}.{ext}",
        os = std::env::consts::OS,
        arch = match std::env::consts::ARCH {
            "x86_64" => "amd64",
            "aarch64" => "arm64",
            other => other,
        },
        ext = match std::env::consts::OS {
            "macos" => "dylib",
            "windows" => "dll",
            _ => "so",
        },
    );
    paths.push(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("vendor/lighter-signer")
            .join(&file),
    );
    paths.push(std::path::Path::new("vendor/lighter-signer").join(&file));
    paths
}

fn signer() -> anyhow::Result<&'static SignerLib> {
    static LIB: OnceLock<Result<SignerLib, String>> = OnceLock::new();
    let result = LIB.get_or_init(|| {
        crate::load_dotenv();
        let candidates = signer_lib_candidates();
        let mut last_err = String::new();
        for path in &candidates {
            if !path.exists() {
                last_err = format!("{} does not exist", path.display());
                continue;
            }
            match unsafe { libloading::Library::new(path) } {
                Ok(lib) => {
                    let lib: &'static libloading::Library = Box::leak(Box::new(lib));
                    unsafe {
                        return Ok(SignerLib {
                            generate_api_key: *lib
                                .get::<GenerateApiKeyFn>(b"GenerateAPIKey\0")
                                .map_err(|e| e.to_string())?,
                            create_client: *lib
                                .get::<CreateClientFn>(b"CreateClient\0")
                                .map_err(|e| e.to_string())?,
                            check_client: *lib
                                .get::<CheckClientFn>(b"CheckClient\0")
                                .map_err(|e| e.to_string())?,
                            sign_change_pub_key: *lib
                                .get::<SignChangePubKeyFn>(b"SignChangePubKey\0")
                                .map_err(|e| e.to_string())?,
                            sign_create_order: *lib
                                .get::<SignCreateOrderFn>(b"SignCreateOrder\0")
                                .map_err(|e| e.to_string())?,
                            sign_cancel_order: *lib
                                .get::<SignCancelOrderFn>(b"SignCancelOrder\0")
                                .map_err(|e| e.to_string())?,
                            sign_modify_order: *lib
                                .get::<SignModifyOrderFn>(b"SignModifyOrder\0")
                                .map_err(|e| e.to_string())?,
                            sign_cancel_all: *lib
                                .get::<SignCancelAllFn>(b"SignCancelAllOrders\0")
                                .map_err(|e| e.to_string())?,
                            sign_update_account_asset_config: *lib
                                .get::<SignUpdateAccountAssetConfigFn>(
                                    b"SignUpdateAccountAssetConfig\0",
                                )
                                .map_err(|e| e.to_string())?,
                            create_auth_token: *lib
                                .get::<CreateAuthTokenFn>(b"CreateAuthToken\0")
                                .map_err(|e| e.to_string())?,
                            free: *lib.get::<FreeFn>(b"Free\0").map_err(|e| e.to_string())?,
                            _lib: lib,
                        });
                    }
                }
                Err(err) => last_err = format!("{}: {err}", path.display()),
            }
        }
        Err(format!(
            "no usable lighter signer library (tried {:?}): {last_err}. \
             Set LIGHTER_SIGNER_LIB to a binary from \
             https://github.com/elliottech/lighter-python/tree/main/lighter/signers",
            candidates
        ))
    });
    result.as_ref().map_err(|err| anyhow::anyhow!("{err}"))
}

impl SignerLib {
    /// Read a signer-allocated C string and free it with the library's own
    /// allocator (mirrors the Python SDK's `decode_and_free`).
    fn take(&self, ptr: *mut c_char) -> Option<String> {
        if ptr.is_null() {
            return None;
        }
        let value = unsafe { CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned();
        unsafe { (self.free)(ptr as *mut c_void) };
        Some(value)
    }

    fn take_signed(&self, response: FfiSignedTx) -> anyhow::Result<SignedTx> {
        let err = self.take(response.err);
        let tx_info = self.take(response.tx_info);
        let tx_hash = self.take(response.tx_hash);
        let message_to_sign = self
            .take(response.message_to_sign)
            .filter(|m| !m.is_empty());
        if let Some(err) = err {
            bail!("lighter signer: {err}");
        }
        Ok(SignedTx {
            tx_type: response.tx_type,
            tx_info: tx_info.context("lighter signer returned no tx_info")?,
            tx_hash: tx_hash.unwrap_or_default(),
            message_to_sign,
        })
    }

    fn create_client(
        &self,
        url: &str,
        private_key: &str,
        chain_id: c_int,
        api_key_index: u8,
        account_index: i64,
    ) -> anyhow::Result<()> {
        let url = CString::new(url)?;
        let key = CString::new(private_key.trim_start_matches("0x"))?;
        let err = unsafe {
            (self.create_client)(
                url.as_ptr() as *mut c_char,
                key.as_ptr() as *mut c_char,
                chain_id,
                api_key_index as c_int,
                account_index,
            )
        };
        if let Some(err) = self.take(err) {
            bail!("lighter signer CreateClient: {err}");
        }
        Ok(())
    }

    fn check_client(&self, api_key_index: u8, account_index: i64) -> anyhow::Result<()> {
        let err = unsafe { (self.check_client)(api_key_index as c_int, account_index) };
        if let Some(err) = self.take(err) {
            bail!("lighter API key mismatch (api_key_index {api_key_index}): {err}");
        }
        Ok(())
    }

    fn auth_token(
        &self,
        deadline_unix: i64,
        api_key_index: u8,
        account_index: i64,
    ) -> anyhow::Result<String> {
        let result = unsafe {
            (self.create_auth_token)(deadline_unix, api_key_index as c_int, account_index)
        };
        let token = self.take(result.str_);
        if let Some(err) = self.take(result.err) {
            bail!("lighter signer CreateAuthToken: {err}");
        }
        token.context("lighter signer returned no auth token")
    }
}

/// Generate a fresh Lighter API key pair (hex private/public key).
/// Registration on an account still requires [`change_api_key`].
pub fn generate_api_key() -> anyhow::Result<(String, String)> {
    let lib = signer()?;
    let result = unsafe { (lib.generate_api_key)() };
    let private_key = lib.take(result.private_key);
    let public_key = lib.take(result.public_key);
    if let Some(err) = lib.take(result.err) {
        bail!("lighter signer GenerateAPIKey: {err}");
    }
    Ok((
        private_key.context("no private key returned")?,
        public_key.context("no public key returned")?,
    ))
}

// ---------------------------------------------------------------------------
// EIP-191 personal_sign (for ChangePubKey L1 co-signature)
// ---------------------------------------------------------------------------

fn keccak256(data: &[u8]) -> [u8; 32] {
    use sha3::{Digest, Keccak256};
    let mut hasher = Keccak256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// `personal_sign` (EIP-191) over `message`, returning the 65-byte
/// `0x{r}{s}{v}` signature Lighter expects in `L1Sig`.
fn eth_personal_sign(eth_private_key: &str, message: &str) -> anyhow::Result<String> {
    use k256::ecdsa::SigningKey;
    let key_bytes = hex::decode(eth_private_key.trim_start_matches("0x"))
        .context("eth private key is not valid hex")?;
    let signing_key = SigningKey::from_slice(&key_bytes)
        .context("eth private key is not a valid secp256k1 key")?;
    let prefixed = format!("\x19Ethereum Signed Message:\n{}{}", message.len(), message);
    let digest = keccak256(prefixed.as_bytes());
    let (signature, recovery_id) = signing_key
        .sign_prehash_recoverable(&digest)
        .context("eth signing failed")?;
    let mut out = [0u8; 65];
    out[..64].copy_from_slice(&signature.to_bytes());
    out[64] = 27 + recovery_id.to_byte();
    Ok(format!("0x{}", hex::encode(out)))
}

// ---------------------------------------------------------------------------
// REST plumbing
// ---------------------------------------------------------------------------

fn http() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .expect("reqwest client")
    })
}

fn base_url() -> String {
    crate::env_or("LIGHTER_BASE_URL", MAINNET_BASE_URL)
        .trim_end_matches('/')
        .to_owned()
}

fn chain_id_for(url: &str) -> c_int {
    if url.contains("testnet") {
        TESTNET_CHAIN_ID
    } else {
        MAINNET_CHAIN_ID
    }
}

#[derive(Debug, Deserialize)]
struct ResultCode {
    code: i64,
    #[serde(default)]
    message: Option<String>,
}

async fn get_json<T: serde::de::DeserializeOwned>(
    url: &str,
    query: &[(&str, String)],
    auth: Option<&str>,
) -> anyhow::Result<T> {
    let mut request = http().get(url).query(query);
    if let Some(token) = auth {
        request = request.header("authorization", token);
    }
    let response = request.send().await.with_context(|| format!("GET {url}"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .with_context(|| format!("GET {url}: body"))?;
    if let Ok(result) = serde_json::from_str::<ResultCode>(&body) {
        if result.code != 200 {
            bail!(
                "lighter GET {url}: code {} {}",
                result.code,
                result.message.unwrap_or_default()
            );
        }
    } else if !status.is_success() {
        if body.contains("Human Verification") {
            bail!("lighter GET {url}: CloudFront Human Verification (HTTP {status})");
        }
        bail!(
            "lighter GET {url}: HTTP {status}: {}",
            body.chars().take(240).collect::<String>()
        );
    }
    serde_json::from_str(&body).with_context(|| format!("GET {url}: decoding {body}"))
}

#[derive(Debug, Deserialize)]
struct RespSendTx {
    code: i64,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    tx_hash: Option<String>,
}

/// Push a signed transaction through `POST /api/v1/sendTx`.
pub async fn send_tx(base: &str, tx: &SignedTx) -> anyhow::Result<String> {
    let url = format!("{base}/api/v1/sendTx");
    let form = [
        ("tx_type", tx.tx_type.to_string()),
        ("tx_info", tx.tx_info.clone()),
    ];
    let response = http()
        .post(&url)
        .form(&form)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    let body = response.text().await.context("sendTx: body")?;
    let parsed: RespSendTx =
        serde_json::from_str(&body).with_context(|| format!("sendTx: decoding {body}"))?;
    if parsed.code != 200 {
        bail!(
            "lighter sendTx: code {} {}",
            parsed.code,
            parsed.message.unwrap_or_default()
        );
    }
    parsed.tx_hash.context("sendTx: response had no tx_hash")
}

// ---------------------------------------------------------------------------
// Market metadata
// ---------------------------------------------------------------------------

/// Static per-market constraints from `orderBookDetails`.
#[derive(Clone, Debug)]
pub struct MarketInfo {
    pub symbol: String,
    pub market_id: u16,
    /// Decimals used to express sizes as integers on the wire.
    pub size_decimals: u32,
    /// Decimals used to express prices as integers on the wire.
    pub price_decimals: u32,
    /// Minimum maker order size in base units.
    pub min_base_amount: f64,
    /// Minimum maker order value in USDC.
    pub min_quote_amount: f64,
    pub taker_fee: f64,
    pub maker_fee: f64,
    pub last_trade_price: f64,
}

impl MarketInfo {
    /// Price step implied by the price decimals.
    pub fn tick_size(&self) -> f64 {
        10f64.powi(-(self.price_decimals as i32))
    }

    /// Size step implied by the size decimals.
    pub fn size_step(&self) -> f64 {
        10f64.powi(-(self.size_decimals as i32))
    }

    /// Smallest maker-eligible size at `price` (max of base/quote minimums,
    /// not yet rounded onto the size grid).
    pub fn min_qty(&self, price: f64) -> f64 {
        self.min_base_amount.max(self.min_quote_amount / price)
    }
}

#[derive(Debug, Deserialize)]
struct OrderBookDetailsResponse {
    // Either list is null when the filter excludes it.
    order_book_details: Option<Vec<OrderBookDetail>>,
    spot_order_book_details: Option<Vec<OrderBookDetail>>,
}

#[derive(Clone, Debug, Deserialize)]
struct OrderBookDetail {
    symbol: String,
    market_id: u16,
    #[serde(default)]
    status: String,
    taker_fee: String,
    maker_fee: String,
    min_base_amount: String,
    min_quote_amount: String,
    supported_size_decimals: u32,
    supported_price_decimals: u32,
    #[serde(default)]
    last_trade_price: f64,
}

impl OrderBookDetail {
    fn into_market_info(self) -> anyhow::Result<MarketInfo> {
        Ok(MarketInfo {
            symbol: self.symbol,
            market_id: self.market_id,
            size_decimals: self.supported_size_decimals,
            price_decimals: self.supported_price_decimals,
            min_base_amount: self.min_base_amount.parse().unwrap_or(0.0),
            min_quote_amount: self.min_quote_amount.parse().unwrap_or(0.0),
            taker_fee: self.taker_fee.parse().unwrap_or(0.0),
            maker_fee: self.maker_fee.parse().unwrap_or(0.0),
            last_trade_price: self.last_trade_price,
        })
    }
}

/// Credential-free market metadata lookup by symbol, case-insensitive.
/// Perp markets are plain symbols (`"ETH"`, `"BTC"`); spot markets carry the
/// quote (`"LIT/USDC"`, `"ETH/USDC"`).
pub async fn fetch_market(symbol: &str) -> anyhow::Result<MarketInfo> {
    crate::load_dotenv();
    fetch_market_from(&base_url(), symbol).await
}

async fn fetch_market_from(base: &str, symbol: &str) -> anyhow::Result<MarketInfo> {
    let wanted = symbol.trim().to_uppercase();
    let spot = wanted.contains('/');
    // One orderBookDetails response covers every market. Share it across
    // capsules and book streams instead of requesting it for each symbol.
    enum Entry {
        Ready(Instant, Vec<OrderBookDetail>),
        Blocked(Instant),
    }
    static CACHE: OnceLock<tokio::sync::Mutex<HashMap<(String, bool), Entry>>> = OnceLock::new();
    let mut cache = CACHE
        .get_or_init(|| tokio::sync::Mutex::new(HashMap::new()))
        .lock()
        .await;
    let key = (base.to_owned(), spot);
    match cache.get(&key) {
        Some(Entry::Ready(at, _)) if at.elapsed() < Duration::from_secs(600) => {}
        Some(Entry::Blocked(at)) if at.elapsed() < Duration::from_secs(60) => {
            bail!("lighter market metadata temporarily blocked by venue; retry later");
        }
        _ => {
            let response: OrderBookDetailsResponse = match get_json(
                &format!("{base}/api/v1/orderBookDetails"),
                &[("filter", if spot { "spot" } else { "perp" }.to_owned())],
                None,
            )
            .await
            {
                Ok(response) => response,
                Err(err) => {
                    let message = format!("{err:#}");
                    if message.contains("405") || message.contains("429") {
                        cache.insert(key, Entry::Blocked(Instant::now()));
                    }
                    return Err(err);
                }
            };
            let details = if spot {
                response.spot_order_book_details
            } else {
                response.order_book_details
            };
            cache.insert(
                key.clone(),
                Entry::Ready(Instant::now(), details.unwrap_or_default()),
            );
        }
    }
    let Some(Entry::Ready(_, details)) = cache.get(&key) else {
        unreachable!()
    };
    let detail = details
        .iter()
        .find(|detail| detail.symbol.to_uppercase() == wanted && detail.status != "inactive")
        .cloned()
        .with_context(|| format!("lighter has no active market {wanted}"))?;
    detail.into_market_info()
}

// ---------------------------------------------------------------------------
// Wire scaling helpers
// ---------------------------------------------------------------------------

fn scale_from_int(value: i64, decimals: u32) -> f64 {
    value as f64 / 10f64.powi(decimals as i32)
}

/// Round a price onto the market's price grid, conservatively for the order
/// side: bids down, asks up (same convention as the Hyperliquid capsule).
fn price_to_wire(info: &MarketInfo, side: Side, price: f64) -> anyhow::Result<i64> {
    if !price.is_finite() || price <= 0.0 {
        bail!("invalid price {price}");
    }
    let scaled = price * 10f64.powi(info.price_decimals as i32);
    // Epsilon so values that are exactly on the grid (up to f64 noise from a
    // previous wire->f64 roundtrip) do not jump a tick.
    let eps = scaled.abs() * 1e-9 + 1e-9;
    let ticks = match side {
        Side::Buy => (scaled + eps).floor(),
        Side::Sell => (scaled - eps).ceil(),
    };
    if ticks < 1.0 || ticks > u32::MAX as f64 {
        bail!("price {price} out of range for {}", info.symbol);
    }
    Ok(ticks as i64)
}

/// Truncate a size onto the market's size grid.
fn size_to_wire(info: &MarketInfo, qty: f64) -> anyhow::Result<i64> {
    if !qty.is_finite() || qty <= 0.0 {
        bail!("invalid size {qty}");
    }
    let exact = qty * 10f64.powi(info.size_decimals as i32);
    // Same grid-point epsilon as prices: 0.0048 must scale to 48, not 47.
    let scaled = (exact + exact.abs() * 1e-9 + 1e-9).floor() as i64;
    if scaled <= 0 {
        bail!("size {qty} is below one size step on {}", info.symbol);
    }
    Ok(scaled)
}

/// Process-unique client order index (u48: millis << 6 | counter).
fn next_client_order_index() -> u64 {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let count = COUNTER.fetch_add(1, Ordering::Relaxed) as u64 % 64;
    ((millis << 6) | count) & MAX_CLIENT_ORDER_INDEX
}

// ---------------------------------------------------------------------------
// Account / order REST payloads
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct AccountsByL1Response {
    sub_accounts: Vec<SubAccount>,
}

#[derive(Debug, Deserialize)]
struct SubAccount {
    index: i64,
    #[serde(default)]
    account_type: i64,
}

/// Resolve the master account index for an L1 address.
pub async fn account_index_for(l1_address: &str) -> anyhow::Result<i64> {
    crate::load_dotenv();
    let base = base_url();
    let response: AccountsByL1Response = get_json(
        &format!("{base}/api/v1/accountsByL1Address"),
        &[("l1_address", l1_address.to_owned())],
        None,
    )
    .await
    .with_context(|| format!("no lighter account for {l1_address}"))?;
    response
        .sub_accounts
        .iter()
        .find(|acct| acct.account_type == 0)
        .or(response.sub_accounts.first())
        .map(|acct| acct.index)
        .with_context(|| format!("no lighter account for {l1_address}"))
}

#[derive(Debug, Deserialize)]
struct NextNonceResponse {
    nonce: i64,
}

#[derive(Debug, Deserialize)]
struct OrdersResponse {
    orders: Vec<WireOrder>,
}

#[derive(Debug, Deserialize)]
struct WireOrder {
    client_order_index: u64,
    market_index: u16,
    price: String,
    remaining_base_amount: String,
    filled_base_amount: String,
    #[serde(default)]
    filled_quote_amount: String,
    is_ask: bool,
    status: String,
}

/// Executed amounts of one order (from the order record, not per-trade).
#[derive(Clone, Copy, Debug)]
pub struct OrderFill {
    pub side: Side,
    /// Filled base quantity.
    pub qty: f64,
    /// Filled quote amount (USDC).
    pub quote: f64,
}

fn order_state(status: &str) -> OrderState {
    match status {
        "open" | "pending" | "in-progress" => OrderState::Open,
        "filled" => OrderState::Filled,
        _ if status.starts_with("canceled") => OrderState::Canceled,
        _ => OrderState::Rejected,
    }
}

impl WireOrder {
    fn fill(&self) -> OrderFill {
        OrderFill {
            side: if self.is_ask { Side::Sell } else { Side::Buy },
            qty: self.filled_base_amount.parse().unwrap_or(0.0),
            quote: self.filled_quote_amount.parse().unwrap_or(0.0),
        }
    }

    fn into_order_info(self) -> OrderInfo {
        OrderInfo {
            order_id: self.client_order_index,
            state: order_state(&self.status),
            side: if self.is_ask { Side::Sell } else { Side::Buy },
            limit_px: self.price.parse().unwrap_or(0.0),
            filled_qty: self.filled_base_amount.parse().unwrap_or(0.0),
            remaining_qty: self.remaining_base_amount.parse().unwrap_or(0.0),
        }
    }
}

#[derive(Debug, Deserialize)]
struct DetailedAccountsResponse {
    accounts: Vec<DetailedAccount>,
}

#[derive(Debug, Deserialize)]
struct DetailedAccount {
    collateral: String,
    #[serde(default)]
    available_balance: String,
    #[serde(default)]
    total_asset_value: String,
    #[serde(default)]
    positions: Vec<WirePosition>,
}

#[derive(Debug, Deserialize)]
struct WirePosition {
    market_id: u16,
    sign: i32,
    position: String,
    avg_entry_price: String,
    unrealized_pnl: String,
    #[serde(default)]
    liquidation_price: String,
}

/// One market's position state.
#[derive(Clone, Debug, Default)]
pub struct PositionSnapshot {
    /// Signed base size (>0 long, <0 short, 0 flat).
    pub position: f64,
    pub avg_entry_price: f64,
    pub unrealized_pnl: f64,
    pub liquidation_price: Option<f64>,
}

/// Account-wide balances (USDC terms).
#[derive(Clone, Debug)]
pub struct AccountSummary {
    pub collateral: f64,
    pub available_balance: f64,
    pub total_asset_value: f64,
}

#[derive(Debug, Deserialize)]
struct OrderBookOrdersResponse {
    bids: Vec<WireBookOrder>,
    asks: Vec<WireBookOrder>,
}

#[derive(Debug, Deserialize)]
struct WireBookOrder {
    price: String,
    remaining_base_amount: String,
}

fn book_side(levels: Vec<WireBookOrder>) -> Vec<(f64, f64)> {
    use std::collections::BTreeMap;
    // orderBookOrders lists individual orders; aggregate per price level.
    let mut side: BTreeMap<i64, f64> = BTreeMap::new();
    for level in levels {
        let (Ok(price), Ok(qty)) = (
            level.price.parse::<f64>(),
            level.remaining_base_amount.parse::<f64>(),
        ) else {
            continue;
        };
        *side.entry(crate::book::price_key(price)).or_default() += qty;
    }
    side.into_iter()
        .map(|(key, qty)| (crate::book::key_to_price(key), qty))
        .collect()
}

// ---------------------------------------------------------------------------
// The capsule
// ---------------------------------------------------------------------------

struct AuthCache {
    token: String,
    expires_unix: i64,
}

/// Lighter perp venue capsule for a single market. All methods take `&self`;
/// wrap in `Arc` to share across tasks.
pub struct Lighter {
    base: String,
    account_index: i64,
    api_key_index: u8,
    trading: bool,
    /// Next nonce for this API key (trading mode; -1 when unseeded).
    nonce: AtomicI64,
    auth: Mutex<Option<AuthCache>>,
    market: MarketInfo,
}

impl Lighter {
    /// Read-only mode: markets, books, positions, account summary. Needs a
    /// resolvable account (`LIGHTER_ACCOUNT_INDEX` or `WALLET_ADDRESS`).
    pub async fn connect(symbol: &str) -> anyhow::Result<Self> {
        Self::build(symbol, false).await
    }

    /// Trading mode: everything in [`connect`](Self::connect) plus signed
    /// actions and auth-gated queries. Needs `LIGHTER_PRIVATE_KEY` (and
    /// `LIGHTER_API_KEY_INDEX` if not 2).
    pub async fn connect_trading(symbol: &str) -> anyhow::Result<Self> {
        Self::build(symbol, true).await
    }

    async fn build(symbol: &str, trading: bool) -> anyhow::Result<Self> {
        crate::load_dotenv();
        let base = base_url();
        let account_index = match std::env::var("LIGHTER_ACCOUNT_INDEX") {
            Ok(value) if !value.trim().is_empty() => value
                .trim()
                .parse()
                .context("LIGHTER_ACCOUNT_INDEX is not an integer")?,
            _ => {
                let wallet = std::env::var("WALLET_ADDRESS")
                    .context("LIGHTER_ACCOUNT_INDEX or WALLET_ADDRESS is required")?;
                account_index_for(&wallet).await?
            }
        };
        let api_key_index: u8 = crate::env_or("LIGHTER_API_KEY_INDEX", "2")
            .parse()
            .context("LIGHTER_API_KEY_INDEX is not a u8")?;
        let market = fetch_market(symbol).await?;

        let capsule = Self {
            base,
            account_index,
            api_key_index,
            trading,
            nonce: AtomicI64::new(-1),
            auth: Mutex::new(None),
            market,
        };
        if trading {
            let key = std::env::var("LIGHTER_PRIVATE_KEY")
                .context("LIGHTER_PRIVATE_KEY is required for trading mode")?;
            let lib = signer()?;
            lib.create_client(
                &capsule.base,
                &key,
                chain_id_for(&capsule.base),
                api_key_index,
                account_index,
            )?;
            lib.check_client(api_key_index, account_index)
                .context("registered API key does not match LIGHTER_PRIVATE_KEY")?;
            capsule.refresh_nonce().await?;
        }
        Ok(capsule)
    }

    pub fn symbol(&self) -> &str {
        &self.market.symbol
    }

    pub fn account_index(&self) -> i64 {
        self.account_index
    }

    /// This market's trading constraints (cached from connect time).
    pub fn market_info(&self) -> MarketInfo {
        self.market.clone()
    }

    fn require_trading(&self) -> anyhow::Result<()> {
        if !self.trading {
            bail!(
                "Lighter {} is read-only: connect with connect_trading (LIGHTER_PRIVATE_KEY)",
                self.market.symbol
            );
        }
        Ok(())
    }

    async fn refresh_nonce(&self) -> anyhow::Result<()> {
        let response: NextNonceResponse = get_json(
            &format!("{}/api/v1/nextNonce", self.base),
            &[
                ("account_index", self.account_index.to_string()),
                ("api_key_index", self.api_key_index.to_string()),
            ],
            None,
        )
        .await?;
        self.nonce.store(response.nonce, Ordering::SeqCst);
        Ok(())
    }

    fn take_nonce(&self) -> anyhow::Result<i64> {
        let nonce = self.nonce.fetch_add(1, Ordering::SeqCst);
        if nonce < 0 {
            bail!("lighter nonce not seeded (read-only capsule?)");
        }
        Ok(nonce)
    }

    /// 10-minute auth token for auth-gated REST/WS queries (cached, renewed
    /// with >60s slack). Trading mode only.
    pub fn auth_token(&self) -> anyhow::Result<String> {
        self.require_trading()?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let mut cache = self.auth.lock().expect("auth cache lock");
        if let Some(cached) = cache.as_ref() {
            if cached.expires_unix - now > 60 {
                return Ok(cached.token.clone());
            }
        }
        let expires_unix = now + 600;
        let token = signer()?.auth_token(expires_unix, self.api_key_index, self.account_index)?;
        *cache = Some(AuthCache {
            token: token.clone(),
            expires_unix,
        });
        Ok(token)
    }

    // -- market data --------------------------------------------------------

    /// REST book snapshot (top 100 orders per side, aggregated by level).
    pub async fn book(&self) -> anyhow::Result<OrderBook> {
        let response: OrderBookOrdersResponse = get_json(
            &format!("{}/api/v1/orderBookOrders", self.base),
            &[
                ("market_id", self.market.market_id.to_string()),
                ("limit", "100".to_owned()),
            ],
            None,
        )
        .await?;
        Ok(OrderBook::from_sides(
            book_side(response.bids),
            book_side(response.asks),
        ))
    }

    /// Mid of the current top of book.
    pub async fn mid_price(&self) -> anyhow::Result<f64> {
        self.book()
            .await?
            .mid()
            .with_context(|| format!("lighter {} book is one-sided or empty", self.market.symbol))
    }

    // -- trading ------------------------------------------------------------

    async fn submit_order(
        &self,
        side: Side,
        qty: f64,
        price: f64,
        reduce_only: bool,
        order_type: c_int,
        time_in_force: c_int,
        order_expiry: c_longlong,
    ) -> anyhow::Result<ActiveOrder> {
        self.require_trading()?;
        let base_amount = size_to_wire(&self.market, qty)?;
        let price_wire = price_to_wire(&self.market, side, price)?;
        if !reduce_only {
            let notional = scale_from_int(base_amount, self.market.size_decimals)
                * scale_from_int(price_wire, self.market.price_decimals);
            let min_quote = self.market.min_quote_amount;
            let min_base = self.market.min_base_amount;
            if order_type == ORDER_TYPE_LIMIT
                && (notional < min_quote
                    || scale_from_int(base_amount, self.market.size_decimals) < min_base)
            {
                bail!(
                    "order below lighter {} minimum (>= {min_base} base and >= {min_quote} USDC)",
                    self.market.symbol
                );
            }
        }
        let client_order_index = next_client_order_index();
        let nonce = self.take_nonce()?;
        let lib = signer()?;
        let signed = lib.take_signed(unsafe {
            (lib.sign_create_order)(
                self.market.market_id as c_int,
                client_order_index as c_longlong,
                base_amount,
                price_wire as c_int,
                if side.is_buy() { 0 } else { 1 },
                order_type,
                time_in_force,
                if reduce_only { 1 } else { 0 },
                NIL_TRIGGER_PRICE,
                order_expiry,
                0, // integrator account index
                0, // integrator taker fee
                0, // integrator maker fee
                0, // self trade behavior mode
                0, // self trade equality mode
                0, // skip nonce
                nonce,
                self.api_key_index as c_int,
                self.account_index,
            )
        });
        let signed = match signed {
            Ok(signed) => signed,
            Err(err) => {
                // Signing never reached the server: the nonce was not consumed.
                let _ = self.refresh_nonce().await;
                return Err(err);
            }
        };
        if let Err(err) = send_tx(&self.base, &signed).await {
            // API-level rejections do not consume the nonce; resync.
            let _ = self.refresh_nonce().await;
            return Err(err);
        }
        Ok(ActiveOrder {
            order_id: client_order_index,
            side,
            qty: scale_from_int(base_amount, self.market.size_decimals),
        })
    }

    /// Place a good-till-time (28 day) limit order. Price is rounded onto the
    /// market price grid (bids down, asks up), size truncated onto the size
    /// grid. The returned [`ActiveOrder::order_id`] is the client order index.
    pub async fn place_limit(
        &self,
        side: Side,
        qty: f64,
        price: f64,
        reduce_only: bool,
    ) -> anyhow::Result<ActiveOrder> {
        self.submit_order(
            side,
            qty,
            price,
            reduce_only,
            ORDER_TYPE_LIMIT,
            TIF_GOOD_TILL_TIME,
            DEFAULT_28_DAY_ORDER_EXPIRY,
        )
        .await
    }

    /// Place a post-only limit order (canceled by the venue if it would cross).
    pub async fn place_post_only(
        &self,
        side: Side,
        qty: f64,
        price: f64,
        reduce_only: bool,
    ) -> anyhow::Result<ActiveOrder> {
        self.submit_order(
            side,
            qty,
            price,
            reduce_only,
            ORDER_TYPE_LIMIT,
            TIF_POST_ONLY,
            DEFAULT_28_DAY_ORDER_EXPIRY,
        )
        .await
    }

    /// Synthetic market order: IOC crossing the opposite top of book with 5%
    /// worst-price slack.
    pub async fn place_market(
        &self,
        side: Side,
        qty: f64,
        reduce_only: bool,
    ) -> anyhow::Result<ActiveOrder> {
        let book = self.book().await?;
        let top = match side {
            Side::Buy => book.best_ask(),
            Side::Sell => book.best_bid(),
        }
        .with_context(|| format!("lighter {}: no opposite side to cross", self.market.symbol))?;
        self.place_market_at(side, qty, reduce_only, top).await
    }

    /// [`place_market`](Self::place_market) with a caller-provided opposite
    /// top-of-book price (saves the REST book fetch on hot paths).
    pub async fn place_market_at(
        &self,
        side: Side,
        qty: f64,
        reduce_only: bool,
        opposite_top: f64,
    ) -> anyhow::Result<ActiveOrder> {
        let worst = match side {
            Side::Buy => opposite_top * (1.0 + MARKET_SLIPPAGE),
            Side::Sell => opposite_top * (1.0 - MARKET_SLIPPAGE),
        };
        self.submit_order(
            side,
            qty,
            worst,
            reduce_only,
            ORDER_TYPE_MARKET,
            TIF_IOC,
            DEFAULT_IOC_EXPIRY,
        )
        .await
    }

    /// Cancel by client order index. Returns the cancel transaction hash;
    /// verify with [`fetch_order`](Self::fetch_order) — the venue accepts the
    /// transaction even when the order is already gone.
    pub async fn cancel_order(&self, order_id: u64) -> anyhow::Result<String> {
        self.require_trading()?;
        let nonce = self.take_nonce()?;
        let lib = signer()?;
        let signed = lib.take_signed(unsafe {
            (lib.sign_cancel_order)(
                self.market.market_id as c_int,
                order_id as c_longlong,
                0, // skip nonce
                nonce,
                self.api_key_index as c_int,
                self.account_index,
            )
        });
        let signed = match signed {
            Ok(signed) => signed,
            Err(err) => {
                let _ = self.refresh_nonce().await;
                return Err(err);
            }
        };
        match send_tx(&self.base, &signed).await {
            Ok(tx_hash) => Ok(tx_hash),
            Err(err) => {
                let _ = self.refresh_nonce().await;
                Err(err)
            }
        }
    }

    /// Modify an order's price (and size) in place, keyed by client order
    /// index. Returns the same [`ActiveOrder`] with the size the venue now
    /// has.
    pub async fn modify_order(
        &self,
        order: &ActiveOrder,
        price: f64,
        qty: Option<f64>,
    ) -> anyhow::Result<ActiveOrder> {
        self.require_trading()?;
        let qty = qty.unwrap_or(order.qty);
        let base_amount = size_to_wire(&self.market, qty)?;
        let price_wire = price_to_wire(&self.market, order.side, price)?;
        let nonce = self.take_nonce()?;
        let lib = signer()?;
        let signed = lib.take_signed(unsafe {
            (lib.sign_modify_order)(
                self.market.market_id as c_int,
                order.order_id as c_longlong,
                base_amount,
                price_wire,
                NIL_TRIGGER_PRICE as c_longlong,
                0, // integrator account index
                0, // integrator taker fee
                0, // integrator maker fee
                0, // self trade behavior mode
                0, // self trade equality mode
                0, // skip nonce
                nonce,
                self.api_key_index as c_int,
                self.account_index,
            )
        });
        let signed = match signed {
            Ok(signed) => signed,
            Err(err) => {
                let _ = self.refresh_nonce().await;
                return Err(err);
            }
        };
        if let Err(err) = send_tx(&self.base, &signed).await {
            let _ = self.refresh_nonce().await;
            return Err(err);
        }
        Ok(ActiveOrder {
            order_id: order.order_id,
            side: order.side,
            qty: scale_from_int(base_amount, self.market.size_decimals),
        })
    }

    /// All active orders on this market (auth-gated).
    pub async fn open_orders(&self) -> anyhow::Result<Vec<OrderInfo>> {
        let auth = self.auth_token()?;
        let response: OrdersResponse = get_json(
            &format!("{}/api/v1/accountActiveOrders", self.base),
            &[
                ("account_index", self.account_index.to_string()),
                ("market_id", self.market.market_id.to_string()),
            ],
            Some(&auth),
        )
        .await?;
        Ok(response
            .orders
            .into_iter()
            .filter(|order| order.market_index == self.market.market_id)
            .map(WireOrder::into_order_info)
            .collect())
    }

    /// Active orders across every market for this account (one REST call).
    pub async fn all_open_orders(&self) -> anyhow::Result<Vec<(u16, OrderInfo)>> {
        let auth = self.auth_token()?;
        let response: OrdersResponse = get_json(
            &format!("{}/api/v1/accountActiveOrders", self.base),
            &[("account_index", self.account_index.to_string())],
            Some(&auth),
        )
        .await?;
        Ok(response
            .orders
            .into_iter()
            .map(|order| (order.market_index, order.into_order_info()))
            .collect())
    }

    /// Look one order up by client order index: active orders first, then the
    /// recent inactive orders (filled/canceled land there). `None` when the
    /// venue does not know the order (yet) — sequencing is asynchronous, so
    /// poll after placing.
    pub async fn fetch_order(&self, order_id: u64) -> anyhow::Result<Option<OrderInfo>> {
        Ok(self
            .fetch_wire_order(order_id)
            .await?
            .map(WireOrder::into_order_info))
    }

    /// Executed base/quote amounts of one order (`None` when the venue
    /// does not know it). The quote amount is the venue's own sum over the
    /// fills, i.e. the actual traded notional.
    pub async fn fetch_order_fill(&self, order_id: u64) -> anyhow::Result<Option<OrderFill>> {
        Ok(self
            .fetch_wire_order(order_id)
            .await?
            .map(|order| order.fill()))
    }

    async fn fetch_wire_order(&self, order_id: u64) -> anyhow::Result<Option<WireOrder>> {
        let auth = self.auth_token()?;
        for path in [
            "/api/v1/accountActiveOrders",
            "/api/v1/accountInactiveOrders",
        ] {
            let mut params = vec![
                ("account_index", self.account_index.to_string()),
                ("market_id", self.market.market_id.to_string()),
            ];
            if path.ends_with("InactiveOrders") {
                params.push(("limit", "100".to_owned()));
            }
            let response: OrdersResponse =
                get_json(&format!("{}{path}", self.base), &params, Some(&auth)).await?;
            let found = response
                .orders
                .into_iter()
                .filter(|order| order.market_index == self.market.market_id)
                .find(|order| order.client_order_index == order_id);
            if found.is_some() {
                return Ok(found);
            }
        }
        Ok(None)
    }

    /// Enable/disable a spot asset (by asset id, e.g. LIT = 2) as unified
    /// perp margin collateral (`UpdateAccountAssetConfig`). Returns the tx
    /// hash; the sequencer applies it asynchronously.
    pub async fn set_asset_margin(&self, asset_id: i16, enabled: bool) -> anyhow::Result<String> {
        self.require_trading()?;
        let nonce = self.take_nonce()?;
        let lib = signer()?;
        let signed = lib.take_signed(unsafe {
            (lib.sign_update_account_asset_config)(
                asset_id,
                if enabled { 1 } else { 0 },
                0, // skip nonce
                nonce,
                self.api_key_index as c_int,
                self.account_index,
            )
        });
        let signed = match signed {
            Ok(signed) => signed,
            Err(err) => {
                let _ = self.refresh_nonce().await;
                return Err(err);
            }
        };
        match send_tx(&self.base, &signed).await {
            Ok(tx_hash) => Ok(tx_hash),
            Err(err) => {
                let _ = self.refresh_nonce().await;
                Err(err)
            }
        }
    }

    // -- account ------------------------------------------------------------

    async fn detailed_account(&self) -> anyhow::Result<DetailedAccount> {
        let response: DetailedAccountsResponse = get_json(
            &format!("{}/api/v1/account", self.base),
            &[
                ("by", "index".to_owned()),
                ("value", self.account_index.to_string()),
            ],
            None,
        )
        .await?;
        response
            .accounts
            .into_iter()
            .next()
            .context("lighter: account not found")
    }

    /// Position state on this capsule's market.
    pub async fn position_snapshot(&self) -> anyhow::Result<PositionSnapshot> {
        let account = self.detailed_account().await?;
        let Some(position) = account
            .positions
            .into_iter()
            .find(|position| position.market_id == self.market.market_id)
        else {
            return Ok(PositionSnapshot::default());
        };
        let size: f64 = position.position.parse().unwrap_or(0.0);
        Ok(PositionSnapshot {
            position: size * position.sign as f64,
            avg_entry_price: position.avg_entry_price.parse().unwrap_or(0.0),
            unrealized_pnl: position.unrealized_pnl.parse().unwrap_or(0.0),
            liquidation_price: position
                .liquidation_price
                .parse()
                .ok()
                .filter(|px| *px > 0.0),
        })
    }

    /// Signed position size (>0 long, <0 short, 0 flat).
    pub async fn position_amt(&self) -> anyhow::Result<f64> {
        Ok(self.position_snapshot().await?.position)
    }

    /// Account-wide USDC balances.
    pub async fn account_summary(&self) -> anyhow::Result<AccountSummary> {
        let account = self.detailed_account().await?;
        Ok(AccountSummary {
            collateral: account.collateral.parse().unwrap_or(0.0),
            available_balance: account.available_balance.parse().unwrap_or(0.0),
            total_asset_value: account.total_asset_value.parse().unwrap_or(0.0),
        })
    }
}

// ---------------------------------------------------------------------------
// WS book stream
// ---------------------------------------------------------------------------

fn ws_url(base: &str) -> String {
    let stripped = base
        .trim_end_matches('/')
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    format!("wss://{stripped}/stream")
}

#[derive(Debug, Deserialize)]
struct WsBookMessage {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    channel: String,
    #[serde(default)]
    order_book: Option<WsBookPayload>,
}

#[derive(Debug, Deserialize)]
struct WsBookPayload {
    #[serde(default)]
    bids: Vec<WsBookLevel>,
    #[serde(default)]
    asks: Vec<WsBookLevel>,
    #[serde(default)]
    nonce: u64,
    #[serde(default)]
    begin_nonce: u64,
}

#[derive(Debug, Deserialize)]
struct WsBookLevel {
    price: String,
    size: String,
}

fn ws_levels(levels: Vec<WsBookLevel>) -> Vec<(f64, f64)> {
    levels
        .into_iter()
        .filter_map(|level| Some((level.price.parse().ok()?, level.size.parse().ok()?)))
        .collect()
}

/// Stream many Lighter order books through one WebSocket connection. The
/// watched symbol list is shared by all application subscribers.
pub async fn stream_books(
    mut wanted: tokio::sync::watch::Receiver<Vec<String>>,
    tx: mpsc::Sender<(String, BookUpdate)>,
) -> anyhow::Result<()> {
    crate::load_dotenv();
    let url = ws_url(&base_url());
    let mut retry = Duration::from_secs(1);
    'reconnect: loop {
        // A batch of newly enabled symbols should produce one handshake.
        loop {
            match tokio::time::timeout(Duration::from_millis(750), wanted.changed()).await {
                Ok(Ok(())) => continue,
                Ok(Err(_)) => return Ok(()),
                Err(_) => break,
            }
        }
        let symbols = wanted.borrow_and_update().clone();
        if symbols.is_empty() {
            if wanted.changed().await.is_err() {
                return Ok(());
            }
            continue;
        }
        let mut markets = HashMap::new();
        let mut ids = Vec::with_capacity(symbols.len());
        for symbol in &symbols {
            let info = fetch_market(symbol).await?;
            ids.push(info.market_id);
            markets.insert(
                info.market_id,
                (symbol.clone(), OrderBook::default(), None::<u64>),
            );
        }
        let (mut socket, _) = match connect_async(&url).await {
            Ok(ok) => ok,
            Err(err) => {
                let waf = matches!(&err, tokio_tungstenite::tungstenite::Error::Http(response)
                    if response.headers().contains_key("x-amzn-waf-action"));
                let delay = if waf { Duration::from_secs(300) } else { retry };
                let notice = if waf {
                    "lighter ws blocked by CloudFront CAPTCHA (HTTP 405); retrying in 300s"
                        .to_owned()
                } else {
                    format!("lighter ws connect failed: {err}")
                };
                if tx
                    .send((String::new(), BookUpdate::Notice(notice)))
                    .await
                    .is_err()
                {
                    return Ok(());
                }
                tokio::time::sleep(delay).await;
                retry = (retry * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        retry = Duration::from_secs(1);
        let mut next_id = 0;
        let mut subscribe_tick = tokio::time::interval(Duration::from_millis(100));
        subscribe_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut ping = tokio::time::interval(Duration::from_secs(50));
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                changed = wanted.changed() => {
                    if changed.is_err() { return Ok(()); }
                    continue 'reconnect;
                }
                _ = subscribe_tick.tick(), if next_id < ids.len() => {
                    let request = serde_json::json!({"type":"subscribe", "channel":format!("order_book/{}", ids[next_id])});
                    if socket.send(Message::Text(request.to_string().into())).await.is_err() {
                        continue 'reconnect;
                    }
                    next_id += 1;
                }
                _ = ping.tick() => {
                    if socket.send(Message::Text(r#"{"type":"ping"}"#.into())).await.is_err() {
                        continue 'reconnect;
                    }
                }
                incoming = socket.next() => {
                    let Some(Ok(incoming)) = incoming else { continue 'reconnect; };
                    let text = match incoming {
                        Message::Text(text) => text,
                        Message::Ping(payload) => {
                            if socket.send(Message::Pong(payload)).await.is_err() { continue 'reconnect; }
                            continue;
                        }
                        Message::Close(_) => continue 'reconnect,
                        _ => continue,
                    };
                    let Ok(message) = serde_json::from_str::<WsBookMessage>(&text) else { continue; };
                    if message.kind == "ping" {
                        if socket.send(Message::Text(r#"{"type":"pong"}"#.into())).await.is_err() { continue 'reconnect; }
                        continue;
                    }
                    let Some(id) = message.channel.rsplit(':').next().and_then(|part| part.parse::<u16>().ok()) else { continue; };
                    let Some((symbol, book, last_nonce)) = markets.get_mut(&id) else { continue; };
                    let Some(payload) = message.order_book else { continue; };
                    match message.kind.as_str() {
                        "subscribed/order_book" => {
                            book.load_levels(0, ws_levels(payload.bids), ws_levels(payload.asks), true);
                            *last_nonce = Some(payload.nonce);
                        }
                        "update/order_book" => {
                            let Some(previous) = *last_nonce else { continue; };
                            if payload.begin_nonce != previous {
                                if tx.send((symbol.clone(), BookUpdate::Notice(format!(
                                    "lighter book gap (begin_nonce {} != last {previous}); reconnecting", payload.begin_nonce
                                )))).await.is_err() { return Ok(()); }
                                continue 'reconnect;
                            }
                            *last_nonce = Some(payload.nonce);
                            let (mut bids, mut asks) = book.levels(usize::MAX);
                            apply_ws_side(&mut bids, ws_levels(payload.bids));
                            apply_ws_side(&mut asks, ws_levels(payload.asks));
                            book.load_levels(0, bids, asks, true);
                        }
                        _ => continue,
                    }
                    if tx.send((symbol.clone(), BookUpdate::Book(book.clone()))).await.is_err() {
                        return Ok(());
                    }
                }
            }
        }
    }
}

/// Merge WS state changes into a level list (zero size deletes a level).
fn apply_ws_side(levels: &mut Vec<(f64, f64)>, changes: Vec<(f64, f64)>) {
    for (price, qty) in changes {
        let key = crate::book::price_key(price);
        match levels
            .iter()
            .position(|(px, _)| crate::book::price_key(*px) == key)
        {
            Some(at) if qty == 0.0 => {
                levels.remove(at);
            }
            Some(at) => levels[at].1 = qty,
            None if qty != 0.0 => levels.push((price, qty)),
            None => {}
        }
    }
}

// ---------------------------------------------------------------------------
// API key bootstrap (ChangePubKey)
// ---------------------------------------------------------------------------

/// Register `new_public_key` (from [`generate_api_key`]) on
/// `api_key_index` of the account behind `eth_private_key`. The matching new
/// private key must already be loaded in the signer client slot, so call
/// order is: [`generate_api_key`] → `CreateClient`(new key) → this. Use
/// [`bootstrap_api_key`] for the whole flow.
pub async fn change_api_key(
    base: &str,
    eth_private_key: &str,
    new_public_key: &str,
    api_key_index: u8,
    account_index: i64,
) -> anyhow::Result<String> {
    let lib = signer()?;
    // The Go signer requires the 0x prefix on public keys.
    let pubkey = if new_public_key.starts_with("0x") {
        CString::new(new_public_key)?
    } else {
        CString::new(format!("0x{new_public_key}"))?
    };
    let signed = lib.take_signed(unsafe {
        (lib.sign_change_pub_key)(
            pubkey.as_ptr() as *mut c_char,
            0,  // skip nonce
            -1, // nonce: signer fetches the slot's next nonce itself
            api_key_index as c_int,
            account_index,
        )
    })?;
    let message = signed
        .message_to_sign
        .as_deref()
        .context("ChangePubKey returned no L1 message to sign")?;
    let l1_sig = eth_personal_sign(eth_private_key, message)?;
    let mut tx_info: serde_json::Value =
        serde_json::from_str(&signed.tx_info).context("ChangePubKey tx_info is not JSON")?;
    tx_info["L1Sig"] = serde_json::Value::String(l1_sig);
    let signed = SignedTx {
        tx_info: tx_info.to_string(),
        ..signed
    };
    send_tx(base, &signed).await
}

/// Full API-key setup: generate a key pair, register it on `api_key_index`
/// via `ChangePubKey` (co-signed by the L1 wallet key), wait until the venue
/// reports the new public key, and return `(private_key, public_key)`.
///
/// Put the results into `LIGHTER_PRIVATE_KEY` / `LIGHTER_PUBLIC_KEY`.
pub async fn bootstrap_api_key(
    eth_private_key: &str,
    api_key_index: u8,
    account_index: i64,
) -> anyhow::Result<(String, String)> {
    crate::load_dotenv();
    let base = base_url();
    let (private_key, public_key) = generate_api_key()?;
    let lib = signer()?;
    lib.create_client(
        &base,
        &private_key,
        chain_id_for(&base),
        api_key_index,
        account_index,
    )?;
    change_api_key(
        &base,
        eth_private_key,
        &public_key,
        api_key_index,
        account_index,
    )
    .await?;

    // Wait for the sequencer to apply the key change.
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_secs(2)).await;
        if lib.check_client(api_key_index, account_index).is_ok() {
            return Ok((private_key, public_key));
        }
    }
    bail!("ChangePubKey accepted but api key {api_key_index} did not activate within 60s");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn market_metadata_is_shared_across_symbols_and_blocks_repeated_challenges() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        fn serve_once(
            status: &'static str,
            body: &'static str,
        ) -> (String, std::thread::JoinHandle<()>) {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let worker = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = [0u8; 2048];
                let _ = stream.read(&mut request).unwrap();
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            });
            (base, worker)
        }

        let body = r#"{"order_book_details":[{"symbol":"BTC","market_id":1,"status":"active","taker_fee":"0.02","maker_fee":"0","min_base_amount":"0.001","min_quote_amount":"10","supported_size_decimals":4,"supported_price_decimals":2},{"symbol":"ETH","market_id":2,"status":"active","taker_fee":"0.02","maker_fee":"0","min_base_amount":"0.001","min_quote_amount":"10","supported_size_decimals":4,"supported_price_decimals":2}],"spot_order_book_details":null}"#;
        let (base, worker) = serve_once("200 OK", body);
        assert_eq!(fetch_market_from(&base, "BTC").await.unwrap().market_id, 1);
        assert_eq!(fetch_market_from(&base, "ETH").await.unwrap().market_id, 2);
        worker.join().unwrap();

        let (base, worker) = serve_once(
            "405 Method Not Allowed",
            "<title>Human Verification</title>",
        );
        assert!(
            fetch_market_from(&base, "BTC")
                .await
                .unwrap_err()
                .to_string()
                .contains("Human Verification")
        );
        assert!(
            fetch_market_from(&base, "ETH")
                .await
                .unwrap_err()
                .to_string()
                .contains("temporarily blocked")
        );
        worker.join().unwrap();
    }

    fn market() -> MarketInfo {
        MarketInfo {
            symbol: "ETH".into(),
            market_id: 0,
            size_decimals: 4,
            price_decimals: 2,
            min_base_amount: 0.005,
            min_quote_amount: 10.0,
            taker_fee: 0.0002,
            maker_fee: 0.0,
            last_trade_price: 3000.0,
        }
    }

    #[test]
    fn scales_prices_conservatively_per_side() {
        let info = market();
        // Bids round down, asks round up onto the 0.01 grid.
        assert_eq!(price_to_wire(&info, Side::Buy, 3100.129).unwrap(), 310012);
        assert_eq!(price_to_wire(&info, Side::Sell, 3100.121).unwrap(), 310013);
        assert_eq!(price_to_wire(&info, Side::Buy, 3100.0).unwrap(), 310000);
        assert!(price_to_wire(&info, Side::Buy, 0.0).is_err());
        assert!(price_to_wire(&info, Side::Buy, -1.0).is_err());
        assert!(price_to_wire(&info, Side::Buy, f64::NAN).is_err());
    }

    #[test]
    fn truncates_sizes_onto_the_grid() {
        let info = market();
        assert_eq!(size_to_wire(&info, 0.12349).unwrap(), 1234);
        assert_eq!(size_to_wire(&info, 1.0).unwrap(), 10000);
        assert!(size_to_wire(&info, 0.00001).is_err()); // below one step
        assert!(size_to_wire(&info, 0.0).is_err());
        assert!(size_to_wire(&info, -3.0).is_err());
    }

    #[test]
    fn wire_scaling_is_stable_across_roundtrips() {
        let info = market();
        // A size that came back off the wire (48 -> 0.0048, stored as
        // 0.004799999...) must scale back to 48, not 47.
        let qty = scale_from_int(48, info.size_decimals);
        assert_eq!(size_to_wire(&info, qty).unwrap(), 48);
        // Same for prices sitting exactly on the grid, both sides.
        let px = scale_from_int(220302, info.price_decimals);
        assert_eq!(price_to_wire(&info, Side::Buy, px).unwrap(), 220302);
        assert_eq!(price_to_wire(&info, Side::Sell, px).unwrap(), 220302);
        // Ten scale/unscale cycles stay fixed.
        let mut wire = 48;
        for _ in 0..10 {
            wire = size_to_wire(&info, scale_from_int(wire, info.size_decimals)).unwrap();
        }
        assert_eq!(wire, 48);
    }

    #[test]
    fn min_qty_takes_the_binding_minimum() {
        let info = market();
        // At $3000, 10 USDC needs 0.00333 ETH < 0.005 min base -> base binds.
        assert!((info.min_qty(3000.0) - 0.005).abs() < 1e-12);
        // At $100, quote minimum binds (0.1 ETH).
        assert!((info.min_qty(100.0) - 0.1).abs() < 1e-12);
        assert!((info.tick_size() - 0.01).abs() < 1e-12);
        assert!((info.size_step() - 0.0001).abs() < 1e-12);
    }

    #[test]
    fn client_order_indexes_are_unique_and_u48() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..64 {
            let id = next_client_order_index();
            assert!(id <= MAX_CLIENT_ORDER_INDEX);
            assert!(seen.insert(id), "duplicate client order index {id}");
        }
    }

    #[test]
    fn maps_wire_statuses_to_order_states() {
        assert_eq!(order_state("open"), OrderState::Open);
        assert_eq!(order_state("pending"), OrderState::Open);
        assert_eq!(order_state("in-progress"), OrderState::Open);
        assert_eq!(order_state("filled"), OrderState::Filled);
        assert_eq!(order_state("canceled"), OrderState::Canceled);
        assert_eq!(order_state("canceled-post-only"), OrderState::Canceled);
        assert_eq!(
            order_state("canceled-margin-not-allowed"),
            OrderState::Canceled
        );
        assert_eq!(order_state("weird"), OrderState::Rejected);
    }

    #[test]
    fn aggregates_book_orders_per_level() {
        let side = book_side(vec![
            WireBookOrder {
                price: "100.5".into(),
                remaining_base_amount: "1.0".into(),
            },
            WireBookOrder {
                price: "100.5".into(),
                remaining_base_amount: "2.0".into(),
            },
            WireBookOrder {
                price: "101.0".into(),
                remaining_base_amount: "0.5".into(),
            },
            WireBookOrder {
                price: "bogus".into(),
                remaining_base_amount: "1".into(),
            },
        ]);
        assert_eq!(side.len(), 2);
        assert!((side[0].1 - 3.0).abs() < 1e-9);
        assert!((side[1].1 - 0.5).abs() < 1e-9);
    }

    #[test]
    fn applies_ws_state_changes_with_zero_deletes() {
        let mut levels = vec![(100.0, 1.0), (101.0, 2.0)];
        apply_ws_side(
            &mut levels,
            vec![(100.0, 0.0), (102.0, 3.0), (101.0, 5.0), (103.0, 0.0)],
        );
        levels.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        assert_eq!(levels, vec![(101.0, 5.0), (102.0, 3.0)]);
    }

    #[test]
    fn personal_sign_recovers_to_the_signing_address() {
        use k256::ecdsa::{RecoveryId, Signature, SigningKey, VerifyingKey};
        let key_hex = "4c0883a69102937d6231471b5dbb6204fe51296170827936ea5cce4b76994b0f";
        let message = "lighter change pubkey test";
        let signature = eth_personal_sign(key_hex, message).unwrap();
        let bytes = hex::decode(signature.trim_start_matches("0x")).unwrap();
        assert_eq!(bytes.len(), 65);
        assert!(bytes[64] == 27 || bytes[64] == 28);

        let prefixed = format!("\x19Ethereum Signed Message:\n{}{}", message.len(), message);
        let digest = keccak256(prefixed.as_bytes());
        let recovered = VerifyingKey::recover_from_prehash(
            &digest,
            &Signature::from_slice(&bytes[..64]).unwrap(),
            RecoveryId::try_from(bytes[64] - 27).unwrap(),
        )
        .unwrap();
        let expected = SigningKey::from_slice(&hex::decode(key_hex).unwrap())
            .unwrap()
            .verifying_key()
            .to_owned();
        assert_eq!(recovered, expected);
    }

    #[test]
    fn ws_url_and_chain_id_derivation() {
        assert_eq!(
            ws_url("https://mainnet.zklighter.elliot.ai"),
            "wss://mainnet.zklighter.elliot.ai/stream"
        );
        assert_eq!(
            ws_url("https://mainnet.zklighter.elliot.ai/"),
            "wss://mainnet.zklighter.elliot.ai/stream"
        );
        assert_eq!(chain_id_for("https://mainnet.zklighter.elliot.ai"), 304);
        assert_eq!(chain_id_for("https://testnet.zklighter.elliot.ai"), 300);
    }
}
