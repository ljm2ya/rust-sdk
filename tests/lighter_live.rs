//! **Real order trading tests** — verification of every `Lighter` method
//! against the LIVE exchange with your credentials.
//!
//! Ignored by default so a bare `cargo test` can never trade; run explicitly:
//!
//! ```bash
//! cargo test --test lighter_live -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Credentials come from the environment or `.env` (`LIGHTER_PRIVATE_KEY`,
//! `LIGHTER_API_KEY_INDEX`, and `LIGHTER_ACCOUNT_INDEX` or `WALLET_ADDRESS`).
//! The target market defaults to `PAXG`; override with `RUSK_LIGHTER_MARKET`.
//!
//! Safety posture:
//! - Orders are sized just above the market's minimum (max of min base /
//!   min quote) — the smallest real order the venue accepts.
//! - `live_order_roundtrip` only rests a deep bid at ~50% of mid (never
//!   fills), exercises place/open_orders/fetch/modify/cancel, and always
//!   cleans up after itself.
//! - `live_market_fill_and_flatten` REALLY fills a minimum-size order and
//!   immediately flattens with a reduce-only market order.
//! - `live_rejections` only sends orders the venue/capsule must refuse.
//! - `live_bootstrap_api_key` is additionally gated on
//!   `RUSK_LIGHTER_BOOTSTRAP=1` because it rotates the API key slot.
//! - Run with `--test-threads=1`; never run in CI.

use rusk_sdk::book::BookUpdate;
use rusk_sdk::lighter::{Lighter, PositionSnapshot, bootstrap_api_key, fetch_market, stream_book};
use rusk_sdk::order::{OrderState, Side};

fn live_market() -> String {
    std::env::var("RUSK_LIGHTER_MARKET").unwrap_or_else(|_| "PAXG".to_string())
}

fn install_crypto() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    dotenvy::dotenv().ok();
}

async fn connect_trading() -> Lighter {
    install_crypto();
    let market = live_market();
    let lighter = Lighter::connect_trading(&market)
        .await
        .expect("connect_trading (needs LIGHTER_PRIVATE_KEY and a resolvable account)");
    println!(
        "connected: market={} account_index={}",
        lighter.symbol(),
        lighter.account_index()
    );
    lighter
}

/// Smallest maker-eligible size on this market, with slack for truncation.
fn min_size(info: &rusk_sdk::lighter::MarketInfo, price: f64) -> f64 {
    let qty = info.min_qty(price) * 1.05;
    // Round up onto the size grid.
    let step = info.size_step();
    (qty / step).ceil() * step
}

/// Poll `position_snapshot` until `done` (sequencing lags the REST reads).
async fn poll_position(lighter: &Lighter, done: impl Fn(&PositionSnapshot) -> bool) -> PositionSnapshot {
    const ATTEMPTS: u32 = 10;
    const DELAY: std::time::Duration = std::time::Duration::from_millis(1500);
    let mut last = lighter.position_snapshot().await.expect("position_snapshot");
    for _ in 0..ATTEMPTS {
        if done(&last) {
            return last;
        }
        tokio::time::sleep(DELAY).await;
        last = lighter.position_snapshot().await.expect("position_snapshot");
    }
    panic!("position did not converge after {ATTEMPTS} attempts: {last:?}");
}

/// Poll `fetch_order` until it is visible and satisfies `done` (the
/// sequencer applies transactions asynchronously).
async fn poll_order(
    lighter: &Lighter,
    order_id: u64,
    done: impl Fn(&rusk_sdk::order::OrderInfo) -> bool,
) -> rusk_sdk::order::OrderInfo {
    const ATTEMPTS: u32 = 10;
    const DELAY: std::time::Duration = std::time::Duration::from_millis(1000);
    let mut last = None;
    for _ in 0..ATTEMPTS {
        if let Some(info) = lighter.fetch_order(order_id).await.expect("fetch_order") {
            if done(&info) {
                return info;
            }
            last = Some(info);
        }
        tokio::time::sleep(DELAY).await;
    }
    panic!("order {order_id} did not converge after {ATTEMPTS} attempts: {last:?}");
}

/// One-time setup: generate an API key pair and register it on the account
/// via ChangePubKey (co-signed by the L1 `PRIVATE_KEY`). Prints the keys to
/// put into `.env`. Extra-gated: set `RUSK_LIGHTER_BOOTSTRAP=1`.
#[tokio::test]
#[ignore = "rotates the account's Lighter API key slot (RUSK_LIGHTER_BOOTSTRAP=1 to run)"]
async fn live_bootstrap_api_key() {
    install_crypto();
    if std::env::var("RUSK_LIGHTER_BOOTSTRAP").as_deref() != Ok("1") {
        println!("skipped: set RUSK_LIGHTER_BOOTSTRAP=1 to rotate the API key");
        return;
    }
    let eth_key = std::env::var("PRIVATE_KEY").expect("PRIVATE_KEY (L1 wallet key)");
    let wallet = std::env::var("WALLET_ADDRESS").expect("WALLET_ADDRESS");
    let account_index = rusk_sdk::lighter::account_index_for(&wallet)
        .await
        .expect("account_index_for");
    let api_key_index: u8 = rusk_sdk::env_or("LIGHTER_API_KEY_INDEX", "2").parse().unwrap();
    println!("bootstrapping api key {api_key_index} on account {account_index}");
    let (private_key, public_key) = bootstrap_api_key(&eth_key, api_key_index, account_index)
        .await
        .expect("bootstrap_api_key");
    println!("LIGHTER_PRIVATE_KEY={private_key}");
    println!("LIGHTER_PUBLIC_KEY={public_key}");
    println!("LIGHTER_ACCOUNT_INDEX={account_index}");
    println!("LIGHTER_API_KEY_INDEX={api_key_index}");
}

/// Credential-free market metadata: grids and bounds must be coherent, and
/// must match the trading capsule's cached view.
#[tokio::test]
#[ignore = "hits live Lighter market metadata (public, no credentials)"]
async fn live_fetch_market() {
    install_crypto();

    let info = fetch_market(&live_market()).await.expect("fetch_market");
    println!("fetch_market({}): {info:?}", live_market());
    assert_eq!(info.symbol.to_uppercase(), live_market().to_uppercase());
    assert!(info.size_decimals <= 8, "{info:?}");
    assert!(info.price_decimals <= 8, "{info:?}");
    assert!(info.min_base_amount > 0.0 || info.min_quote_amount > 0.0, "{info:?}");
    assert!(info.last_trade_price > 0.0, "{info:?}");

    // Unknown market must be rejected, not silently defaulted.
    let err = fetch_market("RUSK_NO_SUCH_MARKET").await.unwrap_err();
    println!("unknown market -> {err:#}");
    assert!(format!("{err:#}").contains("RUSK_NO_SUCH_MARKET"));

    // Capsule cache agrees with the standalone fetch.
    let lighter = Lighter::connect(&live_market()).await.expect("connect (read-only)");
    let cached = lighter.market_info();
    assert_eq!(cached.market_id, info.market_id);
    assert_eq!(cached.size_decimals, info.size_decimals);
    assert_eq!(cached.price_decimals, info.price_decimals);
}

/// Public REST book + WS book stream: synced, uncrossed, and the stream
/// keeps emitting coherent books.
#[tokio::test]
#[ignore = "hits live Lighter market data (public, no credentials)"]
async fn live_book_and_stream() {
    install_crypto();

    let lighter = Lighter::connect(&live_market()).await.expect("connect (read-only)");
    let book = lighter.book().await.expect("book");
    let (bid, ask) = (book.best_bid(), book.best_ask());
    println!("REST book top: bid={bid:?} ask={ask:?}");
    let (bid, ask) = (bid.expect("bid side"), ask.expect("ask side"));
    assert!(bid < ask, "book is crossed: {bid} >= {ask}");
    let mid = lighter.mid_price().await.expect("mid_price");
    assert!((mid - (bid + ask) / 2.0).abs() / mid < 0.01);

    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let market = live_market();
    let stream = tokio::spawn(async move { stream_book(&market, tx).await });

    let mut books = 0;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    while books < 5 {
        let update = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .expect("stream produced fewer than 5 books in 30s")
            .expect("stream ended prematurely");
        match update {
            BookUpdate::Book(book) => {
                let (Some(bid), Some(ask)) = (book.best_bid(), book.best_ask()) else {
                    continue;
                };
                assert!(book.is_synced());
                assert!(bid < ask, "streamed book crossed: {bid} >= {ask}");
                books += 1;
                println!("streamed book {books}: bid={bid} ask={ask}");
            }
            BookUpdate::Notice(notice) => println!("stream notice: {notice}"),
        }
    }
    drop(rx);
    stream.abort();
}

/// Read-only account access: summary and position parse cleanly.
#[tokio::test]
#[ignore = "hits live Lighter account data (needs a resolvable account)"]
async fn live_account_readonly() {
    install_crypto();

    let lighter = Lighter::connect(&live_market()).await.expect("connect (read-only)");
    let summary = lighter.account_summary().await.expect("account_summary");
    println!("account_summary: {summary:?}");
    assert!(summary.collateral >= 0.0);

    let position = lighter.position_snapshot().await.expect("position_snapshot");
    println!("position_snapshot: {position:?}");
    assert!(position.position.is_finite());

    let amt = lighter.position_amt().await.expect("position_amt");
    assert_eq!(amt, position.position);
}

/// Deep resting bid → open_orders → fetch_order → modify_order →
/// cancel_order → fetch shows Canceled. Nothing ever fills.
#[tokio::test]
#[ignore = "places a real (deep, non-filling) minimum-size order on Lighter"]
async fn live_order_roundtrip() {
    let lighter = connect_trading().await;
    let info = lighter.market_info();

    let mid = lighter.mid_price().await.expect("mid_price");
    // Deep bid at ~50% of mid. Minimums are checked against the order's own
    // limit price, so size for the deep price.
    let deep_px = mid * 0.5;
    let qty = min_size(&info, deep_px);
    println!("placing deep bid: qty={qty} @ ~{deep_px:.6} (mid {mid})");

    let order = lighter
        .place_limit(Side::Buy, qty, deep_px, false)
        .await
        .expect("place_limit");
    println!("placed: {order:?}");
    assert!(order.order_id > 0);

    let resting = poll_order(&lighter, order.order_id, |info| info.state == OrderState::Open).await;
    println!("resting: {resting:?}");
    assert_eq!(resting.side, Side::Buy);
    assert_eq!(resting.filled_qty, 0.0);
    assert!(resting.limit_px <= deep_px * 1.001);

    let open = lighter.open_orders().await.expect("open_orders");
    assert!(open.iter().any(|o| o.order_id == order.order_id), "{open:?}");

    // Modify: nudge the price up 2% (still ~half of mid, still non-filling).
    let new_px = deep_px * 1.02;
    let modified = lighter
        .modify_order(&order, new_px, None)
        .await
        .expect("modify_order");
    println!("modified: {modified:?}");
    assert_eq!(modified.order_id, order.order_id);
    let repriced = poll_order(&lighter, order.order_id, |info| {
        info.state == OrderState::Open && (info.limit_px - resting.limit_px).abs() > 1e-12
    })
    .await;
    println!("repriced: {repriced:?}");
    assert!(repriced.limit_px > resting.limit_px);

    let tx_hash = lighter.cancel_order(order.order_id).await.expect("cancel_order");
    println!("cancel tx: {tx_hash}");
    let closed = poll_order(&lighter, order.order_id, |info| info.state == OrderState::Canceled).await;
    println!("after cancel: {closed:?}");

    let leftovers = lighter.open_orders().await.expect("open_orders");
    assert!(
        !leftovers.iter().any(|o| o.order_id == order.order_id),
        "no leftovers from this test: {leftovers:?}"
    );
    println!("roundtrip complete, book clean");
}

/// Market order (minimum size, REALLY fills) → position reflects it →
/// reduce-only market order restores the starting position.
///
/// Lighter's min base/quote amounts apply to maker orders only, so the
/// smallest real taker order is a few size steps — that is what we send
/// (bounded cost: a fraction of a cent on fine-grained markets).
#[tokio::test]
#[ignore = "fills a real minimum-size order on Lighter, then flattens it"]
async fn live_market_fill_and_flatten() {
    let lighter = connect_trading().await;
    let info = lighter.market_info();

    let start = lighter.position_snapshot().await.expect("position_snapshot");
    println!("starting position: {start:?}");
    let (open_side, close_side, sign) = if start.position < 0.0 {
        (Side::Sell, Side::Buy, -1.0)
    } else {
        (Side::Buy, Side::Sell, 1.0)
    };

    let mid = lighter.mid_price().await.expect("mid_price");
    let qty = 3.0 * info.size_step();
    println!("opening {open_side:?} qty={qty} (~{:.2} USDC at mid {mid})", qty * mid);

    let open = lighter
        .place_market(open_side, qty, false)
        .await
        .expect("place_market open leg");
    println!("market open leg: {open:?}");

    let eps = open.qty * 0.1;
    let after_open =
        poll_position(&lighter, |p| (p.position - start.position) * sign >= open.qty - eps).await;
    println!(
        "position after fill: {after_open:?} (position_amt={})",
        lighter.position_amt().await.expect("position_amt")
    );
    assert!(after_open.avg_entry_price > 0.0);

    // Restore exactly the delta we just opened, reduce-only.
    let delta = (after_open.position - start.position) * sign;
    let close = lighter
        .place_market(close_side, delta, true)
        .await
        .expect("place_market close leg (reduce-only)");
    println!("reduce-only market close leg: {close:?}");

    let flattened =
        poll_position(&lighter, |p| (p.position - start.position).abs() <= delta * 0.05).await;
    println!("position restored: {flattened:?}");
}

/// Orders the venue (or the capsule, locally) must refuse. Nothing rests.
#[tokio::test]
#[ignore = "hits the live venue (rejections only)"]
async fn live_rejections() {
    let lighter = connect_trading().await;
    let mid = lighter.mid_price().await.expect("mid_price");
    let deep_px = mid * 0.5;

    // Size that truncates to zero on the market grid → local error, no network.
    let err = lighter.place_limit(Side::Buy, 0.0, deep_px, false).await.unwrap_err();
    println!("zero size -> {err}");

    // Negative price → local error.
    let err = lighter.place_limit(Side::Buy, 1.0, -1.0, false).await.unwrap_err();
    println!("negative price -> {err}");

    // Below the market minimum → refused client-side (never hits the wire).
    let info = lighter.market_info();
    let tiny = info.size_step();
    if tiny * deep_px < info.min_quote_amount || tiny < info.min_base_amount {
        let err = lighter.place_limit(Side::Buy, tiny, deep_px, false).await.unwrap_err();
        println!("below minimum -> {err}");
        assert!(err.to_string().to_lowercase().contains("minimum"), "{err}");
    }

    // Unknown client order index: the order is simply not known.
    assert!(lighter.fetch_order(1).await.expect("fetch_order query").is_none());

    // Read-only capsules refuse signed actions.
    install_crypto();
    let watcher = Lighter::connect(&live_market()).await.expect("connect (read-only)");
    let err = watcher.place_limit(Side::Buy, 1.0, deep_px, false).await.unwrap_err();
    println!("read-only place -> {err}");
    assert!(err.to_string().contains("read-only"), "{err}");
    let err = watcher.open_orders().await.unwrap_err();
    println!("read-only open_orders -> {err}");
}
