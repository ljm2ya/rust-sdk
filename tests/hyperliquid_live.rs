//! **Real order trading tests** — the canonical verification of every
//! `Hyperliquid` method against the LIVE exchange with your credentials
//! (originally developed in perppair).
//!
//! Ignored by default so a bare `cargo test` can never trade; run explicitly:
//!
//! ```bash
//! cargo test --test hyperliquid_live -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Credentials come from the environment or `.env` (`PRIVATE_KEY`,
//! `WALLET_ADDRESS`). The target market defaults to the dashboard's leg A
//! (`SKHY` → `xyz:SKHY`); override with `RUSK_LIVE_COIN`.
//!
//! Safety posture:
//! - Every order is sized to ~$10.5 notional — just above Hyperliquid's $10
//!   minimum order value, i.e. the smallest real order the venue accepts.
//! - `live_order_roundtrip` only rests a deep bid at ~50% of mid (never
//!   fills), exercises place/open_orders/fetch/modify/cancel, and always
//!   cleans up after itself.
//! - `live_market_fill_and_flatten` REALLY fills ~$10.5 (bounded cost:
//!   size × spread) and immediately flattens with a reduce-only sell.
//! - `live_rejections` only sends orders the venue must refuse; nothing
//!   ever rests.
//! - Run with `--test-threads=1`; never run in CI.

use rusk_sdk::hyperliquid::{Hyperliquid, PositionSnapshot};
use rusk_sdk::order::{OrderState, Side};

/// Just above the venue's $10 minimum order value.
const TARGET_NOTIONAL_USDC: f64 = 10.5;

fn live_coin() -> String {
    std::env::var("RUSK_LIVE_COIN").unwrap_or_else(|_| "SKHY".to_string())
}

async fn connect_trading() -> Hyperliquid {
    let _ = rustls::crypto::ring::default_provider().install_default();
    dotenvy::dotenv().ok();
    let coin = live_coin();
    let hl = Hyperliquid::connect_trading(&coin)
        .await
        .expect("connect_trading (needs PRIVATE_KEY + WALLET_ADDRESS and a resolvable market)");
    println!("connected: label={coin} coin={}", hl.coin());
    hl
}

/// Poll `position_snapshot` until `done` (fills can lag the position read).
async fn poll_position(hl: &Hyperliquid, done: impl Fn(&PositionSnapshot) -> bool) -> PositionSnapshot {
    const ATTEMPTS: u32 = 10;
    const DELAY: std::time::Duration = std::time::Duration::from_millis(1500);
    let mut last = hl.position_snapshot().await.expect("position_snapshot");
    for _ in 0..ATTEMPTS {
        if done(&last) {
            return last;
        }
        tokio::time::sleep(DELAY).await;
        last = hl.position_snapshot().await.expect("position_snapshot");
    }
    panic!("position did not converge after {ATTEMPTS} attempts: {last:?}");
}

/// Deep resting bid → open_orders → fetch_order → modify_order →
/// cancel_order → fetch shows Canceled. Nothing ever fills.
#[tokio::test]
#[ignore = "places a real (deep, non-filling) ~$10 order on Hyperliquid"]
async fn live_order_roundtrip() {
    let hl = connect_trading().await;

    let book = hl.book().await.expect("book");
    let mid = hl.mid_price().await.expect("mid_price");
    println!("book top: bid={:?} ask={:?} mid={mid}", book.best_bid(), book.best_ask());

    // Deep bid at ~50% of mid; sized so notional at the limit price clears
    // the $10 minimum (with margin for size truncation).
    let deep_px = mid * 0.5;
    let qty = TARGET_NOTIONAL_USDC / deep_px * 1.02;
    let order = hl.place_limit(Side::Buy, qty, deep_px, false).await.expect("place_limit");
    println!("placed deep bid: {order:?} (limit ~{deep_px:.4})");
    assert!(order.order_id > 0);

    let open = hl.open_orders().await.expect("open_orders");
    println!("open_orders: {open:?}");
    assert!(open.iter().any(|o| o.order_id == order.order_id), "our deep bid should be resting");

    let fetched = hl.fetch_order(order.order_id).await.expect("fetch_order").expect("known oid");
    println!("fetch_order: {fetched:?}");
    assert_eq!(fetched.state, OrderState::Open);
    assert_eq!(fetched.side, Side::Buy);
    assert_eq!(fetched.filled_qty, 0.0);

    let replaced = hl
        .modify_order(&order, deep_px * 1.02, false)
        .await
        .expect("modify_order")
        .expect("deep order must still be live after modify");
    println!("modify_order -> {replaced:?}");

    hl.cancel_order(replaced.order_id).await.expect("cancel_order");
    let closed = hl
        .fetch_order(replaced.order_id)
        .await
        .expect("fetch_order after cancel")
        .expect("orderStatus keeps closed orders");
    println!("after cancel: {closed:?}");
    assert_eq!(closed.state, OrderState::Canceled);

    let leftovers = hl.open_orders().await.expect("open_orders");
    assert!(
        !leftovers.iter().any(|o| o.order_id == replaced.order_id || o.order_id == order.order_id),
        "no leftovers from this test: {leftovers:?}"
    );
    println!("roundtrip complete, book clean");
}

/// Synthetic market order (~$10.5, REALLY fills) → position_snapshot
/// reflects it → reduce-only market order restores the starting position.
///
/// Direction-aware: the opening leg extends the account's CURRENT position
/// direction (short → sell first), so the restore leg always opposes the
/// position and is legal as reduce-only.
#[tokio::test]
#[ignore = "fills a real ~$10 order on Hyperliquid, then flattens it"]
async fn live_market_fill_and_flatten() {
    let hl = connect_trading().await;

    let start = hl.position_snapshot().await.expect("position_snapshot");
    println!("starting position: {start:?}");
    let (open_side, close_side, sign) = if start.size < 0.0 {
        (Side::Sell, Side::Buy, -1.0)
    } else {
        (Side::Buy, Side::Sell, 1.0)
    };
    let mid = hl.mid_price().await.expect("mid_price");
    let qty = TARGET_NOTIONAL_USDC / mid;
    println!("opening {open_side:?} qty~{qty:.4} (~${TARGET_NOTIONAL_USDC} at mid {mid})");

    let open = hl.place_market(open_side, qty, false).await.expect("place_market open leg");
    println!("market open leg: {open:?}");
    assert!(open.order_id > 0);

    // position_amt is the scalar convenience view of position_snapshot.
    let eps = open.qty * 0.1;
    let after_open = poll_position(&hl, |p| (p.size - start.size) * sign >= open.qty - eps).await;
    println!("position after fill: {after_open:?} (position_amt={})", hl.position_amt().await.expect("position_amt"));
    assert!(after_open.entry_px.is_some());

    // Restore exactly the delta we just opened, reduce-only.
    let delta = (after_open.size - start.size) * sign;
    let close = hl.place_market(close_side, delta, true).await.expect("place_market close leg (reduce-only)");
    println!("reduce-only market close leg: {close:?}");

    let flattened = poll_position(&hl, |p| (p.size - start.size).abs() <= delta * 0.05).await;
    println!("position restored: {flattened:?}");
}

/// Orders the venue (or the capsule, locally) must refuse. Nothing rests.
#[tokio::test]
#[ignore = "hits the live venue (rejections only)"]
async fn live_rejections() {
    let hl = connect_trading().await;
    let mid = hl.mid_price().await.expect("mid_price");

    // Size that truncates to zero on the market grid → local error, no network.
    let err = hl.place_limit(Side::Buy, 0.0, mid * 0.5, false).await.unwrap_err();
    println!("zero size -> {err}");

    // Negative price → local error.
    let err = hl.place_limit(Side::Buy, 1.0, -1.0, false).await.unwrap_err();
    println!("negative price -> {err}");

    // ~$2 notional is below the venue's $10 minimum order value → rejected
    // server-side, never rests.
    let deep_px = mid * 0.5;
    let err = hl.place_limit(Side::Buy, 2.0 / deep_px, deep_px, false).await.unwrap_err();
    println!("below $10 minimum -> {err}");
    assert!(err.to_string().to_lowercase().contains("minimum"), "{err}");

    // Unknown oid: fetch returns None, cancel is refused.
    assert!(hl.fetch_order(1).await.expect("fetch_order query").is_none());
    let err = hl.cancel_order(1).await.unwrap_err();
    println!("cancel unknown oid -> {err}");

    // Read-only capsules refuse signed actions.
    let watcher = Hyperliquid::connect(&live_coin()).await.expect("connect (read-only)");
    let err = watcher.place_limit(Side::Buy, 1.0, deep_px, false).await.unwrap_err();
    println!("read-only place -> {err}");
    assert!(err.to_string().contains("read-only"), "{err}");
}
