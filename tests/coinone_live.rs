//! **Real order trading tests** — verification of the `Coinone` capsule
//! against the LIVE exchange with your credentials.
//!
//! Ignored by default so a bare `cargo test` can never trade; run explicitly:
//!
//! ```bash
//! cargo test --test coinone_live -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Credentials come from the environment or `.env` (`COINONE_ACCESS_TOKEN` /
//! `COINONE_SECRET_KEY`; the API key must whitelist this machine's IP).
//! The target market defaults to `KRW-BTC`; override with
//! `RUSK_COINONE_MARKET`.
//!
//! Safety posture:
//! - `live_order_roundtrip` only rests a deep bid at ~50% of mid, sized just
//!   above the market's `min_order_amount` (never fills), exercises
//!   place/open_orders/fetch/cancel, and always cleans up after itself.
//! - `live_market_fill_and_flatten_with_user_events` REALLY fills a
//!   minimum-size market order and immediately flattens it (bounded cost:
//!   fees + spread, a few KRW).
//! - `live_rejections` only sends orders the venue/capsule must refuse.
//! - Run with `--test-threads=1`; never run in CI.

use rusk_sdk::book::BookUpdate;
use rusk_sdk::coinone::{Coinone, UserEvent, stream_book, stream_user_events};
use rusk_sdk::order::{OrderState, Side};

fn live_market() -> String {
    std::env::var("RUSK_COINONE_MARKET").unwrap_or_else(|_| "KRW-BTC".to_string())
}

fn install_crypto() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    dotenvy::dotenv().ok();
}

async fn connect_trading() -> Coinone {
    install_crypto();
    let coinone = Coinone::connect_trading(&live_market())
        .await
        .expect("connect_trading (needs COINONE_ACCESS_TOKEN / COINONE_SECRET_KEY)");
    println!("connected: market={} info={:?}", coinone.market(), coinone.market_info());
    coinone
}

/// Poll `fetch_order` until visible and satisfying `done`.
async fn poll_order(
    coinone: &Coinone,
    order_id: u64,
    done: impl Fn(&rusk_sdk::order::OrderInfo) -> bool,
) -> rusk_sdk::order::OrderInfo {
    const ATTEMPTS: u32 = 10;
    const DELAY: std::time::Duration = std::time::Duration::from_millis(1000);
    let mut last = None;
    for _ in 0..ATTEMPTS {
        if let Some(info) = coinone.fetch_order(order_id).await.expect("fetch_order") {
            if done(&info) {
                return info;
            }
            last = Some(info);
        }
        tokio::time::sleep(DELAY).await;
    }
    panic!("order {order_id} did not converge after {ATTEMPTS} attempts: {last:?}");
}

/// Poll the free balance of `currency` until `done(available)`.
async fn poll_balance(coinone: &Coinone, currency: &str, done: impl Fn(f64) -> bool) -> f64 {
    const ATTEMPTS: u32 = 15;
    const DELAY: std::time::Duration = std::time::Duration::from_millis(1000);
    let mut last = 0.0;
    for _ in 0..ATTEMPTS {
        last = coinone
            .balance(currency)
            .await
            .expect("balance")
            .map(|b| b.available)
            .unwrap_or(0.0);
        if done(last) {
            return last;
        }
        tokio::time::sleep(DELAY).await;
    }
    panic!("{currency} balance did not converge after {ATTEMPTS} attempts: {last}");
}

/// Public quotation data: metadata, ticker, REST book, trades, candles.
#[tokio::test]
#[ignore = "hits live Coinone market data (public, no credentials)"]
async fn live_ticker_and_book() {
    install_crypto();

    let coinone = Coinone::connect(&live_market()).await.expect("connect (read-only)");
    let info = coinone.market_info();
    println!("market_info: {info:?}");
    assert!(info.price_unit > 0.0 && info.qty_unit > 0.0 && info.min_order_amount > 0.0);

    let ticker = coinone.ticker().await.expect("ticker");
    println!("ticker: {ticker:?}");
    assert!(ticker.last > 0.0);

    let book = coinone.book().await.expect("book");
    let (bid, ask) = (book.best_bid().expect("bid side"), book.best_ask().expect("ask side"));
    println!("REST book top: bid={bid} ask={ask}");
    assert!(bid < ask, "book is crossed: {bid} >= {ask}");
    let mid = coinone.mid_price().await.expect("mid_price");
    assert!((mid - (bid + ask) / 2.0).abs() / mid < 0.01);

    let trades = coinone.recent_trades(10).await.expect("recent_trades");
    println!("recent trades: {}", trades.len());
    assert!(!trades.is_empty());
    let candles = coinone.candles("1m", 3).await.expect("candles");
    assert_eq!(candles.len(), 3);

    // Unknown market must be rejected at connect.
    let err = Coinone::connect("KRW-RUSKNOSUCH").await.unwrap_err();
    println!("unknown market -> {err:#}");
}

/// Public WS book stream keeps emitting coherent snapshots.
#[tokio::test]
#[ignore = "hits live Coinone market data (public, no credentials)"]
async fn live_stream_book() {
    install_crypto();

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

/// Signed read-only access: balances and fees parse cleanly.
#[tokio::test]
#[ignore = "hits live Coinone account data (needs credentials)"]
async fn live_account_readonly() {
    let coinone = connect_trading().await;

    let balances = coinone.balances().await.expect("balances");
    println!("balances ({}): {balances:?}", balances.len());
    for balance in &balances {
        assert!(balance.available >= 0.0 && balance.locked >= 0.0, "{balance:?}");
    }

    let quote = coinone.balance(coinone.quote()).await.expect("balance");
    println!("{} balance: {quote:?}", coinone.quote());

    let (maker, taker) = coinone.trade_fees().await.expect("trade_fees");
    println!("fees: maker={maker} taker={taker}");
    assert!((0.0..0.1).contains(&maker) && (0.0..0.1).contains(&taker));
}

/// Deep resting bid → open_orders → fetch_order → cancel_order → fetch shows
/// Canceled. Nothing ever fills.
#[tokio::test]
#[ignore = "places a real (deep, non-filling) minimum-size order on Coinone"]
async fn live_order_roundtrip() {
    let coinone = connect_trading().await;
    let info = coinone.market_info();

    let mid = coinone.mid_price().await.expect("mid_price");
    // Deep bid at ~50% of mid; size just above the venue minimum value.
    let deep_px = mid * 0.5;
    let qty = info.min_order_amount * 1.1 / deep_px;
    println!("placing deep bid: qty={qty} @ ~{deep_px:.4} (mid {mid})");

    let order = coinone.place_limit(Side::Buy, qty, deep_px).await.expect("place_limit");
    println!("placed: {order:?}");
    assert!(order.order_id > 0);
    assert_eq!(order.side, Side::Buy);

    let resting = poll_order(&coinone, order.order_id, |info| info.state == OrderState::Open).await;
    println!("resting: {resting:?}");
    assert_eq!(resting.side, Side::Buy);
    assert_eq!(resting.filled_qty, 0.0);
    assert!(resting.limit_px <= deep_px * 1.001);

    let open = coinone.open_orders().await.expect("open_orders");
    assert!(open.iter().any(|o| o.order_id == order.order_id), "{open:?}");

    coinone.cancel_order(order.order_id).await.expect("cancel_order");
    let closed =
        poll_order(&coinone, order.order_id, |info| info.state == OrderState::Canceled).await;
    println!("after cancel: {closed:?}");

    let leftovers = coinone.open_orders().await.expect("open_orders");
    assert!(
        !leftovers.iter().any(|o| o.order_id == order.order_id),
        "no leftovers from this test: {leftovers:?}"
    );
    println!("roundtrip complete, book clean");
}

/// Market buy (minimum size, REALLY fills) → base balance reflects it →
/// market sell of exactly the received amount restores the position — with
/// the private WS (`MYORDER`/`MYASSET`) streaming throughout.
#[tokio::test]
#[ignore = "REALLY fills a minimum-size market order on Coinone, then flattens it (also verifies the private WS)"]
async fn live_market_fill_and_flatten_with_user_events() {
    let coinone = connect_trading().await;
    let target = coinone.target().to_string();
    let spend = coinone.market_info().min_order_amount * 1.2;

    // Private WS up before trading so the subscription catches the fills.
    let (tx, mut rx) = tokio::sync::mpsc::channel(256);
    let stream = tokio::spawn(async move { stream_user_events(tx).await });
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert!(!stream.is_finished(), "private WS died before trading started");

    let start = coinone
        .balance(&target)
        .await
        .expect("balance")
        .map(|b| b.available)
        .unwrap_or(0.0);
    println!("starting {target} balance: {start}");

    let buy = coinone.market_buy(spend).await.expect("market_buy");
    println!("market buy ({spend} {}): {buy:?}", coinone.quote());
    let after_buy = poll_balance(&coinone, &target, |bal| bal > start + 1e-12).await;
    let bought = after_buy - start;
    println!("filled: +{bought} {target}");

    let info = coinone
        .fetch_order(buy.order_id)
        .await
        .expect("fetch_order")
        .expect("buy order visible");
    println!("buy order: {info:?}");
    assert_ne!(info.state, OrderState::Open);
    assert!(info.filled_qty > 0.0, "{info:?}");

    let sell = coinone.market_sell(bought).await.expect("market_sell");
    println!("market sell: {sell:?}");
    let flat = poll_balance(&coinone, &target, |bal| bal <= start + bought * 0.01).await;
    println!("flattened: {target} balance {flat} (start {start})");

    // Both legs must have produced private WS traffic.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let (mut order_events, mut asset_events) = (0, 0);
    while let Ok(event) = rx.try_recv() {
        match event {
            UserEvent::Order(order) => {
                println!(
                    "ws order: status={:?} side={:?} executed_qty={:?} user_order_id={:?}",
                    order.status, order.side, order.executed_qty, order.user_order_id
                );
                order_events += 1;
            }
            UserEvent::Asset(asset) => {
                println!("ws asset: {asset:?}");
                asset_events += 1;
            }
        }
    }
    stream.abort();
    println!("user events: {order_events} MYORDER, {asset_events} MYASSET");
    assert!(order_events >= 2, "expected MYORDER events for both legs, got {order_events}");
    assert!(asset_events >= 1, "expected at least one MYASSET event, got {asset_events}");
}

/// Orders the venue (or the capsule, locally) must refuse. Nothing rests.
#[tokio::test]
#[ignore = "hits the live venue (rejections only)"]
async fn live_rejections() {
    let coinone = connect_trading().await;
    let info = coinone.market_info();
    let mid = coinone.mid_price().await.expect("mid_price");
    let deep_px = mid * 0.5;

    // Size that truncates to zero → local error, no network.
    let err = coinone.place_limit(Side::Buy, 0.0, deep_px).await.unwrap_err();
    println!("zero size -> {err}");

    // Negative price → local error.
    let err = coinone.place_limit(Side::Buy, 1.0, -1.0).await.unwrap_err();
    println!("negative price -> {err}");

    // Below the venue minimum value → refused client-side.
    let tiny_qty = info.min_order_amount * 0.2 / deep_px;
    let err = coinone.place_limit(Side::Buy, tiny_qty, deep_px).await.unwrap_err();
    println!("below minimum -> {err}");
    assert!(err.to_string().contains("minimum"), "{err}");

    // Unknown client order id: the order is simply not known.
    assert!(coinone.fetch_order(1).await.expect("fetch_order query").is_none());

    // Read-only capsules refuse signed actions.
    install_crypto();
    let watcher = Coinone::connect(&live_market()).await.expect("connect (read-only)");
    let err = watcher.place_limit(Side::Buy, 1.0, deep_px).await.unwrap_err();
    println!("read-only place -> {err}");
    assert!(err.to_string().contains("read-only"), "{err}");
    let err = watcher.balances().await.unwrap_err();
    println!("read-only balances -> {err}");
}
