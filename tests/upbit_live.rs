//! **Real order trading tests** — verification of the `Upbit` capsule
//! against the LIVE exchange with your credentials.
//!
//! Ignored by default so a bare `cargo test` can never trade; run explicitly:
//!
//! ```bash
//! cargo test --test upbit_live -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Credentials come from the environment or `.env` (`UPBIT_ACCESS_KEY` /
//! `UPBIT_SECRET_KEY`; the API key must whitelist this machine's IP).
//! The target market defaults to `KRW-BTC`; override with `RUSK_UPBIT_MARKET`.
//!
//! Safety posture:
//! - `live_order_roundtrip` only rests a deep bid at ~50% of mid, sized just
//!   above Upbit's 5,000 KRW minimum (never fills), exercises
//!   place/open_orders/fetch/cancel, and always cleans up after itself.
//! - `live_rejections` only sends orders the venue/capsule must refuse.
//! - No test ever fills or transfers.
//! - Run with `--test-threads=1`; never run in CI.

use rusk_sdk::book::BookUpdate;
use rusk_sdk::order::{OrderState, Side};
use rusk_sdk::upbit::{Upbit, UserEvent, stream_book, stream_user_events};

fn live_market() -> String {
    std::env::var("RUSK_UPBIT_MARKET").unwrap_or_else(|_| "KRW-BTC".to_string())
}

fn install_crypto() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    dotenvy::dotenv().ok();
}

async fn connect_trading() -> Upbit {
    install_crypto();
    let upbit = Upbit::connect_trading(&live_market())
        .await
        .expect("connect_trading (needs UPBIT_ACCESS_KEY / UPBIT_SECRET_KEY)");
    println!("connected: market={}", upbit.market());
    upbit
}

/// Poll `fetch_order` until visible and satisfying `done`.
async fn poll_order(
    upbit: &Upbit,
    order_id: u64,
    done: impl Fn(&rusk_sdk::order::OrderInfo) -> bool,
) -> rusk_sdk::order::OrderInfo {
    const ATTEMPTS: u32 = 10;
    const DELAY: std::time::Duration = std::time::Duration::from_millis(1000);
    let mut last = None;
    for _ in 0..ATTEMPTS {
        if let Some(info) = upbit.fetch_order(order_id).await.expect("fetch_order") {
            if done(&info) {
                return info;
            }
            last = Some(info);
        }
        tokio::time::sleep(DELAY).await;
    }
    panic!("order {order_id} did not converge after {ATTEMPTS} attempts: {last:?}");
}

/// Public quotation data: ticker, REST book, and mid must be coherent.
#[tokio::test]
#[ignore = "hits live Upbit market data (public, no credentials)"]
async fn live_ticker_and_book() {
    install_crypto();

    let upbit = Upbit::connect(&live_market()).await.expect("connect (read-only)");
    let ticker = upbit.ticker().await.expect("ticker");
    println!("ticker: {ticker:?}");
    assert_eq!(ticker.market, live_market().to_uppercase());
    assert!(ticker.trade_price > 0.0);

    let book = upbit.book().await.expect("book");
    let (bid, ask) = (book.best_bid().expect("bid side"), book.best_ask().expect("ask side"));
    println!("REST book top: bid={bid} ask={ask}");
    assert!(bid < ask, "book is crossed: {bid} >= {ask}");
    let mid = upbit.mid_price().await.expect("mid_price");
    assert!((mid - (bid + ask) / 2.0).abs() / mid < 0.01);

    let trades = upbit.recent_trades(5).await.expect("recent_trades");
    println!("recent trades: {}", trades.len());
    assert!(!trades.is_empty());
    let candles = upbit.minute_candles(1, 3).await.expect("minute_candles");
    assert_eq!(candles.len(), 3);

    // Unknown market must be rejected at connect.
    let err = Upbit::connect("KRW-RUSKNOSUCH").await.unwrap_err();
    println!("unknown market -> {err:#}");
}

/// Public WS book stream keeps emitting coherent snapshots.
#[tokio::test]
#[ignore = "hits live Upbit market data (public, no credentials)"]
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
#[ignore = "hits live Upbit account data (needs credentials)"]
async fn live_account_readonly() {
    let upbit = connect_trading().await;

    let balances = upbit.balances().await.expect("balances");
    println!("balances: {balances:?}");
    for balance in &balances {
        assert!(balance.balance >= 0.0 && balance.locked >= 0.0, "{balance:?}");
    }

    let quote = upbit.balance(upbit.quote()).await.expect("balance");
    println!("{} balance: {quote:?}", upbit.quote());

    let (bid_fee, ask_fee) = upbit.order_chance_fees().await.expect("order_chance_fees");
    println!("fees: bid={bid_fee} ask={ask_fee}");
    assert!((0.0..0.1).contains(&bid_fee) && (0.0..0.1).contains(&ask_fee));
}

/// Deep resting bid → open_orders → fetch_order → cancel_order → fetch shows
/// Canceled. Nothing ever fills.
#[tokio::test]
#[ignore = "places a real (deep, non-filling) ~5500 KRW order on Upbit"]
async fn live_order_roundtrip() {
    let upbit = connect_trading().await;

    let mid = upbit.mid_price().await.expect("mid_price");
    // Deep bid at ~50% of mid; size just above the 5,000 KRW venue minimum.
    let deep_px = mid * 0.5;
    let qty = 5_500.0 / deep_px;
    println!("placing deep bid: qty={qty} @ ~{deep_px:.4} (mid {mid})");

    let order = upbit.place_limit(Side::Buy, qty, deep_px).await.expect("place_limit");
    println!("placed: {order:?}");
    assert!(order.order_id > 0);
    assert_eq!(order.side, Side::Buy);

    let resting = poll_order(&upbit, order.order_id, |info| info.state == OrderState::Open).await;
    println!("resting: {resting:?}");
    assert_eq!(resting.side, Side::Buy);
    assert_eq!(resting.filled_qty, 0.0);
    assert!(resting.limit_px <= deep_px * 1.001);

    let open = upbit.open_orders().await.expect("open_orders");
    assert!(open.iter().any(|o| o.order_id == order.order_id), "{open:?}");

    upbit.cancel_order(order.order_id).await.expect("cancel_order");
    let closed = poll_order(&upbit, order.order_id, |info| info.state == OrderState::Canceled).await;
    println!("after cancel: {closed:?}");

    let leftovers = upbit.open_orders().await.expect("open_orders");
    assert!(
        !leftovers.iter().any(|o| o.order_id == order.order_id),
        "no leftovers from this test: {leftovers:?}"
    );
    println!("roundtrip complete, book clean");
}

/// Poll the free balance of `currency` until `done(balance)`.
async fn poll_balance(upbit: &Upbit, currency: &str, done: impl Fn(f64) -> bool) -> f64 {
    const ATTEMPTS: u32 = 15;
    const DELAY: std::time::Duration = std::time::Duration::from_millis(1000);
    let mut last = 0.0;
    for _ in 0..ATTEMPTS {
        last = upbit
            .balance(currency)
            .await
            .expect("balance")
            .map(|b| b.balance)
            .unwrap_or(0.0);
        if done(last) {
            return last;
        }
        tokio::time::sleep(DELAY).await;
    }
    panic!("{currency} balance did not converge after {ATTEMPTS} attempts: {last}");
}

/// Market buy (~6,000 KRW, REALLY fills) → base balance reflects it →
/// market sell of exactly the received amount restores the position — with
/// the private WS (`myOrder`/`myAsset`) streaming throughout, so both fills
/// must show up as user events.
///
/// Bounded cost: taker fee (0.05%) on both legs plus the spread — a few KRW.
#[tokio::test]
#[ignore = "REALLY fills a ~6,000 KRW market order on Upbit, then flattens it (also verifies the private WS)"]
async fn live_market_fill_and_flatten_with_user_events() {
    let upbit = connect_trading().await;
    let base = upbit.base().to_string();

    // Private WS up before trading so the subscription catches the fills.
    let (tx, mut rx) = tokio::sync::mpsc::channel(256);
    let stream = tokio::spawn(async move { stream_user_events(tx).await });
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert!(!stream.is_finished(), "private WS died before trading started");

    let start = upbit
        .balance(&base)
        .await
        .expect("balance")
        .map(|b| b.balance)
        .unwrap_or(0.0);
    println!("starting {base} balance: {start}");

    // Open leg: market buy by quote total.
    let buy = upbit.market_buy(6_000.0).await.expect("market_buy");
    println!("market buy: {buy:?}");
    let after_buy = poll_balance(&upbit, &base, |bal| bal > start + 1e-12).await;
    let bought = after_buy - start;
    println!("filled: +{bought} {base}");

    // Upbit finishes ord_type=price orders as done OR cancel (leftover quote
    // dust is auto-cancelled) — either way the executed volume must be there.
    let info = upbit
        .fetch_order(buy.order_id)
        .await
        .expect("fetch_order")
        .expect("buy order visible");
    println!("buy order: {info:?}");
    assert_ne!(info.state, OrderState::Open);
    assert!(info.filled_qty > 0.0, "{info:?}");

    // Close leg: market sell of exactly what the buy leg received.
    let sell = upbit.market_sell(bought).await.expect("market_sell");
    println!("market sell: {sell:?}");
    let flat = poll_balance(&upbit, &base, |bal| bal <= start + bought * 0.01).await;
    println!("flattened: {base} balance {flat} (start {start})");

    let sell_info = upbit
        .fetch_order(sell.order_id)
        .await
        .expect("fetch_order")
        .expect("sell order visible");
    println!("sell order: {sell_info:?}");
    assert_eq!(sell_info.state, OrderState::Filled);
    assert!(sell_info.filled_qty > 0.0);

    // Both legs must have produced private WS traffic.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let (mut order_events, mut asset_events) = (0, 0);
    while let Ok(event) = rx.try_recv() {
        match event {
            UserEvent::Order(order) => {
                println!(
                    "ws order: code={} state={} ask_bid={} volume={:?} executed={:?} identifier={:?}",
                    order.code, order.state, order.ask_bid, order.volume, order.executed_volume,
                    order.identifier
                );
                assert_eq!(order.code, upbit.market());
                order_events += 1;
            }
            UserEvent::Asset(asset) => {
                println!("ws asset: {asset:?}");
                asset_events += 1;
            }
        }
    }
    stream.abort();
    println!("user events: {order_events} myOrder, {asset_events} myAsset");
    assert!(order_events >= 2, "expected myOrder events for both legs, got {order_events}");
    assert!(asset_events >= 1, "expected at least one myAsset event, got {asset_events}");
}

/// Orders the venue (or the capsule, locally) must refuse. Nothing rests.
#[tokio::test]
#[ignore = "hits the live venue (rejections only)"]
async fn live_rejections() {
    let upbit = connect_trading().await;
    let mid = upbit.mid_price().await.expect("mid_price");
    let deep_px = mid * 0.5;

    // Size that truncates to zero → local error, no network.
    let err = upbit.place_limit(Side::Buy, 0.0, deep_px).await.unwrap_err();
    println!("zero size -> {err}");

    // Negative price → local error.
    let err = upbit.place_limit(Side::Buy, 1.0, -1.0).await.unwrap_err();
    println!("negative price -> {err}");

    // Below the 5,000 KRW minimum → refused client-side (never hits the wire).
    if upbit.quote() == "KRW" {
        let err = upbit.place_limit(Side::Buy, 1_000.0 / deep_px, deep_px).await.unwrap_err();
        println!("below minimum -> {err}");
        assert!(err.to_string().contains("minimum"), "{err}");
    }

    // Unknown client order id: the order is simply not known.
    assert!(upbit.fetch_order(1).await.expect("fetch_order query").is_none());

    // Read-only capsules refuse signed actions.
    install_crypto();
    let watcher = Upbit::connect(&live_market()).await.expect("connect (read-only)");
    let err = watcher.place_limit(Side::Buy, 1.0, deep_px).await.unwrap_err();
    println!("read-only place -> {err}");
    assert!(err.to_string().contains("read-only"), "{err}");
    let err = watcher.balances().await.unwrap_err();
    println!("read-only balances -> {err}");
}
