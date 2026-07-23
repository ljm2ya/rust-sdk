//! Live Binance verification: managed depth streaming (public data) and
//! read-only signed trade-WS calls. No orders are ever placed.
//!
//! ```bash
//! cargo test --test binance_live -- --ignored --nocapture --test-threads=1
//! ```

use rusk_sdk::binance::{self, BinanceFutures};
use rusk_sdk::book::BookUpdate;

const SYMBOL: &str = "CLUSDT";

fn setup() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    dotenvy::dotenv().ok();
}

/// The managed stream must deliver a synced, uncrossed book and keep it in
/// sync across several depth diffs.
#[tokio::test]
#[ignore = "hits live Binance market data"]
async fn live_stream_book_syncs_and_applies_diffs() {
    setup();
    let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
    let handle = tokio::spawn(async move { binance::stream_book(SYMBOL, None, tx).await });

    let mut books = 0;
    while books < 20 {
        let update = tokio::time::timeout(std::time::Duration::from_secs(30), rx.recv())
            .await
            .expect("stream produced updates within 30s")
            .expect("stream alive");
        match update {
            BookUpdate::Notice(notice) => println!("notice: {notice}"),
            BookUpdate::Book(book) => {
                // The seeded REST book is unsynced until the first
                // overlapping diff applies (Binance sync rules).
                if !book.is_synced() {
                    assert_eq!(books, 0, "book must stay synced after the initial sync");
                    println!("seed book (unsynced yet): bid={:?} ask={:?}", book.best_bid(), book.best_ask());
                    continue;
                }
                books += 1;
                let (bid, ask) = (book.best_bid().unwrap(), book.best_ask().unwrap());
                assert!(bid < ask, "book must not be crossed: bid={bid} ask={ask}");
                if books % 10 == 0 || books == 1 {
                    println!("book #{books}: bid={bid} ask={ask} mid={:?}", book.mid());
                }
            }
        }
    }
    handle.abort();
}

/// Signed trade-WS round trip without trading: position lookup plus the
/// venue's rejection of an unknown order id.
#[tokio::test]
#[ignore = "signed read-only calls; needs BINANCE_API_KEY/SECRET"]
async fn live_trade_ws_reads_position_and_rejects_unknown_order() {
    setup();
    let mut client = BinanceFutures::connect(SYMBOL).await.expect("trade WS connect");
    let position = client.position_amt().await.expect("position_amt");
    println!("{SYMBOL} position_amt = {position}");

    let err = client.order_status(1).await.unwrap_err();
    println!("order_status(1) -> {err}");
    assert!(err.to_string().contains("-2013"), "expected unknown-order error, got {err}");
}
