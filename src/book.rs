//! Local orderbook management, extracted from mondayfuck.
//!
//! [`OrderBook`] keeps both sides as price-keyed maps (fixed-point keys at
//! 1e-6 resolution) and supports the two live-sync styles the venue streams
//! use:
//!
//! - **Snapshot + sequenced diffs** (Binance depth streams):
//!   [`OrderBook::load_levels`] then [`OrderBook::apply`], which implements
//!   Binance's documented sync rules (initial-sync window, gap detection via
//!   `prev_final_id`).
//! - **Full snapshots / overlays** (Hyperliquid L2 feeds): rebuilt maps via
//!   [`OrderBook::from_sides`].
//!
//! The venue streaming methods own an `OrderBook` internally and emit
//! managed clones, so consumers never re-implement sync logic.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::order::Side;

/// A sequenced depth diff (Binance `@depth` stream shape).
#[derive(Debug, Clone)]
pub struct DepthEvent {
    pub first_id: u64,
    pub final_id: u64,
    pub prev_final_id: Option<u64>,
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
}

/// Serializable book view for dashboards (`levels` per side, best-first bids,
/// asks listed high→low as the dashboards expect).
#[derive(Debug, Clone, serde::Serialize)]
pub struct BookSnapshot {
    pub synced: bool,
    pub last_update_id: u64,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub mid: Option<f64>,
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
}

/// An update from a managed book stream.
#[derive(Debug, Clone)]
pub enum BookUpdate {
    /// The book after applying an event / reload.
    Book(OrderBook),
    /// A human-readable stream notice (gap detected, resync failed, ...).
    Notice(String),
}

#[derive(Default, Clone, Debug)]
pub struct OrderBook {
    bids: BTreeMap<i64, f64>,
    asks: BTreeMap<i64, f64>,
    last_update_id: u64,
    last_stream_u: Option<u64>,
    synced: bool,
}

impl OrderBook {
    /// Build a synced book from complete side levels (full-snapshot feeds).
    pub fn from_sides(bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>) -> Self {
        let mut book = Self::default();
        book.load_levels(0, bids, asks, true);
        book
    }

    /// Replace the whole book with snapshot levels.
    pub fn load_levels(
        &mut self,
        last_update_id: u64,
        bids: Vec<(f64, f64)>,
        asks: Vec<(f64, f64)>,
        synced: bool,
    ) {
        self.bids.clear();
        self.asks.clear();
        for (price, qty) in bids {
            set_level(&mut self.bids, price, qty);
        }
        for (price, qty) in asks {
            set_level(&mut self.asks, price, qty);
        }
        self.last_update_id = last_update_id;
        self.last_stream_u = None;
        self.synced = synced;
    }

    /// `(last_update_id, last_stream_u)` for gap diagnostics.
    pub fn sequence(&self) -> (u64, Option<u64>) {
        (self.last_update_id, self.last_stream_u)
    }

    pub fn mark_unsynced(&mut self) {
        self.synced = false;
    }

    pub fn is_synced(&self) -> bool {
        self.synced
    }

    /// Apply a sequenced diff. Returns `false` on a sequence gap (caller —
    /// normally the stream — must reload a snapshot).
    pub fn apply(&mut self, event: DepthEvent) -> bool {
        if !self.synced {
            let next_update_id = self.last_update_id + 1;
            if event.final_id < next_update_id {
                return true;
            }
            // Two valid "first event" forms: the spot-style overlap
            // (U <= lastUpdateId+1 <= u) and the futures `pu` chain, where the
            // event's previous-final id IS the snapshot's lastUpdateId even
            // though its U is far beyond it (ids between are non-depth
            // updates). Rejecting the latter made every resync a gap.
            let chained = event.prev_final_id == Some(self.last_update_id);
            if !chained && event.first_id > next_update_id {
                return false;
            }
            self.apply_levels(event.bids, event.asks);
            self.last_stream_u = Some(event.final_id);
            self.synced = true;
            return true;
        }

        if event.prev_final_id != self.last_stream_u {
            return false;
        }
        self.apply_levels(event.bids, event.asks);
        self.last_stream_u = Some(event.final_id);
        true
    }

    fn apply_levels(&mut self, bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>) {
        for (price, qty) in bids {
            set_level(&mut self.bids, price, qty);
        }
        for (price, qty) in asks {
            set_level(&mut self.asks, price, qty);
        }
    }

    pub fn best_bid(&self) -> Option<f64> {
        self.bids.keys().next_back().map(|p| key_to_price(*p))
    }

    pub fn best_ask(&self) -> Option<f64> {
        self.asks.keys().next().map(|p| key_to_price(*p))
    }

    /// Best bid as (price, qty).
    pub fn best_bid_level(&self) -> Option<(f64, f64)> {
        self.bids.iter().next_back().map(|(p, q)| (key_to_price(*p), *q))
    }

    /// Best ask as (price, qty).
    pub fn best_ask_level(&self) -> Option<(f64, f64)> {
        self.asks.iter().next().map(|(p, q)| (key_to_price(*p), *q))
    }

    pub fn mid(&self) -> Option<f64> {
        Some((self.best_bid()? + self.best_ask()?) / 2.0)
    }

    /// Total quote value (price × qty) available up to `limit_price` on the
    /// side a `side` order would take from.
    pub fn aggregate_quote(&self, side: Side, limit_price: f64) -> f64 {
        let limit_key = price_key(limit_price);
        match side {
            Side::Buy => self
                .asks
                .range(..=limit_key)
                .map(|(price, qty)| key_to_price(*price) * qty)
                .sum(),
            Side::Sell => self
                .bids
                .range(limit_key..)
                .map(|(price, qty)| key_to_price(*price) * qty)
                .sum(),
        }
    }

    /// Best-first level lists (bids high→low, asks low→high).
    pub fn levels(&self, depth: usize) -> (Vec<(f64, f64)>, Vec<(f64, f64)>) {
        (
            self.bids.iter().rev().take(depth).map(|(p, q)| (key_to_price(*p), *q)).collect(),
            self.asks.iter().take(depth).map(|(p, q)| (key_to_price(*p), *q)).collect(),
        )
    }

    /// Dashboard view (bids best-first; asks listed high→low, matching the
    /// original mondayfuck dashboard JSON).
    pub fn snapshot(&self, levels: usize) -> BookSnapshot {
        BookSnapshot {
            synced: self.synced,
            last_update_id: self.last_update_id,
            best_bid: self.best_bid(),
            best_ask: self.best_ask(),
            mid: self.mid(),
            bids: self
                .bids
                .iter()
                .rev()
                .take(levels)
                .map(|(p, q)| (key_to_price(*p), *q))
                .collect(),
            asks: self
                .asks
                .iter()
                .take(levels)
                .rev()
                .map(|(p, q)| (key_to_price(*p), *q))
                .collect(),
        }
    }
}

/// Overlay `source` onto `book`: within `source`'s price range the source is
/// authoritative (stale levels removed), outside it `book` is kept.
pub(crate) fn overlay_source(book: &mut BTreeMap<i64, f64>, source: &BTreeMap<i64, f64>) {
    let (Some(first), Some(last)) = (source.keys().next(), source.keys().next_back()) else {
        return;
    };
    let stale_keys = book.range(*first..=*last).map(|(price, _)| *price).collect::<Vec<_>>();
    for price in stale_keys {
        book.remove(&price);
    }
    book.extend(source.iter().map(|(price, qty)| (*price, *qty)));
}

pub(crate) fn levels_to_map(levels: Vec<(f64, f64)>) -> BTreeMap<i64, f64> {
    levels
        .into_iter()
        .filter(|(_, qty)| *qty != 0.0)
        .map(|(price, qty)| (price_key(price), qty))
        .collect()
}

fn set_level(side: &mut BTreeMap<i64, f64>, price: f64, qty: f64) {
    let key = price_key(price);
    if qty == 0.0 {
        side.remove(&key);
    } else {
        side.insert(key, qty);
    }
}

pub(crate) fn price_key(price: f64) -> i64 {
    (price * 1_000_000.0).round() as i64
}

pub(crate) fn key_to_price(key: i64) -> f64 {
    key as f64 / 1_000_000.0
}

/// Parse `["price", "qty"]` string levels (Binance wire shape).
pub(crate) fn parse_level(level: &[String]) -> anyhow::Result<(f64, f64)> {
    if level.len() != 2 {
        anyhow::bail!("bad book level: {level:?}");
    }
    Ok((level[0].parse()?, level[1].parse()?))
}

pub(crate) fn parse_levels(levels: Vec<Vec<String>>) -> anyhow::Result<Vec<(f64, f64)>> {
    levels.iter().map(|level| parse_level(level)).collect()
}

/// Binance REST depth snapshot payload.
#[derive(Debug, Deserialize)]
pub struct DepthSnapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    bids: Vec<Vec<String>>,
    asks: Vec<Vec<String>>,
}

impl DepthSnapshot {
    /// Replace `book` with this snapshot (unsynced until the first
    /// overlapping diff is applied, per Binance's sync rules).
    pub fn load_into(&self, book: &mut OrderBook) -> anyhow::Result<()> {
        let bids = self.bids.iter().map(|level| parse_level(level)).collect::<anyhow::Result<Vec<_>>>()?;
        let asks = self.asks.iter().map(|level| parse_level(level)).collect::<anyhow::Result<Vec<_>>>()?;
        book.load_levels(self.last_update_id, bids, asks, false);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(first: u64, last: u64, prev: Option<u64>, bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>) -> DepthEvent {
        DepthEvent { first_id: first, final_id: last, prev_final_id: prev, bids, asks }
    }

    #[test]
    fn syncs_via_futures_pu_chain() {
        let mut book = OrderBook::default();
        book.load_levels(58784, vec![(63.10, 5.0)], vec![(63.20, 7.0)], false);
        // U far past lastUpdateId, but pu == lastUpdateId: valid first event.
        assert!(book.apply(event(65914, 67318, Some(58784), vec![(63.11, 2.0)], vec![])));
        assert!(book.is_synced());
        assert_eq!(book.best_bid(), Some(63.11));
        // Next must chain on u.
        assert!(!book.apply(event(67400, 67500, Some(67319), vec![], vec![])));
    }

    #[test]
    fn syncs_snapshot_plus_diffs_and_detects_gaps() {
        let mut book = OrderBook::default();
        book.load_levels(100, vec![(63.10, 5.0)], vec![(63.20, 7.0)], false);
        assert!(!book.is_synced());

        // Stale events before the snapshot window are dropped harmlessly.
        assert!(book.apply(event(90, 99, None, vec![(1.0, 1.0)], vec![])));
        assert!(!book.is_synced());

        // A diff overlapping lastUpdateId+1 completes the initial sync.
        assert!(book.apply(event(95, 105, None, vec![(63.11, 2.0)], vec![(63.20, 0.0)])));
        assert!(book.is_synced());
        assert_eq!(book.best_bid(), Some(63.11));
        assert_eq!(book.best_ask(), None); // ask removed by qty 0

        // Contiguous diff chains through prev_final_id.
        assert!(book.apply(event(106, 110, Some(105), vec![], vec![(63.25, 1.0)])));
        assert_eq!(book.best_ask(), Some(63.25));
        assert_eq!(book.mid(), Some((63.11 + 63.25) / 2.0));

        // A gap must be rejected (caller reloads a snapshot).
        assert!(!book.apply(event(115, 120, Some(114), vec![], vec![])));

        // Events ahead of the snapshot force a reload too.
        let mut fresh = OrderBook::default();
        fresh.load_levels(100, vec![], vec![], false);
        assert!(!fresh.apply(event(150, 160, None, vec![], vec![])));
    }

    #[test]
    fn aggregates_quote_value_up_to_a_limit_price() {
        let book = OrderBook::from_sides(
            vec![(63.10, 2.0), (63.00, 3.0)],
            vec![(63.20, 1.0), (63.30, 4.0), (63.50, 9.0)],
        );
        assert!(book.is_synced());
        // Buy sweeps asks priced <= limit.
        let buy = book.aggregate_quote(Side::Buy, 63.30);
        assert!((buy - (63.20 * 1.0 + 63.30 * 4.0)).abs() < 1e-9, "{buy}");
        // Sell sweeps bids priced >= limit.
        let sell = book.aggregate_quote(Side::Sell, 63.05);
        assert!((sell - 63.10 * 2.0).abs() < 1e-9, "{sell}");
    }

    #[test]
    fn snapshot_orders_bids_best_first_and_asks_high_to_low() {
        let book = OrderBook::from_sides(
            vec![(63.10, 2.0), (63.00, 3.0), (62.90, 1.0)],
            vec![(63.20, 1.0), (63.30, 4.0), (63.40, 2.0)],
        );
        let snapshot = book.snapshot(2);
        assert_eq!(snapshot.bids, vec![(63.10, 2.0), (63.00, 3.0)]);
        assert_eq!(snapshot.asks, vec![(63.30, 4.0), (63.20, 1.0)]); // best 2, listed high->low
        assert_eq!(snapshot.best_bid, Some(63.10));
        assert_eq!(snapshot.best_ask, Some(63.20));

        let (bids, asks) = book.levels(2);
        assert_eq!(bids, vec![(63.10, 2.0), (63.00, 3.0)]);
        assert_eq!(asks, vec![(63.20, 1.0), (63.30, 4.0)]); // best-first
    }

    #[test]
    fn price_keys_roundtrip_and_zero_levels_are_dropped() {
        for price in [25.135, 0.000001, 123456.789012] {
            assert!((key_to_price(price_key(price)) - price).abs() < 1e-9, "{price}");
        }
        let map = levels_to_map(vec![(25.1, 1.0), (25.2, 0.0), (25.3, 2.0)]);
        assert_eq!(map.len(), 2);
        assert!(!map.contains_key(&price_key(25.2)));
    }

    #[test]
    fn overlays_replace_levels_only_inside_the_source_range() {
        let mut base = levels_to_map(vec![(63.00, 1.0), (63.10, 2.0), (63.20, 3.0), (63.30, 4.0)]);
        let overlay = levels_to_map(vec![(63.10, 9.0), (63.25, 5.0)]);
        overlay_source(&mut base, &overlay);
        // 63.20 fell inside the overlay range and was not re-listed -> gone.
        assert_eq!(base.get(&price_key(63.20)), None);
        assert_eq!(base.get(&price_key(63.10)), Some(&9.0));
        assert_eq!(base.get(&price_key(63.25)), Some(&5.0));
        // Outside the range the base survives.
        assert_eq!(base.get(&price_key(63.00)), Some(&1.0));
        assert_eq!(base.get(&price_key(63.30)), Some(&4.0));
        // Empty overlays change nothing.
        overlay_source(&mut base, &BTreeMap::new());
        assert_eq!(base.len(), 4);
    }
}
