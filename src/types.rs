//! Venue-agnostic core types for orderbook trading.

use std::fmt;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Opaque market identifier.
///
/// - Polymarket: the outcome token id (decimal `U256` string), e.g. `"7132107..."`.
/// - Pascal: the market symbol, e.g. `"ME_SEN_2026.REP"`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MarketId(pub String);

impl MarketId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MarketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for MarketId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

/// Order side. `Buy` maps to `BID` on Pascal and `BUY` on Polymarket.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

/// Time in force.
///
/// Venue mapping:
///
/// | Variant | Polymarket | Pascal |
/// |---------|------------|--------|
/// | `Gtc`   | `GTC`      | `GTC`  |
/// | `Gtd`   | `GTD`      | `GTT`  |
/// | `Ioc`   | `FAK`      | `IOC`  |
/// | `Fok`   | `FOK`      | unsupported |
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeInForce {
    Gtc,
    Gtd { expires_at_ms: i64 },
    Ioc,
    Fok,
}

/// Order type: resting limit order or immediate market order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    Limit,
    Market,
}

/// Input to [`crate::OrderbookTrader::place_order`].
#[derive(Clone, Debug, PartialEq)]
pub struct PlaceOrderRequest {
    pub market: MarketId,
    pub side: Side,
    pub order_type: OrderType,
    /// Limit price in dollars (0.000001..=1.0). Required for `OrderType::Limit`,
    /// ignored for `OrderType::Market`.
    pub price: Option<Decimal>,
    /// Size in contracts/shares. Pascal only accepts whole contracts.
    pub size: Decimal,
    pub tif: TimeInForce,
    /// Maker-only. Rejected instead of crossing.
    pub post_only: bool,
}

impl PlaceOrderRequest {
    /// Convenience constructor for a GTC limit order.
    pub fn limit(market: MarketId, side: Side, price: Decimal, size: Decimal) -> Self {
        Self {
            market,
            side,
            order_type: OrderType::Limit,
            price: Some(price),
            size,
            tif: TimeInForce::Gtc,
            post_only: false,
        }
    }

    /// Convenience constructor for a market (IOC) order.
    pub fn market(market: MarketId, side: Side, size: Decimal) -> Self {
        Self {
            market,
            side,
            order_type: OrderType::Market,
            price: None,
            size,
            tif: TimeInForce::Ioc,
            post_only: false,
        }
    }
}

/// Normalized order status.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderStatus {
    Open,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
    Unknown,
}

/// Normalized order snapshot returned by place/fetch/modify.
#[derive(Clone, Debug, PartialEq)]
pub struct Order {
    /// Exchange-assigned order id.
    pub order_id: String,
    pub status: OrderStatus,
    pub filled_size: Decimal,
    pub remaining_size: Decimal,
}

/// Normalized position: `size` is signed (positive = long, negative = short).
///
/// Note: Polymarket positions are always non-negative token holdings ("short YES"
/// is a long position in the opposite token).
#[derive(Clone, Debug, PartialEq)]
pub struct Position {
    pub market: MarketId,
    pub size: Decimal,
    pub avg_price: Option<Decimal>,
}

/// One price level of an orderbook.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Level {
    pub price: Decimal,
    pub size: Decimal,
}

/// Full L2 orderbook snapshot delivered on every update.
#[derive(Clone, Debug, PartialEq)]
pub struct Orderbook {
    pub market: MarketId,
    /// Sorted best (highest) price first.
    pub bids: Vec<Level>,
    /// Sorted best (lowest) price first.
    pub asks: Vec<Level>,
    /// **Event time**: venue timestamp of the last book *change*, in Unix ms
    /// (0 when the venue omits it). On a quiet market this legitimately ages —
    /// it is NOT a liveness signal.
    pub timestamp_ms: i64,
    /// **Receive time**: local wall clock when this snapshot arrived over WS.
    /// Use this for freshness/staleness monitoring; while updates keep
    /// arriving the book is current even if `timestamp_ms` is old.
    pub received_at_ms: i64,
}

impl Orderbook {
    /// Size-weighted midpoint ("microprice") of the top of book: each side's
    /// price is weighted by the *opposite* side's size, so the side with more
    /// resting size pulls the price toward its own — and, symmetrically, the
    /// bigger side's own price contributes less (multiplied by the smaller,
    /// opposing size) than the thinner side's price does.
    ///
    /// `(bid_price * ask_size + ask_price * bid_size) / (bid_size + ask_size)`
    ///
    /// `None` if either side is empty or total top-of-book size is zero.
    pub fn weighted_midprice(&self) -> Option<Decimal> {
        let bid = self.bids.first()?;
        let ask = self.asks.first()?;
        let total_size = bid.size + ask.size;
        if total_size == Decimal::ZERO {
            return None;
        }
        Some((bid.price * ask.size + ask.price * bid.size) / total_size)
    }
}

/// Local wall clock in Unix milliseconds.
pub fn unix_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_millis() as i64
}

/// Derive a normalized [`OrderStatus`] from fill accounting shared by both venues.
///
/// `is_open` means the order still has live remaining size on the book.
pub fn status_from_fills(
    is_open: bool,
    filled: Decimal,
    original: Decimal,
) -> OrderStatus {
    if is_open {
        if filled > Decimal::ZERO {
            OrderStatus::PartiallyFilled
        } else {
            OrderStatus::Open
        }
    } else if filled == original && filled > Decimal::ZERO {
        OrderStatus::Filled
    } else {
        OrderStatus::Canceled
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn market_id_display_roundtrip() {
        let id = MarketId::from("ME_SEN_2026.REP");
        assert_eq!(id.to_string(), "ME_SEN_2026.REP");
        assert_eq!(id.as_str(), "ME_SEN_2026.REP");
    }

    #[test]
    fn limit_helper_sets_expected_defaults() {
        let req = PlaceOrderRequest::limit("M".into(), Side::Buy, dec!(0.55), dec!(10));
        assert_eq!(req.order_type, OrderType::Limit);
        assert_eq!(req.tif, TimeInForce::Gtc);
        assert_eq!(req.price, Some(dec!(0.55)));
        assert!(!req.post_only);
    }

    #[test]
    fn market_helper_uses_ioc() {
        let req = PlaceOrderRequest::market("M".into(), Side::Sell, dec!(3));
        assert_eq!(req.order_type, OrderType::Market);
        assert_eq!(req.tif, TimeInForce::Ioc);
        assert_eq!(req.price, None);
    }

    #[test]
    fn status_from_fills_covers_lifecycle() {
        assert_eq!(
            status_from_fills(true, dec!(0), dec!(10)),
            OrderStatus::Open
        );
        assert_eq!(
            status_from_fills(true, dec!(3), dec!(10)),
            OrderStatus::PartiallyFilled
        );
        assert_eq!(
            status_from_fills(false, dec!(10), dec!(10)),
            OrderStatus::Filled
        );
        // Canceled after a partial fill is Canceled, not Filled.
        assert_eq!(
            status_from_fills(false, dec!(3), dec!(10)),
            OrderStatus::Canceled
        );
        assert_eq!(
            status_from_fills(false, dec!(0), dec!(10)),
            OrderStatus::Canceled
        );
    }

    fn book(bids: Vec<(Decimal, Decimal)>, asks: Vec<(Decimal, Decimal)>) -> Orderbook {
        let level = |(price, size)| Level { price, size };
        Orderbook {
            market: MarketId::from("M"),
            bids: bids.into_iter().map(level).collect(),
            asks: asks.into_iter().map(level).collect(),
            timestamp_ms: 0,
            received_at_ms: 0,
        }
    }

    #[test]
    fn weighted_midprice_weights_toward_bigger_side() {
        // Balanced sizes -> plain midpoint.
        let balanced = book(vec![(dec!(0.48), dec!(100))], vec![(dec!(0.49), dec!(100))]);
        assert_eq!(balanced.weighted_midprice(), Some(dec!(0.485)));

        // Bid side 3x bigger -> price pulled toward the ask (the bigger
        // side's own price gets the smaller multiplier).
        let bid_heavy = book(vec![(dec!(0.48), dec!(300))], vec![(dec!(0.49), dec!(100))]);
        assert_eq!(bid_heavy.weighted_midprice(), Some(dec!(0.4875)));

        // Ask side 3x bigger -> pulled toward the bid.
        let ask_heavy = book(vec![(dec!(0.48), dec!(100))], vec![(dec!(0.49), dec!(300))]);
        assert_eq!(ask_heavy.weighted_midprice(), Some(dec!(0.4825)));
    }

    #[test]
    fn weighted_midprice_requires_both_sides_and_nonzero_size() {
        assert_eq!(book(vec![], vec![(dec!(0.49), dec!(100))]).weighted_midprice(), None);
        assert_eq!(book(vec![(dec!(0.48), dec!(100))], vec![]).weighted_midprice(), None);
        assert_eq!(
            book(vec![(dec!(0.48), dec!(0))], vec![(dec!(0.49), dec!(0))]).weighted_midprice(),
            None
        );
    }
}
