//! Shared trading primitives used by every venue capsule.

/// Order side, shared across venues (`as_str` matches Binance's wire enum).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }

    pub fn is_buy(self) -> bool {
        matches!(self, Side::Buy)
    }
}

/// A live order as this process knows it (id + immutable side/size).
#[derive(Clone, Debug)]
pub struct ActiveOrder {
    pub order_id: u64,
    pub side: Side,
    /// Size after rounding onto the venue's size grid.
    pub qty: f64,
}

/// Normalized order lifecycle state (collapses venue-specific
/// cancellation/rejection flavors).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderState {
    Open,
    Filled,
    Canceled,
    Rejected,
}

/// Snapshot of one order from a venue's order-status endpoint.
#[derive(Clone, Debug)]
pub struct OrderInfo {
    pub order_id: u64,
    pub state: OrderState,
    pub side: Side,
    pub limit_px: f64,
    pub filled_qty: f64,
    pub remaining_qty: f64,
}
