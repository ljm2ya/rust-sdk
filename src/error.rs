//! Unified error type for the wrapper.

/// Errors returned by every [`crate::OrderbookTrader`] implementation.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Missing/invalid configuration (env vars, keys, hosts).
    #[error("configuration error: {0}")]
    Config(String),

    /// The caller supplied an invalid request (bad price/size/market id).
    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// The operation is not supported by the venue (e.g. FOK on Pascal).
    #[error("unsupported operation: {0}")]
    Unsupported(String),

    /// The referenced order does not exist (or is no longer visible).
    #[error("order not found: {0}")]
    OrderNotFound(String),

    /// The venue rejected the request with a structured error.
    #[error("venue error {code}: {message}")]
    Venue { code: String, message: String },

    /// HTTP transport failure.
    #[error("transport error: {0}")]
    Transport(String),

    /// WebSocket failure.
    #[error("websocket error: {0}")]
    Ws(String),

    /// (De)serialization failure.
    #[error("serde error: {0}")]
    Serde(String),

    /// The venue asked us to slow down (HTTP 429 / `RATE_LIMITED`).
    /// Back off before retrying.
    #[error("rate limited: {0}")]
    RateLimited(String),

    /// Error bubbled up from the Polymarket SDK.
    #[error(transparent)]
    Polymarket(polymarket_client_sdk_v2::error::Error),
}

impl From<polymarket_client_sdk_v2::error::Error> for Error {
    fn from(err: polymarket_client_sdk_v2::error::Error) -> Self {
        // Surface HTTP 429 as the venue-agnostic rate-limit error.
        if let Some(status) = err.downcast_ref::<polymarket_client_sdk_v2::error::Status>()
            && status.status_code == polymarket_client_sdk_v2::error::StatusCode::TOO_MANY_REQUESTS
        {
            return Self::RateLimited(format!("polymarket: {}", status.message));
        }
        Self::Polymarket(err)
    }
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Self::Transport(err.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Self::Serde(err.to_string())
    }
}

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, Error>;
