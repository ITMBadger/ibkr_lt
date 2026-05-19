from __future__ import annotations


class TradeFrameError(Exception):
    """Base exception for all tradeframe errors."""


class ConfigError(TradeFrameError):
    """Invalid or missing configuration."""


class BrokerError(TradeFrameError):
    """Broker adapter error (connection, order rejection, etc.)."""


class DataError(TradeFrameError):
    """Data provider error (missing bars, gap, stale feed, etc.)."""


class StrategyError(TradeFrameError):
    """Strategy registration or execution error."""
