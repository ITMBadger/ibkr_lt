"""Self-contained HTML report card generation for backtest runs."""

from __future__ import annotations

import json
import math
import statistics
import traceback
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from html import escape
from pathlib import Path
from statistics import NormalDist
from typing import Any, Iterable, Sequence
from zoneinfo import ZoneInfo

from core.types import Bar


_EPSILON = 1e-9
_LONG_SIDES = {"long", "buy", "bot", "b"}
_SHORT_SIDES = {"short", "sell", "sld", "s"}
_CARD_BLUE = "#0f5e9c"
_CARD_TEXT = "#243447"
_GRID = "#d9dde3"
_MUTED = "#6b7280"


@dataclass(frozen=True)
class ReportBuildResult:
    """Paths and headline metrics produced by report generation."""

    report_path: Path
    metrics: dict[str, Any]
    warnings: list[str]


@dataclass(frozen=True)
class _InstrumentRef:
    asset_class: str
    symbol: str
    exchange: str | None
    currency: str | None
    expiry: str | None
    strike: str | None
    right: str | None
    multiplier: float

    @property
    def label(self) -> str:
        return self.symbol


@dataclass(frozen=True)
class _FillEvent:
    sequence: int
    timestamp: datetime
    strategy_id: str
    role: str
    trade_id: str | None
    instrument: _InstrumentRef
    side: str
    quantity: float
    price: float
    broker_order_id: str

    @property
    def signed_quantity(self) -> float:
        return _signed_quantity(self.side, self.quantity)


@dataclass
class _OpenLot:
    strategy_id: str
    instrument: _InstrumentRef
    trade_id: str | None
    quantity: float
    entry_price: float
    entry_time: datetime
    entry_sequence: int


@dataclass(frozen=True)
class _ClosedTrade:
    strategy_id: str
    instrument: _InstrumentRef
    trade_id: str | None
    side: str
    quantity: float
    entry_time: datetime
    exit_time: datetime
    entry_price: float
    exit_price: float
    pnl: float
    return_pct: float
    close_result: str


@dataclass(frozen=True)
class _EquityPoint:
    timestamp: datetime
    equity: float
    cash: float
    gross_exposure: float


@dataclass(frozen=True)
class _AccountSeries:
    equity_points: list[_EquityPoint]
    closed_trades: list[_ClosedTrade]
    open_lots: list[_OpenLot]
    monthly_returns: dict[tuple[int, int], float]
    yearly_returns: dict[int, float]
    daily_returns: list[tuple[date, float]]
    max_concurrent_open_lots: int
    warnings: list[str]


def write_html_report(
    *,
    run_dir: Path,
    summary: dict[str, Any],
    replay_bars: Sequence[Bar],
    initial_equity: float,
) -> ReportBuildResult:
    """Write report.html for one completed backtest run."""

    run_dir.mkdir(parents=True, exist_ok=True)
    report_path = run_dir / "report.html"
    fills, load_warnings = _load_fills(run_dir / "fills.jsonl")
    session_tz = _safe_zone(str(summary.get("session_timezone") or "America/New_York"))
    series = _build_account_series(
        fills=fills,
        replay_bars=replay_bars,
        initial_equity=initial_equity,
        session_tz=session_tz,
        warnings=load_warnings,
    )
    metrics = _compute_metrics(series, initial_equity)
    html = _render_report_html(
        summary=summary,
        series=series,
        metrics=metrics,
        initial_equity=initial_equity,
        session_tz=session_tz,
    )
    report_path.write_text(html, encoding="utf-8")
    return ReportBuildResult(
        report_path=report_path,
        metrics=metrics,
        warnings=series.warnings,
    )


def write_report_error(run_dir: Path, exc: BaseException) -> Path:
    """Persist report failure details without disturbing the run summary."""

    path = run_dir / "report_error.txt"
    path.write_text(
        "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
        encoding="utf-8",
    )
    return path


def _load_fills(path: Path) -> tuple[list[_FillEvent], list[str]]:
    warnings: list[str] = []
    if not path.exists():
        return [], [f"{path.name} is missing; report rendered without fill-based PnL."]

    fills: list[_FillEvent] = []
    with path.open("r", encoding="utf-8") as fh:
        for sequence, line in enumerate(fh, start=1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
                fill = event.get("fill") or {}
                instrument = _instrument_from_mapping(fill.get("instrument") or {})
                fills.append(
                    _FillEvent(
                        sequence=sequence,
                        timestamp=_parse_timestamp(fill.get("timestamp")),
                        strategy_id=str(event.get("strategy_id") or "<unknown>"),
                        role=str(event.get("role") or "unknown"),
                        trade_id=_optional_str(event.get("trade_id")),
                        instrument=instrument,
                        side=str(fill.get("side") or ""),
                        quantity=float(fill.get("quantity") or 0.0),
                        price=float(fill.get("price") or 0.0),
                        broker_order_id=str(fill.get("broker_order_id") or ""),
                    )
                )
            except Exception as exc:
                warnings.append(f"Skipped malformed fill line {sequence}: {exc}")
    fills.sort(key=lambda item: (item.timestamp, item.sequence))
    return fills, warnings


def _build_account_series(
    *,
    fills: Sequence[_FillEvent],
    replay_bars: Sequence[Bar],
    initial_equity: float,
    session_tz: ZoneInfo,
    warnings: Sequence[str],
) -> _AccountSeries:
    bar_groups: dict[datetime, list[Bar]] = defaultdict(list)
    fill_groups: dict[datetime, list[_FillEvent]] = defaultdict(list)
    latest_prices: dict[_InstrumentRef, float] = {}
    positions: dict[_InstrumentRef, float] = defaultdict(float)
    open_lots: list[_OpenLot] = []
    closed_trades: list[_ClosedTrade] = []
    report_warnings = list(warnings)
    max_concurrent_open_lots = 0

    for bar in replay_bars:
        bar_groups[_ensure_utc(bar.timestamp)].append(bar)
    for fill in fills:
        fill_groups[fill.timestamp].append(fill)

    cash = float(initial_equity)
    equity_points: list[_EquityPoint] = []
    timestamps = sorted(set(bar_groups) | set(fill_groups))
    if not timestamps:
        now = datetime.now(tz=timezone.utc)
        return _AccountSeries(
            equity_points=[
                _EquityPoint(now, float(initial_equity), float(initial_equity), 0.0)
            ],
            closed_trades=[],
            open_lots=[],
            monthly_returns={},
            yearly_returns={},
            daily_returns=[],
            max_concurrent_open_lots=0,
            warnings=report_warnings,
        )

    for ts in timestamps:
        for bar in bar_groups.get(ts, ()):
            latest_prices[_instrument_from_bar(bar)] = float(bar.close)

        for fill in fill_groups.get(ts, ()):
            signed_qty = fill.signed_quantity
            if abs(signed_qty) <= _EPSILON:
                report_warnings.append(
                    f"Skipped zero-quantity fill at {fill.timestamp.isoformat()}"
                )
                continue
            latest_prices.setdefault(fill.instrument, fill.price)
            cash -= signed_qty * fill.price * fill.instrument.multiplier
            positions[fill.instrument] += signed_qty
            if abs(positions[fill.instrument]) <= _EPSILON:
                positions.pop(fill.instrument, None)
            closed_trades.extend(_apply_fill_to_lots(open_lots, fill, report_warnings))
            max_concurrent_open_lots = max(
                max_concurrent_open_lots,
                sum(1 for lot in open_lots if abs(lot.quantity) > _EPSILON),
            )

        gross_exposure = sum(
            abs(qty) * latest_prices.get(instrument, 0.0) * instrument.multiplier
            for instrument, qty in positions.items()
        )
        equity = cash + sum(
            qty * latest_prices.get(instrument, 0.0) * instrument.multiplier
            for instrument, qty in positions.items()
        )
        equity_points.append(_EquityPoint(ts, equity, cash, gross_exposure))

    daily_returns, monthly_returns, yearly_returns = _period_returns(
        equity_points,
        initial_equity=initial_equity,
        session_tz=session_tz,
    )
    return _AccountSeries(
        equity_points=equity_points,
        closed_trades=closed_trades,
        open_lots=[lot for lot in open_lots if abs(lot.quantity) > _EPSILON],
        monthly_returns=monthly_returns,
        yearly_returns=yearly_returns,
        daily_returns=daily_returns,
        max_concurrent_open_lots=max_concurrent_open_lots,
        warnings=report_warnings,
    )


def _apply_fill_to_lots(
    open_lots: list[_OpenLot],
    fill: _FillEvent,
    warnings: list[str],
) -> list[_ClosedTrade]:
    remaining = fill.signed_quantity
    closed: list[_ClosedTrade] = []

    while abs(remaining) > _EPSILON:
        lot = _select_closing_lot(open_lots, fill, remaining)
        if lot is None:
            break
        close_abs = min(abs(remaining), abs(lot.quantity))
        closing_signed_qty = math.copysign(close_abs, lot.quantity)
        pnl = (
            (fill.price - lot.entry_price)
            * closing_signed_qty
            * lot.instrument.multiplier
        )
        notional = abs(lot.entry_price * closing_signed_qty * lot.instrument.multiplier)
        return_pct = pnl / notional if notional > _EPSILON else 0.0
        closed.append(
            _ClosedTrade(
                strategy_id=lot.strategy_id,
                instrument=lot.instrument,
                trade_id=lot.trade_id,
                side="long" if lot.quantity > 0 else "short",
                quantity=close_abs,
                entry_time=lot.entry_time,
                exit_time=fill.timestamp,
                entry_price=lot.entry_price,
                exit_price=fill.price,
                pnl=pnl,
                return_pct=return_pct,
                close_result=_close_result_label(fill),
            )
        )
        lot.quantity -= closing_signed_qty
        remaining += closing_signed_qty
        if abs(lot.quantity) <= _EPSILON:
            open_lots.remove(lot)

    if abs(remaining) <= _EPSILON:
        return closed

    if fill.role in {"close", "protective_stop"}:
        warnings.append(
            "Close fill had no matching open lot; treating remaining quantity "
            f"as a new open lot. order_id={fill.broker_order_id}"
        )
    _add_open_lot(open_lots, fill, remaining)
    return closed


def _close_result_label(fill: _FillEvent) -> str:
    result = _close_result_key(fill)
    labels = {
        "hold_close": "End of Session",
        "atr_trailing_stop": "ATR Stop",
        "protective_stop": "Protective Stop",
        "stop_loss": "Stop Loss",
        "close": "Close",
    }
    return labels.get(result, result.replace("_", " ").title())


def _close_result_key(fill: _FillEvent) -> str:
    if fill.role == "protective_stop":
        return "protective_stop"

    order_id = fill.broker_order_id
    if not order_id:
        return fill.role or "close"

    suffix = f"-{fill.trade_id}" if fill.trade_id else ""
    stem = order_id[: -len(suffix)] if suffix and order_id.endswith(suffix) else order_id
    marker = "-close-"
    if marker in stem:
        result = stem.rsplit(marker, 1)[1]
        if result:
            return result
    if "-protective-stop-" in stem:
        return "protective_stop"
    return fill.role or "close"


def _select_closing_lot(
    open_lots: Sequence[_OpenLot],
    fill: _FillEvent,
    remaining_signed_qty: float,
) -> _OpenLot | None:
    candidates = [
        lot
        for lot in open_lots
        if lot.strategy_id == fill.strategy_id
        and lot.instrument == fill.instrument
        and lot.quantity * remaining_signed_qty < -_EPSILON
    ]
    if fill.trade_id:
        exact = [lot for lot in candidates if lot.trade_id == fill.trade_id]
        if exact:
            return min(exact, key=lambda lot: lot.entry_sequence)
    if candidates:
        return min(candidates, key=lambda lot: lot.entry_sequence)
    return None


def _add_open_lot(
    open_lots: list[_OpenLot],
    fill: _FillEvent,
    signed_quantity: float,
) -> None:
    if fill.trade_id:
        for lot in open_lots:
            if (
                lot.strategy_id == fill.strategy_id
                and lot.instrument == fill.instrument
                and lot.trade_id == fill.trade_id
                and lot.quantity * signed_quantity > 0
            ):
                old_abs = abs(lot.quantity)
                new_abs = abs(signed_quantity)
                lot.entry_price = (
                    (lot.entry_price * old_abs) + (fill.price * new_abs)
                ) / (old_abs + new_abs)
                lot.quantity += signed_quantity
                lot.entry_time = min(lot.entry_time, fill.timestamp)
                return
    open_lots.append(
        _OpenLot(
            strategy_id=fill.strategy_id,
            instrument=fill.instrument,
            trade_id=fill.trade_id,
            quantity=signed_quantity,
            entry_price=fill.price,
            entry_time=fill.timestamp,
            entry_sequence=fill.sequence,
        )
    )


def _period_returns(
    equity_points: Sequence[_EquityPoint],
    *,
    initial_equity: float,
    session_tz: ZoneInfo,
) -> tuple[list[tuple[date, float]], dict[tuple[int, int], float], dict[int, float]]:
    day_equity: dict[date, float] = {}
    for point in equity_points:
        day = point.timestamp.astimezone(session_tz).date()
        day_equity[day] = point.equity

    daily_returns: list[tuple[date, float]] = []
    previous_equity = initial_equity
    for day in sorted(day_equity):
        equity = day_equity[day]
        daily_returns.append((day, _safe_return(equity, previous_equity)))
        previous_equity = equity

    month_equity: dict[tuple[int, int], float] = {}
    year_equity: dict[int, float] = {}
    for day in sorted(day_equity):
        equity = day_equity[day]
        month_equity[(day.year, day.month)] = equity
        year_equity[day.year] = equity

    monthly_returns: dict[tuple[int, int], float] = {}
    previous_equity = initial_equity
    for month_key in sorted(month_equity):
        equity = month_equity[month_key]
        monthly_returns[month_key] = _safe_return(equity, previous_equity)
        previous_equity = equity

    yearly_returns: dict[int, float] = {}
    previous_equity = initial_equity
    for year in sorted(year_equity):
        equity = year_equity[year]
        yearly_returns[year] = _safe_return(equity, previous_equity)
        previous_equity = equity

    return daily_returns, monthly_returns, yearly_returns


def _compute_drawdown_series(
    equity_points: Sequence[_EquityPoint],
    session_tz: ZoneInfo,
) -> list[tuple[str, float]]:
    if not equity_points:
        return []
    peak = equity_points[0].equity
    result: list[tuple[str, float]] = []
    for point in equity_points:
        peak = max(peak, point.equity)
        dd = point.equity / peak - 1.0 if peak > _EPSILON else 0.0
        result.append((
            point.timestamp.astimezone(session_tz).strftime("%Y-%m-%d %H:%M"),
            dd,
        ))
    return result


def _compute_metrics(series: _AccountSeries, initial_equity: float) -> dict[str, Any]:
    equity_values = [point.equity for point in series.equity_points]
    final_equity = equity_values[-1] if equity_values else initial_equity
    daily = [value for _, value in series.daily_returns]
    monthly = list(series.monthly_returns.values())
    closed = series.closed_trades
    gross_profit = sum(trade.pnl for trade in closed if trade.pnl > 0)
    gross_loss = sum(trade.pnl for trade in closed if trade.pnl < 0)
    winners = sum(1 for trade in closed if trade.pnl > 0)
    positive_years = sum(1 for value in series.yearly_returns.values() if value > 0)
    yearly_count = len(series.yearly_returns)
    max_drawdown = _max_drawdown(equity_values)
    annualized_return = _annualized_return(
        initial_equity=initial_equity,
        final_equity=final_equity,
        start=series.equity_points[0].timestamp if series.equity_points else None,
        end=series.equity_points[-1].timestamp if series.equity_points else None,
    )
    max_exposure_multiple = 0.0
    for point in series.equity_points:
        if abs(point.equity) > _EPSILON:
            max_exposure_multiple = max(
                max_exposure_multiple,
                point.gross_exposure / abs(point.equity),
            )

    return {
        "initial_equity": initial_equity,
        "final_equity": final_equity,
        "strategy_return": _safe_return(final_equity, initial_equity),
        "annualized_return": annualized_return,
        "equity_return": _safe_return(final_equity, initial_equity),
        "closed_trades": len(closed),
        "open_positions": len(series.open_lots),
        "max_concurrent_open_lots": series.max_concurrent_open_lots,
        "max_exposure_multiple": max_exposure_multiple,
        "sum_trade_return": sum(trade.return_pct for trade in closed),
        "win_rate": winners / len(closed) if closed else None,
        "profit_factor": (
            gross_profit / abs(gross_loss)
            if abs(gross_loss) > _EPSILON
            else None
        ),
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "net_pnl": final_equity - initial_equity,
        "max_drawdown": max_drawdown,
        "annualized_sharpe": _annualized_sharpe(daily),
        "sortino": _sortino(daily),
        "positive_years": positive_years,
        "year_count": yearly_count,
        "monthly_observations": len(monthly),
        "daily_observations": len(daily),
    }




# =============================================================================
# HTML RENDERING  (Chart.js + Modern CSS)
# =============================================================================

def _render_report_html(
    *,
    summary: dict[str, Any],
    series: _AccountSeries,
    metrics: dict[str, Any],
    initial_equity: float,
    session_tz: ZoneInfo,
) -> str:
    period_label = _period_label(summary, session_tz)
    generated_at = datetime.now(tz=timezone.utc).astimezone(session_tz)
    strategy_label = ", ".join(str(item) for item in summary.get("strategies") or [])
    if not strategy_label:
        strategy_label = "Backtest"

    # --- Data preparation for charts ---
    equity_labels: list[str] = []
    equity_values: list[float] = []
    for point in series.equity_points:
        equity_labels.append(point.timestamp.astimezone(session_tz).strftime("%Y-%m-%d"))
        equity_values.append(round(point.equity / initial_equity, 4) if initial_equity else 1.0)
    equity_labels, equity_values = _downsample_pairs(list(zip(equity_labels, equity_values)), 900)

    dd_labels: list[str] = []
    dd_values: list[float] = []
    for point in series.equity_points:
        dd_labels.append(point.timestamp.astimezone(session_tz).strftime("%Y-%m-%d"))
    dd_labels, dd_values = _downsample_pairs(list(zip(dd_labels, _drawdown_values(series.equity_points))), 900)

    rolling = _rolling_monthly(series.monthly_returns)
    rolling_labels = [r[0] for r in rolling]
    rolling_rets = [round(r[1], 4) for r in rolling]
    rolling_vols = [round(r[2], 4) for r in rolling]

    yearly_data = [(str(y), round(v, 4)) for y, v in sorted(series.yearly_returns.items())]
    monthly_dist = [round(v, 4) for v in series.monthly_returns.values()]
    monthly_dist_labels = [f"{y}-{m:02d}" for (y, m), v in sorted(series.monthly_returns.items())]

    # --- Chart configurations as JSON ---
    equity_config = json.dumps({
        "type": "line",
        "data": {
            "labels": equity_labels,
            "datasets": [{
                "label": "Equity Multiple",
                "data": equity_values,
                "borderColor": "#0f5e9c",
                "backgroundColor": "rgba(15, 94, 156, 0.08)",
                "fill": True,
                "tension": 0.15,
                "pointRadius": 0,
                "pointHoverRadius": 5,
                "borderWidth": 2,
            }]
        },
        "options": {
            "responsive": True,
            "maintainAspectRatio": False,
            "interaction": {"intersect": False, "mode": "index"},
            "plugins": {
                "legend": {"display": False},
                "tooltip": {
                    "callbacks": {"label": "ctx => 'Equity: ' + ctx.parsed.y.toFixed(2)"}
                }
            },
            "scales": {
                "x": {"grid": {"display": False}, "ticks": {"maxTicksLimit": 8, "maxRotation": 0}},
                "y": {"grid": {"color": "#f1f5f9"}, "ticks": {"callback": "v => v.toFixed(2)"}}
            }
        }
    })

    yearly_config = json.dumps({
        "type": "bar",
        "data": {
            "labels": [y for y, _ in yearly_data],
            "datasets": [{
                "label": "Return",
                "data": [v for _, v in yearly_data],
                "backgroundColor": ["#0f5e9c" if v >= 0 else "#dc2626" for _, v in yearly_data],
                "borderRadius": 0,
            }]
        },
        "options": {
            "responsive": True,
            "maintainAspectRatio": False,
            "indexAxis": "y",
            "plugins": {"legend": {"display": False}},
            "scales": {
                "x": {"grid": {"display": False}, "ticks": {"callback": "v => (v*100).toFixed(1)+'%'"}},
                "y": {"grid": {"display": False}}
            }
        }
    })

    # Histogram bins
    hist_labels, hist_values = _histogram_data(monthly_dist)
    dist_config = json.dumps({
        "type": "bar",
        "data": {
            "labels": hist_labels,
            "datasets": [{
                "label": "Count",
                "data": hist_values,
                "backgroundColor": "#0f5e9c",
                "borderRadius": 0,
            }]
        },
        "options": {
            "responsive": True,
            "maintainAspectRatio": False,
            "plugins": {"legend": {"display": False}},
            "scales": {
                "x": {"grid": {"display": False}, "title": {"display": True, "text": "Monthly Return"}},
                "y": {"grid": {"color": "#f1f5f9"}, "title": {"display": True, "text": "Count"}}
            }
        }
    })

    dd_config = json.dumps({
        "type": "line",
        "data": {
            "labels": dd_labels,
            "datasets": [{
                "label": "Drawdown",
                "data": dd_values,
                "borderColor": "#dc2626",
                "backgroundColor": "rgba(220, 38, 38, 0.08)",
                "fill": True,
                "tension": 0.15,
                "pointRadius": 0,
                "pointHoverRadius": 4,
                "borderWidth": 1.5,
            }]
        },
        "options": {
            "responsive": True,
            "maintainAspectRatio": False,
            "interaction": {"intersect": False, "mode": "index"},
            "plugins": {
                "legend": {"display": False},
                "tooltip": {
                    "callbacks": {"label": "ctx => 'DD: ' + (ctx.parsed.y*100).toFixed(1)+'%'"}
                }
            },
            "scales": {
                "x": {"grid": {"display": False}, "ticks": {"maxTicksLimit": 8}},
                "y": {
                    "grid": {"color": "#f1f5f9"},
                    "ticks": {"callback": "v => (v*100).toFixed(1)+'%'"},
                    "max": 0,
                }
            }
        }
    })

    rolling_config = json.dumps({
        "type": "line",
        "data": {
            "labels": rolling_labels,
            "datasets": [
                {
                    "label": "Rolling Return",
                    "data": rolling_rets,
                    "borderColor": "#0f5e9c",
                    "backgroundColor": "rgba(15, 94, 156, 0.08)",
                    "fill": True,
                    "tension": 0.15,
                    "pointRadius": 0,
                    "borderWidth": 2,
                },
                {
                    "label": "Rolling Volatility",
                    "data": rolling_vols,
                    "borderColor": "#64748b",
                    "backgroundColor": "transparent",
                    "fill": False,
                    "tension": 0.15,
                    "pointRadius": 0,
                    "borderWidth": 2,
                    "borderDash": [5, 5],
                }
            ]
        },
        "options": {
            "responsive": True,
            "maintainAspectRatio": False,
            "interaction": {"intersect": False, "mode": "index"},
            "plugins": {
                "legend": {"position": "top", "align": "end", "labels": {"usePointStyle": True}}
            },
            "scales": {
                "x": {"grid": {"display": False}, "ticks": {"maxTicksLimit": 8}},
                "y": {"grid": {"color": "#f1f5f9"}, "ticks": {"callback": "v => (v*100).toFixed(0)+'%'"}}
            }
        }
    })

    cards = _metric_cards(metrics)
    warnings = _warnings_html(series.warnings)
    charts_script = _charts_js(equity_config, yearly_config, dist_config, dd_config, rolling_config)

    html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Backtest Report - {escape(strategy_label)}</title>
<style>
{_style_css()}
</style>
</head>
<body>
<main class="report">
  <header class="report-header">
    <div>
      <p class="eyebrow">Backtest Performance ({escape(period_label)})</p>
      <h1>{escape(strategy_label)}</h1>
    </div>
    <div class="meta">
      <div>Generated {escape(generated_at.strftime("%Y-%m-%d %H:%M %Z"))}</div>
      <div>Mode {escape(str(summary.get("mode") or "event"))}</div>
    </div>
  </header>
  {warnings}
  <section class="cards">{cards}</section>

  <section class="section">
    <h2 class="section-title">Strategy Performance</h2>
    <div class="chart chart-wide">
      <canvas id="equityChart"></canvas>
    </div>
  </section>

  <section class="section">
    <h2 class="section-title">Period Returns</h2>
    <div class="chart-grid">
      <div class="chart">{_monthly_heatmap_html(series.monthly_returns)}</div>
      <div class="chart"><canvas id="yearlyChart"></canvas></div>
      <div class="chart"><canvas id="distributionChart"></canvas></div>
      <div class="chart"><canvas id="drawdownChart"></canvas></div>
    </div>
  </section>

  <section class="section">
    <h2 class="section-title">Rolling Statistics</h2>
    <div class="chart chart-wide">
      <canvas id="rollingChart"></canvas>
    </div>
  </section>

      <section class="section">
    <h2 class="section-title">Trade History</h2>
    <div class="tables">
      {_closed_trades_table(series.closed_trades, session_tz)}
      {_open_positions_table(series.open_lots, session_tz)}
      {_run_metadata_table(summary, metrics)}
    </div>
  </section>
</main>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script>
{charts_script}
</script>
{_pagination_js()}
</body>
</html>
"""
    return html


def _downsample_pairs(pairs: list[tuple[str, Any]], max_points: int) -> tuple[list[str], list[Any]]:
    if len(pairs) <= max_points:
        return [p[0] for p in pairs], [p[1] for p in pairs]
    step = (len(pairs) - 1) / (max_points - 1)
    sampled = [pairs[round(i * step)] for i in range(max_points)]
    return [p[0] for p in sampled], [p[1] for p in sampled]


def _drawdown_values(equity_points: Sequence[_EquityPoint]) -> list[float]:
    if not equity_points:
        return []
    peak = equity_points[0].equity
    result: list[float] = []
    for point in equity_points:
        peak = max(peak, point.equity)
        dd = point.equity / peak - 1.0 if peak > _EPSILON else 0.0
        result.append(round(dd, 6))
    return result


def _histogram_data(values: Sequence[float]) -> tuple[list[str], list[int]]:
    if len(values) < 2:
        return [], []
    bins = max(5, min(14, int(math.sqrt(len(values))) + 2))
    min_v, max_v = min(values), max(values)
    if abs(max_v - min_v) <= _EPSILON:
        min_v -= 0.01
        max_v += 0.01
    step = (max_v - min_v) / bins
    counts = [0] * bins
    for value in values:
        idx = min(bins - 1, int((value - min_v) / step))
        counts[idx] += 1
    labels = []
    for i in range(bins):
        lo = min_v + i * step
        hi = min_v + (i + 1) * step
        labels.append(f"{_format_pct(lo, decimals=0, force_sign=False)} to {_format_pct(hi, decimals=0, force_sign=False)}")
    return labels, counts


def _charts_js(equity_cfg: str, yearly_cfg: str, dist_cfg: str, dd_cfg: str, rolling_cfg: str) -> str:
    # Chart.js config objects need JavaScript functions for callbacks.
    # We injected them as strings inside the JSON; now strip the quotes
    # around function bodies so they become real JS functions.
    def _fix_callbacks(cfg: str) -> str:
        import re
        # Replace only arrow-function callback values, leaving ordinary dataset
        # labels like "Equity Multiple" as quoted strings.
        return re.sub(
            r'("(?:callback|label)":\s*)"([^"]*=>[^"]*)"',
            r"\1\2",
            cfg,
        )

    equity_cfg = _fix_callbacks(equity_cfg)
    yearly_cfg = _fix_callbacks(yearly_cfg)
    dist_cfg = _fix_callbacks(dist_cfg)
    dd_cfg = _fix_callbacks(dd_cfg)
    rolling_cfg = _fix_callbacks(rolling_cfg)

    return f"""
(function() {{
  var commonOptions = {{
    responsive: true,
    maintainAspectRatio: false,
    plugins: {{
      legend: {{ labels: {{ font: {{ family: 'Inter, sans-serif', size: 12 }} }} }},
      tooltip: {{ titleFont: {{ family: 'Inter, sans-serif', size: 13 }}, bodyFont: {{ family: 'Inter, sans-serif', size: 12 }} }}
    }}
  }};

  new Chart(document.getElementById('equityChart'), {equity_cfg});
  new Chart(document.getElementById('yearlyChart'), {yearly_cfg});
  new Chart(document.getElementById('distributionChart'), {dist_cfg});
  new Chart(document.getElementById('drawdownChart'), {dd_cfg});
  new Chart(document.getElementById('rollingChart'), {rolling_cfg});
}})();
"""


def _metric_cards(metrics: dict[str, Any]) -> str:
    def _colored(value: str, key: str) -> str:
        if key == "pos":
            return f'<span class="val-pos">{escape(value)}</span>'
        if key == "neg":
            return f'<span class="val-neg">{escape(value)}</span>'
        return escape(value)

    sr = metrics["strategy_return"]
    sr_cls = "pos" if isinstance(sr, (int, float)) and sr > 0 else "neg" if isinstance(sr, (int, float)) and sr < 0 else ""
    card_data = [
        (
            "Strategy Return",
            _colored(_format_pct(metrics["strategy_return"]), sr_cls),
            "mark-to-market strategy equity curve",
        ),
        (
            "CAGR",
            _colored(_format_pct(metrics["annualized_return"]), sr_cls),
            "annualized strategy return",
        ),
        (
            "Max Drawdown",
            _colored(_format_pct(metrics["max_drawdown"], decimals=2), "neg"),
            "bar-close equity drawdown",
        ),
        ("Annualized Sharpe", escape(_format_number(metrics["annualized_sharpe"], 2)), "daily equity curve"),
        ("Trades", escape(str(metrics["closed_trades"])), "closed trades"),
        ("Win Rate", escape(_format_pct(metrics["win_rate"])), "winners / closed trades"),
        ("Profit Factor", escape(_format_profit_factor(metrics)), "gross profit / gross loss"),
        (
            "Positive Years",
            escape(f"{metrics['positive_years']} / {metrics['year_count']}"),
            "calendar years with positive return",
        ),
        (
            "Max Open Lots",
            escape(str(metrics["max_concurrent_open_lots"])),
            "maximum concurrent open trade lots",
        ),
        (
            "Max Exposure",
            escape(f"{float(metrics['max_exposure_multiple']):.2f}x"),
            "gross exposure / marked equity",
        ),
    ]
    return "\n".join(
        f"""<article class="card">
  <div class="card-label">{escape(label)}</div>
  <div class="card-value">{value}</div>
  <div class="card-help">{escape(help_text)}</div>
</article>"""
        for label, value, help_text in card_data
    )


def _monthly_heatmap_html(monthly_returns: dict[tuple[int, int], float]) -> str:
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    years = sorted({year for year, _ in monthly_returns})
    if not years:
        return '<div class="heatmap"><p class="empty-text">No monthly returns</p></div>'

    header = "".join(f'<div class="hm-month">{m}</div>' for m in months)
    rows: list[str] = []
    for year in years:
        cells = []
        for month in range(1, 13):
            value = monthly_returns.get((year, month))
            if value is None:
                cells.append('<div class="hm-cell hm-empty"></div>')
            else:
                intensity = min(1.0, abs(value) / 0.15)
                if value >= 0:
                    bg = f"rgba(15, 94, 156, {0.08 + intensity * 0.92})"
                    text = "#ffffff" if intensity > 0.5 else "#0f5e9c"
                else:
                    bg = f"rgba(220, 38, 38, {0.08 + intensity * 0.92})"
                    text = "#ffffff" if intensity > 0.5 else "#dc2626"
                label = _format_pct(value, decimals=1, force_sign=False)
                cells.append(f'<div class="hm-cell" style="background:{bg};color:{text}">{escape(label)}</div>')
        rows.append(
            f'<div class="hm-row"><div class="hm-year">{year}</div>{"".join(cells)}</div>'
        )

    return f'''<div class="heatmap">
  <div class="hm-header"><div class="hm-year"></div>{header}</div>
  {''.join(rows)}
</div>'''


def _closed_trades_table(closed_trades: Sequence[_ClosedTrade], session_tz: ZoneInfo) -> str:
    if not closed_trades:
        return _table_block("Closed Trades", "<p class=\"empty-text\">No closed trades.</p>")
    limit = 500
    trades = list(closed_trades)[-limit:]
    rows = []
    for trade in trades:
        rows.append(
            "<tr>"
            f"<td>{escape(_format_table_datetime(trade.entry_time, session_tz))}</td>"
            f"<td>{escape(_format_table_datetime(trade.exit_time, session_tz))}</td>"
            f"<td>{escape(trade.strategy_id)}</td>"
            f"<td>{escape(trade.instrument.label)}</td>"
            f"<td>{escape(trade.side)}</td>"
            f"<td>{escape(trade.close_result)}</td>"
            f"<td>{trade.entry_price:.3f}</td>"
            f"<td>{trade.exit_price:.3f}</td>"
            f"<td class=\"{'col-pos' if trade.pnl > 0 else 'col-neg' if trade.pnl < 0 else ''}\">{escape(_format_money(trade.pnl))}</td>"
            f"<td>{escape(_format_pct(trade.return_pct))}</td>"
            f"<td>{escape(trade.trade_id or '')}</td>"
            "</tr>"
        )
    total = len(closed_trades)
    showing = len(trades)
    note = "" if total <= limit else f"<p class=\"empty-text\">Showing latest {limit} of {total} trades.</p>"
    pagination = (
        '<div class="pagination">'
        '<button class="page-btn" data-action="prev">← Prev</button>'
        '<span class="page-info">Page 1 of 1</span>'
        '<button class="page-btn" data-action="next">Next →</button>'
        "</div>"
    )
    body = (
        note
        + '<table class="paginated" data-page-size="35"><thead><tr><th>Entry Time (ET)</th><th>Exit Time (ET)</th><th>Strategy</th>'
        "<th>Symbol</th><th>Side</th><th>Close Result</th><th>Entry Price</th><th>Exit Price</th>"
        "<th>PnL</th><th>Return</th><th>Trade ID</th></tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
        + pagination
    )
    return _table_block("Closed Trades", body)


def _style_css() -> str:
    return """
:root {
  --bg: #f5f6f8;
  --card: #ffffff;
  --text: #172033;
  --text-muted: #5f6b7a;
  --primary: #0b5f8f;
  --positive: #059669;
  --negative: #dc2626;
  --border: #d8dee8;
  --border-strong: #b8c2d0;
  --shadow: 0 1px 1px rgba(15, 23, 42, 0.04);
  --radius: 0;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  background: var(--bg);
  color: var(--text);
  line-height: 1.5;
  -webkit-font-smoothing: antialiased;
}
.report {
  max-width: 1200px;
  margin: 0 auto;
  padding: 28px 24px 44px;
}
.report-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-end;
  margin-bottom: 24px;
  padding-bottom: 16px;
  border-bottom: 1px solid var(--border-strong);
}
.eyebrow {
  margin: 0 0 8px;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  font-size: 12px;
  font-weight: 700;
  color: var(--primary);
}
h1 {
  margin: 0;
  font-size: 30px;
  font-weight: 750;
  color: var(--text);
  letter-spacing: 0;
}
.meta {
  text-align: right;
  color: var(--text-muted);
  font-size: 13px;
  line-height: 1.6;
}
.section {
  margin-bottom: 20px;
}
.section-title {
  font-size: 12px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--text-muted);
  margin: 0 0 8px;
}
.cards {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
  margin-bottom: 12px;
}
.card {
  background: var(--card);
  border-radius: var(--radius);
  padding: 12px 14px;
  box-shadow: var(--shadow);
  border: 1px solid var(--border);
}
.card:hover {
  border-color: var(--border-strong);
}
.card-label {
  font-size: 11px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--text-muted);
}
.card-value {
  margin-top: 6px;
  font-size: 25px;
  font-weight: 750;
  color: var(--text);
  letter-spacing: 0;
}
.card-help {
  margin-top: 4px;
  font-size: 12px;
  color: var(--text-muted);
}
.val-pos { color: var(--positive); }
.val-neg { color: var(--negative); }
.chart {
  background: var(--card);
  border-radius: var(--radius);
  padding: 14px;
  box-shadow: var(--shadow);
  border: 1px solid var(--border);
}
.chart-wide {
  height: 400px;
}
.chart-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
}
.chart-grid .chart {
  height: 340px;
  padding: 12px;
}
canvas {
  width: 100% !important;
  height: 100% !important;
}

/* Heatmap */
.heatmap {
  display: flex;
  flex-direction: column;
  gap: 3px;
  height: 100%;
  justify-content: center;
}
.hm-header, .hm-row {
  display: grid;
  grid-template-columns: 36px repeat(12, 1fr);
  gap: 3px;
  align-items: center;
}
.hm-month, .hm-year {
  font-size: 10px;
  font-weight: 700;
  text-align: center;
  color: var(--text-muted);
  text-transform: uppercase;
}
.hm-year {
  text-align: left;
}
.hm-cell {
  aspect-ratio: 1.4;
  border-radius: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 10px;
  font-weight: 700;
}
.hm-empty {
  background: #f8fafc;
}

/* Tables */
.tables {
  display: grid;
  gap: 10px;
}
.table-block {
  background: var(--card);
  border-radius: var(--radius);
  padding: 14px 16px 18px;
  box-shadow: var(--shadow);
  border: 1px solid var(--border);
  overflow-x: auto;
}
.table-block h2 {
  margin: 0 0 14px;
  font-size: 16px;
  font-weight: 700;
  color: var(--text);
}
table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12.5px;
}
th, td {
  border-bottom: 1px solid var(--border);
  padding: 8px 10px;
  text-align: left;
  white-space: nowrap;
}
th {
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 0.04em;
  font-size: 10px;
  font-weight: 700;
  background: #f8fafc;
}
tbody tr:hover td {
  background: #f8fafc;
}
.col-pos { color: var(--positive); font-weight: 700; }
.col-neg { color: var(--negative); font-weight: 700; }
.kv th {
  width: 200px;
}
.empty-text {
  margin: 0;
  color: var(--text-muted);
  font-size: 13px;
}

/* Pagination */
.pagination {
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 12px;
  margin-top: 14px;
  font-size: 13px;
}
.page-btn {
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 6px 14px;
  cursor: pointer;
  color: var(--primary);
  font-weight: 600;
  font-size: 13px;
  transition: background 0.1s;
}
.page-btn:hover {
  background: var(--bg);
}
.page-btn:disabled {
  color: #b0b8c4;
  cursor: not-allowed;
  background: var(--card);
}
.page-info {
  color: var(--text-muted);
  font-weight: 600;
  min-width: 160px;
  text-align: center;
}

/* Warnings */
.warnings {
  background: #fff8f7;
  border: 1px solid #fecaca;
  border-radius: var(--radius);
  padding: 14px 18px;
  margin-bottom: 16px;
}
.warnings h2 {
  margin: 0 0 8px;
  font-size: 14px;
  color: var(--negative);
}
.warnings ul {
  margin: 0;
  padding-left: 20px;
  color: #7f1d1d;
  font-size: 13px;
}

/* Responsive */
@media (max-width: 900px) {
  .report-header { display: block; }
  .meta { text-align: left; margin-top: 12px; }
  .cards { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
  .chart-grid { grid-template-columns: 1fr; }
  .chart-wide { height: 300px; }
  .chart-grid .chart { height: 280px; }
}
@media print {
  body { background: #ffffff; }
  .report { width: 100%; padding: 0; }
  .chart, .table-block, .warnings, .card { break-inside: avoid; }
  .pagination { display: none; }
}
"""
def _open_positions_table(open_lots: Sequence[_OpenLot], session_tz: ZoneInfo) -> str:
    lots = [lot for lot in open_lots if abs(lot.quantity) > _EPSILON]
    if not lots:
        return _table_block("Open Positions", "<p class=\"empty-text\">No open positions.</p>")
    rows = []
    for lot in lots:
        rows.append(
            "<tr>"
            f"<td>{escape(_format_table_datetime(lot.entry_time, session_tz))}</td>"
            f"<td>{escape(lot.strategy_id)}</td>"
            f"<td>{escape(lot.instrument.label)}</td>"
            f"<td>{escape('long' if lot.quantity > 0 else 'short')}</td>"
            f"<td>{abs(lot.quantity):g}</td>"
            f"<td>{lot.entry_price:.3f}</td>"
            f"<td>{escape(lot.trade_id or '')}</td>"
            "</tr>"
        )
    body = (
        "<table><thead><tr><th>Entry Time (ET)</th><th>Strategy</th><th>Symbol</th>"
        "<th>Side</th><th>Qty</th><th>Avg Entry</th><th>Trade ID</th></tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )
    return _table_block("Open Positions", body)


def _run_metadata_table(summary: dict[str, Any], metrics: dict[str, Any]) -> str:
    items = [
        ("Start", _stringify(summary.get("start"))),
        ("End", _stringify(summary.get("end"))),
        ("CSV", _stringify(summary.get("csv_path"))),
        ("Replay Bars", _stringify(summary.get("replay_bars"))),
        ("Initial Equity", _format_money(metrics.get("initial_equity"))),
        ("Final Equity", _format_money(metrics.get("final_equity"))),
        ("Net PnL", _format_money(metrics.get("net_pnl"))),
    ]
    timings = summary.get("timings") if isinstance(summary.get("timings"), dict) else {}
    for key in ("replay_load_seconds", "candidate_generation_seconds", "engine_run_seconds", "total_seconds"):
        if key in timings:
            items.append((key, f"{float(timings[key]):.2f}s"))
    rows = "".join(
        f"<tr><th>{escape(label)}</th><td>{escape(value)}</td></tr>"
        for label, value in items
    )
    return _table_block("Run Metadata", f"<table class=\"kv\"><tbody>{rows}</tbody></table>")


def _table_block(title: str, content: str) -> str:
    return f"<section class=\"table-block\"><h2>{escape(title)}</h2>{content}</section>"

def _pagination_js() -> str:
    return """<script>
(function() {
  document.querySelectorAll('table.paginated').forEach(function(table) {
    var pageSize = parseInt(table.dataset.pageSize || '50', 10);
    var rows = Array.from(table.querySelectorAll('tbody tr'));
    var total = rows.length;
    var pages = Math.max(1, Math.ceil(total / pageSize));
    var current = 0;
    function showPage(n) {
      current = Math.max(0, Math.min(n, pages - 1));
      rows.forEach(function(row, i) {
        row.style.display = (i >= current * pageSize && i < (current + 1) * pageSize) ? '' : 'none';
      });
      var info = table.parentElement.querySelector('.page-info');
      if (info) info.textContent = 'Page ' + (current + 1) + ' of ' + pages + ' (' + total + ' trades)';
      var prevBtn = table.parentElement.querySelector('[data-action="prev"]');
      var nextBtn = table.parentElement.querySelector('[data-action="next"]');
      if (prevBtn) prevBtn.disabled = current === 0;
      if (nextBtn) nextBtn.disabled = current === pages - 1;
    }
    var controls = table.parentElement.querySelector('.pagination');
    if (controls) {
      controls.querySelector('[data-action="prev"]').addEventListener('click', function() { showPage(current - 1); });
      controls.querySelector('[data-action="next"]').addEventListener('click', function() { showPage(current + 1); });
    }
    showPage(0);
  });
})();
</script>"""


def _warnings_html(warnings: Sequence[str]) -> str:
    if not warnings:
        return ""
    items = "".join(f"<li>{escape(message)}</li>" for message in warnings[:20])
    extra = "" if len(warnings) <= 20 else f"<li>{len(warnings) - 20} more warnings omitted.</li>"
    return f'<section class="warnings"><h2>Report Warnings</h2><ul>{items}{extra}</ul></section>'
def _rolling_monthly(monthly_returns: dict[tuple[int, int], float]) -> list[tuple[str, float, float]]:
    rows = sorted(monthly_returns.items())
    result: list[tuple[str, float, float]] = []
    window = 6
    for index in range(window - 1, len(rows)):
        window_rows = rows[index - window + 1 : index + 1]
        values = [value for _, value in window_rows]
        rolling_return = math.prod(1.0 + value for value in values) - 1.0
        volatility = statistics.stdev(values) * math.sqrt(12) if len(values) > 1 else 0.0
        year, month = rows[index][0]
        result.append((f"{year}-{month:02d}", rolling_return, volatility))
    return result


def _annualized_sharpe(values: Sequence[float]) -> float | None:
    if len(values) < 2:
        return None
    std = statistics.stdev(values)
    if std <= _EPSILON:
        return None
    return statistics.mean(values) / std * math.sqrt(252)


def _annualized_return(
    *,
    initial_equity: float,
    final_equity: float,
    start: datetime | None,
    end: datetime | None,
) -> float | None:
    if initial_equity <= _EPSILON or final_equity <= 0 or start is None or end is None:
        return None
    years = (end - start).total_seconds() / (365.25 * 24 * 60 * 60)
    if years <= _EPSILON:
        return None
    return (final_equity / initial_equity) ** (1.0 / years) - 1.0


def _sortino(values: Sequence[float]) -> float | None:
    if len(values) < 2:
        return None
    downside = [min(0.0, value) for value in values]
    downside_std = math.sqrt(sum(value * value for value in downside) / len(downside))
    if downside_std <= _EPSILON:
        return None
    return statistics.mean(values) / downside_std * math.sqrt(252)


def _max_drawdown(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    peak = values[0]
    max_dd = 0.0
    for value in values:
        peak = max(peak, value)
        if peak > _EPSILON:
            max_dd = min(max_dd, value / peak - 1.0)
    return max_dd


def _format_table_datetime(value: datetime, session_tz: ZoneInfo) -> str:
    return value.astimezone(session_tz).strftime("%Y-%m-%d %H:%M")


def _format_pct(value: Any, *, decimals: int = 1, force_sign: bool = True) -> str:
    if value is None:
        return "N/A"
    value = float(value)
    sign = "+" if force_sign and value > 0 else ""
    return f"{sign}{value * 100:.{decimals}f}%"


def _format_number(value: Any, decimals: int = 2) -> str:
    if value is None:
        return "N/A"
    return f"{float(value):.{decimals}f}"


def _format_money(value: Any) -> str:
    if value is None:
        return "N/A"
    return f"${float(value):,.2f}"


def _format_profit_factor(metrics: dict[str, Any]) -> str:
    value = metrics.get("profit_factor")
    if value is not None:
        return _format_number(value, 2)
    if metrics.get("gross_profit", 0.0) > 0 and abs(metrics.get("gross_loss", 0.0)) <= _EPSILON:
        return "inf"
    return "N/A"


def _safe_return(value: float, previous: float) -> float:
    if abs(previous) <= _EPSILON:
        return 0.0
    return value / previous - 1.0


def _signed_quantity(side: str, quantity: float) -> float:
    normalized = side.strip().lower()
    if normalized in _LONG_SIDES:
        return abs(quantity)
    if normalized in _SHORT_SIDES:
        return -abs(quantity)
    return float(quantity)


def _instrument_from_bar(bar: Bar) -> _InstrumentRef:
    inst = bar.instrument
    return _InstrumentRef(
        asset_class=str(inst.asset_class),
        symbol=str(inst.symbol),
        exchange=inst.exchange,
        currency=inst.currency,
        expiry=inst.expiry.isoformat() if inst.expiry else None,
        strike=str(inst.strike) if inst.strike is not None else None,
        right=inst.right,
        multiplier=float(inst.multiplier),
    )


def _instrument_from_mapping(value: dict[str, Any]) -> _InstrumentRef:
    return _InstrumentRef(
        asset_class=str(value.get("asset_class") or "equity"),
        symbol=str(value.get("symbol") or "<unknown>"),
        exchange=_optional_str(value.get("exchange")),
        currency=_optional_str(value.get("currency")),
        expiry=_optional_str(value.get("expiry")),
        strike=_optional_str(value.get("strike")),
        right=_optional_str(value.get("right")),
        multiplier=float(value.get("multiplier") or 1.0),
    )


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if not isinstance(value, str):
        raise ValueError(f"Expected ISO timestamp, got {value!r}")
    return _ensure_utc(datetime.fromisoformat(value))


def _ensure_utc(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def _safe_zone(value: str) -> ZoneInfo:
    try:
        return ZoneInfo(value)
    except Exception:
        return ZoneInfo("UTC")


def _period_label(summary: dict[str, Any], session_tz: ZoneInfo) -> str:
    try:
        start = _parse_timestamp(summary.get("start")).astimezone(session_tz)
        end = _parse_timestamp(summary.get("end")).astimezone(session_tz)
        if start.year == end.year:
            return str(start.year)
        return f"{start.year}-{end.year}"
    except Exception:
        return "Backtest"


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)
