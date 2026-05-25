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

    equity_series = [
        (
            point.timestamp.astimezone(session_tz).strftime("%Y-%m-%d %H:%M"),
            point.equity / initial_equity if initial_equity else 1.0,
        )
        for point in series.equity_points
    ]
    drawdown_series = _compute_drawdown_series(series.equity_points, session_tz)
    monthly_values = list(series.monthly_returns.values())
    rolling = _rolling_monthly(series.monthly_returns)
    cards = _metric_cards(metrics)
    warnings = _warnings_html(series.warnings)
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
  <section class="chart chart-wide">
    {_line_svg(equity_series, "Strategy Performance", y_formatter="{:.2f}")}
  </section>
  <section class="chart-grid">
    <div class="chart">{_monthly_heatmap_svg(series.monthly_returns)}</div>
    <div class="chart">{_yearly_bar_svg(series.yearly_returns)}</div>
    <div class="chart">{_histogram_svg(monthly_values)}</div>
    <div class="chart">{_drawdown_svg(drawdown_series)}</div>
  </section>
  <section class="chart chart-wide">
    {_rolling_svg(rolling)}
  </section>
  <section class="tables">
    {_strategy_table(series.closed_trades)}
    {_closed_trades_table(series.closed_trades)}
    {_open_positions_table(series.open_lots)}
    {_run_metadata_table(summary, metrics)}
  </section>
</main>
</body>
</html>
"""
    return html


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
        ("Trades", escape(str(metrics["closed_trades"])), "closed trades"),
        (
            "Strategy Return",
            _colored(_format_pct(metrics["strategy_return"]), sr_cls),
            "mark-to-market strategy equity curve",
        ),
        ("Annualized Sharpe", escape(_format_number(metrics["annualized_sharpe"], 2)), "daily equity curve"),
        (
            "Max Drawdown",
            _colored(_format_pct(metrics["max_drawdown"], decimals=2), "neg"),
            "bar-close equity drawdown",
        ),
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
            "maximum concurrent strategy lots",
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


def _line_svg(
    points: Sequence[tuple[str, float]],
    title: str,
    *,
    y_formatter: str = "{:.1%}",
) -> str:
    points = _downsample(points, 900)
    if len(points) < 2:
        return _empty_svg(title, "Not enough equity points")
    width, height = 920, 300
    left, right, top, bottom = 58, 20, 38, 38
    plot_w = width - left - right
    plot_h = height - top - bottom
    values = [value for _, value in points]
    min_y, max_y = _padded_range(values)

    def sx(index: int) -> float:
        return left + (index / max(1, len(points) - 1)) * plot_w

    def sy(value: float) -> float:
        return top + ((max_y - value) / (max_y - min_y)) * plot_h

    path = " ".join(
        ("M" if index == 0 else "L") + f"{sx(index):.2f},{sy(value):.2f}"
        for index, (_, value) in enumerate(points)
    )
    grid = _y_grid(left, top, plot_w, plot_h, min_y, max_y, y_formatter)
    start_label = escape(points[0][0][:10])
    end_label = escape(points[-1][0][:10])
    return f"""<svg viewBox="0 0 {width} {height}" role="img" aria-label="{escape(title)}">
  <text x="{width / 2:.0f}" y="22" text-anchor="middle" class="svg-title">{escape(title)}</text>
  {grid}
  <path d="{path}" fill="none" stroke="{_CARD_BLUE}" stroke-width="3" stroke-linejoin="round" stroke-linecap="round"/>
  <line x1="{left}" y1="{height - bottom}" x2="{width - right}" y2="{height - bottom}" class="axis"/>
  <text x="{left}" y="{height - 12}" class="axis-label">{start_label}</text>
  <text x="{width - right}" y="{height - 12}" text-anchor="end" class="axis-label">{end_label}</text>
</svg>"""


def _monthly_heatmap_svg(monthly_returns: dict[tuple[int, int], float]) -> str:
    title = "Monthly Returns"
    if not monthly_returns:
        return _empty_svg(title, "No monthly returns")
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    years = sorted({year for year, _ in monthly_returns})
    width = 920
    cell_w = 62
    cell_h = 31
    left, top = 66, 54
    height = top + len(years) * cell_h + 28
    month_labels = "\n".join(
        f'<text x="{left + index * cell_w + cell_w / 2:.1f}" y="44" text-anchor="middle" class="axis-label">{month}</text>'
        for index, month in enumerate(months)
    )
    rows: list[str] = []
    for row_index, year in enumerate(years):
        y = top + row_index * cell_h
        rows.append(f'<text x="14" y="{y + 20:.1f}" class="axis-label">{year}</text>')
        for month in range(1, 13):
            value = monthly_returns.get((year, month))
            x = left + (month - 1) * cell_w
            if value is None:
                fill = "#f4f6f8"
                label = ""
                text_class = "heat-text"
            else:
                fill = _heat_color(value)
                label = _format_pct(value, decimals=1, force_sign=False)
                text_class = "heat-text heat-strong" if abs(value) >= 0.08 else "heat-text"
            rows.append(
                f'<rect x="{x:.1f}" y="{y:.1f}" width="{cell_w - 3}" height="{cell_h - 3}" rx="2" fill="{fill}"/>'
            )
            rows.append(
                f'<text x="{x + cell_w / 2:.1f}" y="{y + 19:.1f}" text-anchor="middle" class="{text_class}">{escape(label)}</text>'
            )
    return f"""<svg viewBox="0 0 {width} {height}" role="img" aria-label="{title}">
  <text x="{width / 2:.0f}" y="22" text-anchor="middle" class="svg-title">{title}</text>
  {month_labels}
  {"".join(rows)}
</svg>"""


def _yearly_bar_svg(yearly_returns: dict[int, float]) -> str:
    title = "Yearly Returns"
    if not yearly_returns:
        return _empty_svg(title, "No yearly returns")
    years = sorted(yearly_returns)
    values = [yearly_returns[year] for year in years]
    width, height = 920, max(230, 58 + len(years) * 32)
    left, right, top, bottom = 74, 28, 42, 34
    plot_w = width - left - right
    min_x = min(0.0, min(values))
    max_x = max(0.0, max(values))
    if abs(max_x - min_x) <= _EPSILON:
        max_x = 0.01
        min_x = -0.01
    zero_x = left + ((0.0 - min_x) / (max_x - min_x)) * plot_w
    row_h = (height - top - bottom) / len(years)
    parts = [
        f'<text x="{width / 2:.0f}" y="22" text-anchor="middle" class="svg-title">{title}</text>',
        f'<line x1="{zero_x:.1f}" y1="{top}" x2="{zero_x:.1f}" y2="{height - bottom}" class="axis"/>',
    ]
    for index, year in enumerate(years):
        value = yearly_returns[year]
        y = top + index * row_h + row_h * 0.25
        bar_h = max(6, row_h * 0.5)
        x = left + ((min(value, 0.0) - min_x) / (max_x - min_x)) * plot_w
        bar_w = abs(value) / (max_x - min_x) * plot_w
        color = _CARD_BLUE if value >= 0 else "#b42318"
        label_x = x + bar_w + 6 if value >= 0 else x - 6
        anchor = "start" if value >= 0 else "end"
        parts.append(f'<text x="14" y="{y + bar_h - 1:.1f}" class="axis-label">{year}</text>')
        parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{max(1, bar_w):.1f}" height="{bar_h:.1f}" fill="{color}"/>')
        parts.append(f'<text x="{label_x:.1f}" y="{y + bar_h - 1:.1f}" text-anchor="{anchor}" class="axis-label">{escape(_format_pct(value, decimals=1, force_sign=False))}</text>')
    return f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{title}">{"".join(parts)}</svg>'


def _histogram_svg(values: Sequence[float]) -> str:
    title = "Distribution of Monthly Returns"
    if len(values) < 2:
        return _empty_svg(title, "Not enough monthly returns")
    width, height = 920, 280
    left, right, top, bottom = 58, 20, 42, 38
    plot_w = width - left - right
    plot_h = height - top - bottom
    bins = max(5, min(14, int(math.sqrt(len(values))) + 2))
    min_v, max_v = min(values), max(values)
    if abs(max_v - min_v) <= _EPSILON:
        min_v -= 0.01
        max_v += 0.01
    step = (max_v - min_v) / bins
    counts = [0] * bins
    for value in values:
        index = min(bins - 1, int((value - min_v) / step))
        counts[index] += 1
    max_count = max(counts) or 1
    parts = [
        f'<text x="{width / 2:.0f}" y="22" text-anchor="middle" class="svg-title">{title}</text>',
        f'<line x1="{left}" y1="{height - bottom}" x2="{width - right}" y2="{height - bottom}" class="axis"/>',
    ]
    bar_gap = 4
    bar_w = plot_w / bins - bar_gap
    for index, count in enumerate(counts):
        x = left + index * (plot_w / bins)
        bar_h = count / max_count * plot_h
        y = top + plot_h - bar_h
        parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{bar_h:.1f}" fill="{_CARD_BLUE}"/>')
    parts.append(f'<text x="{left}" y="{height - 12}" class="axis-label">{escape(_format_pct(min_v, decimals=1, force_sign=False))}</text>')
    parts.append(f'<text x="{width - right}" y="{height - 12}" text-anchor="end" class="axis-label">{escape(_format_pct(max_v, decimals=1, force_sign=False))}</text>')
    return f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{title}">{"".join(parts)}</svg>'


def _qq_svg(values: Sequence[float]) -> str:
    title = "Normal Distribution Q-Q"
    if len(values) < 3:
        return _empty_svg(title, "Not enough monthly returns")
    width, height = 920, 280
    left, right, top, bottom = 58, 20, 42, 38
    plot_w = width - left - right
    plot_h = height - top - bottom
    sorted_values = sorted(value * 100 for value in values)
    normal = NormalDist()
    n = len(sorted_values)
    quantiles = [normal.inv_cdf((index + 0.5) / n) for index in range(n)]
    min_x, max_x = _padded_range(quantiles)
    min_y, max_y = _padded_range(sorted_values)

    def sx(value: float) -> float:
        return left + ((value - min_x) / (max_x - min_x)) * plot_w

    def sy(value: float) -> float:
        return top + ((max_y - value) / (max_y - min_y)) * plot_h

    points = "".join(
        f'<circle cx="{sx(x):.1f}" cy="{sy(y):.1f}" r="3.2" fill="{_CARD_BLUE}"/>'
        for x, y in zip(quantiles, sorted_values, strict=True)
    )
    line = f'<line x1="{sx(min_x):.1f}" y1="{sy(min_y):.1f}" x2="{sx(max_x):.1f}" y2="{sy(max_y):.1f}" stroke="#777" stroke-width="2"/>'
    zero_x = sx(0.0) if min_x <= 0 <= max_x else None
    zero_y = sy(0.0) if min_y <= 0 <= max_y else None
    axes = ""
    if zero_x is not None:
        axes += f'<line x1="{zero_x:.1f}" y1="{top}" x2="{zero_x:.1f}" y2="{height - bottom}" class="axis"/>'
    if zero_y is not None:
        axes += f'<line x1="{left}" y1="{zero_y:.1f}" x2="{width - right}" y2="{zero_y:.1f}" class="axis"/>'
    return f"""<svg viewBox="0 0 {width} {height}" role="img" aria-label="{title}">
  <text x="{width / 2:.0f}" y="22" text-anchor="middle" class="svg-title">{title}</text>
  {axes}{line}{points}
  <text x="{left}" y="{height - 12}" class="axis-label">normal quantile</text>
  <text x="{width - right}" y="{height - 12}" text-anchor="end" class="axis-label">monthly return %</text>
</svg>"""


def _drawdown_svg(points: Sequence[tuple[str, float]]) -> str:
    title = "Drawdown"
    points = _downsample(points, 900)
    if len(points) < 2:
        return _empty_svg(title, "Not enough equity points")
    width, height = 920, 280
    left, right, top, bottom = 58, 20, 42, 38
    plot_w = width - left - right
    plot_h = height - top - bottom
    values = [value for _, value in points]
    min_y = min(values)
    max_y = min(max(values), 0.0)
    if abs(max_y - min_y) <= _EPSILON:
        pad = max(abs(min_y) * 0.05, 0.01)
    else:
        pad = (max_y - min_y) * 0.08
    min_y = min_y - pad
    max_y = max_y + pad * 0.3

    def sx(index: int) -> float:
        return left + (index / max(1, len(points) - 1)) * plot_w

    def sy(value: float) -> float:
        return top + ((max_y - value) / (max_y - min_y)) * plot_h

    line_path = " ".join(
        ("M" if index == 0 else "L") + f"{sx(index):.2f},{sy(value):.2f}"
        for index, (_, value) in enumerate(points)
    )
    zero_y = sy(0.0)
    area_path = line_path + f" L{sx(len(points)-1):.2f},{zero_y:.2f} L{sx(0):.2f},{zero_y:.2f} Z"

    grid = _y_grid(left, top, plot_w, plot_h, min_y, max_y, "{:.1%}")
    start_label = escape(points[0][0][:10])
    end_label = escape(points[-1][0][:10])
    return f"""<svg viewBox="0 0 {width} {height}" role="img" aria-label="{escape(title)}">
  <text x="{width / 2:.0f}" y="24" text-anchor="middle" class="svg-title">{escape(title)}</text>
  {grid}
  <path d="{area_path}" fill="rgba(180, 35, 24, 0.10)" stroke="none"/>
  <path d="{line_path}" fill="none" stroke="#b42318" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"/>
  <line x1="{left}" y1="{height - bottom}" x2="{width - right}" y2="{height - bottom}" class="axis"/>
  <text x="{left}" y="{height - 10}" class="axis-label">{start_label}</text>
  <text x="{width - right}" y="{height - 10}" text-anchor="end" class="axis-label">{end_label}</text>
</svg>"""


def _rolling_svg(rows: Sequence[tuple[str, float, float]]) -> str:
    title = "Rolling Statistics (6 Months)"
    if len(rows) < 2:
        return _empty_svg(title, "Not enough monthly returns")
    rows = _downsample(rows, 900)
    width, height = 920, 300
    left, right, top, bottom = 58, 20, 38, 38
    plot_w = width - left - right
    plot_h = height - top - bottom
    all_values = [row[1] for row in rows] + [row[2] for row in rows]
    min_y, max_y = _padded_range(all_values)

    def sx(index: int) -> float:
        return left + (index / max(1, len(rows) - 1)) * plot_w

    def sy(value: float) -> float:
        return top + ((max_y - value) / (max_y - min_y)) * plot_h

    ret_path = " ".join(
        ("M" if index == 0 else "L") + f"{sx(index):.2f},{sy(row[1]):.2f}"
        for index, row in enumerate(rows)
    )
    vol_path = " ".join(
        ("M" if index == 0 else "L") + f"{sx(index):.2f},{sy(row[2]):.2f}"
        for index, row in enumerate(rows)
    )
    grid = _y_grid(left, top, plot_w, plot_h, min_y, max_y, "{:.0%}")
    return f"""<svg viewBox="0 0 {width} {height}" role="img" aria-label="{title}">
  <text x="{width / 2:.0f}" y="22" text-anchor="middle" class="svg-title">{title}</text>
  {grid}
  <path d="{ret_path}" fill="none" stroke="{_CARD_BLUE}" stroke-width="3" stroke-linejoin="round" stroke-linecap="round"/>
  <path d="{vol_path}" fill="none" stroke="#737373" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"/>
  <line x1="{left}" y1="{height - bottom}" x2="{width - right}" y2="{height - bottom}" class="axis"/>
  <line x1="{width - 210}" y1="51" x2="{width - 192}" y2="51" stroke="{_CARD_BLUE}" stroke-width="3"/>
  <text x="{width - 186}" y="55" class="legend">Rolling Return</text>
  <line x1="{width - 210}" y1="71" x2="{width - 192}" y2="71" stroke="#737373" stroke-width="3"/>
  <text x="{width - 186}" y="75" class="legend">Rolling Volatility</text>
</svg>"""


def _strategy_table(closed_trades: Sequence[_ClosedTrade]) -> str:
    grouped: dict[str, list[_ClosedTrade]] = defaultdict(list)
    for trade in closed_trades:
        grouped[trade.strategy_id].append(trade)
    if not grouped:
        return _table_block("Strategy Summary", "<p class=\"empty-text\">No closed trades.</p>")
    rows = []
    for strategy_id, trades in sorted(grouped.items()):
        wins = sum(1 for trade in trades if trade.pnl > 0)
        pnl = sum(trade.pnl for trade in trades)
        sum_return = sum(trade.return_pct for trade in trades)
        rows.append(
            "<tr>"
            f"<td>{escape(strategy_id)}</td>"
            f"<td>{len(trades)}</td>"
            f"<td>{escape(_format_pct(wins / len(trades)))}</td>"
            f"<td>{escape(_format_money(pnl))}</td>"
            f"<td>{escape(_format_pct(sum_return))}</td>"
            "</tr>"
        )
    body = (
        "<table><thead><tr><th>Strategy</th><th>Trades</th><th>Win Rate</th>"
        "<th>PnL</th><th>Sum Return</th></tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )
    return _table_block("Strategy Summary", body)


def _closed_trades_table(closed_trades: Sequence[_ClosedTrade]) -> str:
    if not closed_trades:
        return _table_block("Closed Trades", "<p class=\"empty-text\">No closed trades.</p>")
    limit = 500
    trades = list(closed_trades)[-limit:]
    rows = []
    for trade in trades:
        rows.append(
            "<tr>"
            f"<td>{escape(trade.entry_time.isoformat())}</td>"
            f"<td>{escape(trade.exit_time.isoformat())}</td>"
            f"<td>{escape(trade.strategy_id)}</td>"
            f"<td>{escape(trade.instrument.label)}</td>"
            f"<td>{escape(trade.side)}</td>"
            f"<td>{trade.quantity:g}</td>"
            f"<td>{trade.entry_price:.4f}</td>"
            f"<td>{trade.exit_price:.4f}</td>"
            f"<td>{escape(_format_money(trade.pnl))}</td>"
            f"<td>{escape(_format_pct(trade.return_pct))}</td>"
            f"<td>{escape(trade.trade_id or '')}</td>"
            "</tr>"
        )
    total = len(closed_trades)
    showing = len(trades)
    note = "" if total <= limit else f"<p class=\"empty-text\">Showing latest {limit} of {total} trades.</p>"
    body = (
        note
        + "<table><thead><tr><th>Entry</th><th>Exit</th><th>Strategy</th>"
        "<th>Symbol</th><th>Side</th><th>Qty</th><th>Entry</th><th>Exit</th>"
        "<th>PnL</th><th>Return</th><th>Trade ID</th></tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )
    return _table_block("Closed Trades", body)


def _open_positions_table(open_lots: Sequence[_OpenLot]) -> str:
    lots = [lot for lot in open_lots if abs(lot.quantity) > _EPSILON]
    if not lots:
        return _table_block("Open Positions", "<p class=\"empty-text\">No open positions.</p>")
    rows = []
    for lot in lots:
        rows.append(
            "<tr>"
            f"<td>{escape(lot.entry_time.isoformat())}</td>"
            f"<td>{escape(lot.strategy_id)}</td>"
            f"<td>{escape(lot.instrument.label)}</td>"
            f"<td>{escape('long' if lot.quantity > 0 else 'short')}</td>"
            f"<td>{abs(lot.quantity):g}</td>"
            f"<td>{lot.entry_price:.4f}</td>"
            f"<td>{escape(lot.trade_id or '')}</td>"
            "</tr>"
        )
    body = (
        "<table><thead><tr><th>Entry</th><th>Strategy</th><th>Symbol</th>"
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


def _y_grid(
    left: float,
    top: float,
    width: float,
    height: float,
    min_y: float,
    max_y: float,
    formatter: str,
) -> str:
    parts = []
    for index in range(5):
        ratio = index / 4
        value = max_y - ratio * (max_y - min_y)
        y = top + ratio * height
        parts.append(
            f'<line x1="{left}" y1="{y:.1f}" x2="{left + width}" y2="{y:.1f}" class="grid"/>'
        )
        parts.append(
            f'<text x="{left - 8}" y="{y + 4:.1f}" text-anchor="end" class="axis-label">{escape(formatter.format(value))}</text>'
        )
    return "".join(parts)


def _empty_svg(title: str, message: str) -> str:
    return f"""<svg viewBox="0 0 920 240" role="img" aria-label="{escape(title)}">
  <text x="460" y="24" text-anchor="middle" class="svg-title">{escape(title)}</text>
  <rect x="24" y="48" width="872" height="150" fill="#f6f8fb" stroke="#e2e8f0"/>
  <text x="460" y="126" text-anchor="middle" class="empty-svg">{escape(message)}</text>
</svg>"""


def _padded_range(values: Sequence[float]) -> tuple[float, float]:
    min_v = min(values)
    max_v = max(values)
    if abs(max_v - min_v) <= _EPSILON:
        pad = max(abs(max_v) * 0.05, 0.01)
        return min_v - pad, max_v + pad
    pad = (max_v - min_v) * 0.08
    return min_v - pad, max_v + pad


def _downsample(rows: Sequence[Any], max_points: int) -> list[Any]:
    rows = list(rows)
    if len(rows) <= max_points:
        return rows
    step = (len(rows) - 1) / (max_points - 1)
    return [rows[round(index * step)] for index in range(max_points)]


def _heat_color(value: float) -> str:
    intensity = min(1.0, abs(value) / 0.15)
    if value >= 0:
        return _blend("#f8fbff", "#1f77b4", intensity)
    return _blend("#fff8f8", "#c43c39", intensity)


def _blend(start: str, end: str, ratio: float) -> str:
    ratio = max(0.0, min(1.0, ratio))
    s = tuple(int(start[index : index + 2], 16) for index in (1, 3, 5))
    e = tuple(int(end[index : index + 2], 16) for index in (1, 3, 5))
    rgb = tuple(round(sv + (ev - sv) * ratio) for sv, ev in zip(s, e, strict=True))
    return f"#{rgb[0]:02x}{rgb[1]:02x}{rgb[2]:02x}"


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


def _style_css() -> str:
    return """
:root {
  color-scheme: light;
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  color: #243447;
  background: #f3f6fa;
}
* { box-sizing: border-box; }
body { margin: 0; background: #f3f6fa; }
.report {
  width: min(1180px, calc(100vw - 28px));
  margin: 0 auto;
  padding: 22px 0 42px;
}
.report-header {
  display: flex;
  justify-content: space-between;
  gap: 24px;
  align-items: flex-end;
  margin-bottom: 14px;
}
.eyebrow {
  margin: 0 0 4px;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  font-size: 12px;
  font-weight: 700;
  color: #3c5068;
}
h1 {
  margin: 0;
  font-size: 28px;
  line-height: 1.15;
}
.meta {
  text-align: right;
  color: #516174;
  font-size: 13px;
  line-height: 1.45;
}
.cards {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
  margin: 16px 0;
}
.card {
  background: #ffffff;
  border: 1px solid #d9e1ea;
  border-left: 4px solid #0f5e9c;
  border-radius: 8px;
  padding: 14px 14px 12px;
  min-height: 88px;
  box-shadow: 0 1px 2px rgba(15, 54, 87, 0.05);
  transition: transform 0.12s ease, box-shadow 0.12s ease;
}
.card:hover {
  transform: translateY(-2px);
  box-shadow: 0 6px 16px rgba(15, 54, 87, 0.10);
}
.card-label {
  font-size: 11px;
  line-height: 1.2;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: #516174;
  font-weight: 800;
}
.card-value {
  margin-top: 10px;
  font-size: 24px;
  line-height: 1;
  font-weight: 800;
  color: #0f3657;
}
.val-pos { color: #157a43; }
.val-neg { color: #b42318; }
.card-help {
  margin-top: 8px;
  font-size: 11px;
  line-height: 1.3;
  color: #768395;
}
.chart, .table-block, .warnings {
  background: #ffffff;
  border: 1px solid #d9e1ea;
  border-radius: 8px;
  box-shadow: 0 1px 3px rgba(15, 54, 87, 0.05);
}
.chart {
  padding: 14px 16px;
}
.chart-wide {
  margin-top: 12px;
}
.chart-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
  margin-top: 12px;
}
svg {
  display: block;
  width: 100%;
  height: auto;
}
.svg-title {
  font-size: 19px;
  font-weight: 800;
  fill: #1f2937;
}
.grid {
  stroke: #d9dde3;
  stroke-width: 1;
}
.axis {
  stroke: #333333;
  stroke-width: 1.2;
}
.axis-label, .legend {
  fill: #4b5563;
  font-size: 13px;
}
.heat-text {
  fill: #172033;
  font-size: 12px;
  font-weight: 700;
}
.heat-strong {
  fill: #ffffff;
}
.empty-svg {
  fill: #6b7280;
  font-size: 14px;
}
.warnings {
  padding: 12px 16px;
  margin: 12px 0;
  border-left: 4px solid #b42318;
}
.warnings h2 {
  margin: 0 0 6px;
  font-size: 15px;
}
.warnings ul {
  margin: 0;
  padding-left: 20px;
  color: #5b1e17;
  font-size: 13px;
}
.tables {
  display: grid;
  gap: 12px;
  margin-top: 12px;
}
.table-block {
  padding: 14px 16px 16px;
  overflow-x: auto;
}
.table-block h2 {
  margin: 0 0 10px;
  font-size: 17px;
}
table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
}
th, td {
  border-bottom: 1px solid #e5e9ef;
  padding: 8px 10px;
  text-align: left;
  white-space: nowrap;
}
tbody tr:hover td {
  background: #f6f8fb;
}
th {
  color: #526173;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  font-size: 10px;
}
.kv th {
  width: 220px;
}
.empty-text {
  margin: 0;
  color: #6b7280;
  font-size: 13px;
}
@media (max-width: 900px) {
  .report-header { display: block; }
  .meta { text-align: left; margin-top: 8px; }
  .cards { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
  .chart-grid { grid-template-columns: 1fr; }
}
.pagination {
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 12px;
  margin-top: 14px;
  font-size: 13px;
}
.page-btn {
  background: #ffffff;
  border: 1px solid #d9e1ea;
  border-radius: 6px;
  padding: 6px 14px;
  cursor: pointer;
  color: #0f5e9c;
  font-weight: 600;
  font-size: 13px;
  transition: background 0.1s;
}
.page-btn:hover {
  background: #f3f6fa;
}
.page-btn:disabled {
  color: #b0b8c4;
  cursor: not-allowed;
  background: #ffffff;
}
.page-info {
  color: #516174;
  font-weight: 600;
  min-width: 150px;
  text-align: center;
}
@media print {
  body { background: #ffffff; }
  .report { width: 100%; padding: 0; }
  .chart, .table-block, .warnings, .card { break-inside: avoid; }
  .pagination { display: none; }
}
"""
