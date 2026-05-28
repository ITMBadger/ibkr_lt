"""IBKRDataProvider — streaming (5s) and historical bars from IBKR.

Streaming: reqRealTimeBars → 5s bars → bar_queue (pushed by IBKRClient).
  Volume scaling: STK contracts ×100 (IBKR uses 100-share lots), futures ×1.
Historical: reqHistoricalData → 1-min bars.

StreamCapabilities: native_timeframes = {5s}.
DataManager receives 5s bars, BarBuilder produces 1m bars from them.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, AsyncIterator

from ...types import Bar, Instrument, StreamCapabilities
from ...engine.timeframes import Timeframe, TF_5S, TF_1M
from .contracts import instrument_to_contract

if TYPE_CHECKING:
    from .client import IBKRClient

log = logging.getLogger(__name__)

_MAX_1M_HISTORICAL_DAYS = 10


class IBKRDataProvider:
    """Streaming + historical data provider backed by an IBKRClient."""

    capabilities = StreamCapabilities(
        native_timeframes=frozenset({TF_5S}),
        supports_intrabar=False,
    )

    def __init__(
        self,
        client: "IBKRClient",
        host: str = "127.0.0.1",
        port: int = 7497,
        client_id: int = 1,
    ) -> None:
        self._client = client
        self._host = host
        self._port = port
        self._client_id = client_id
        self._subscriptions: dict[Instrument, int] = {}  # instrument → req_id
        self._subscription_timeframes: dict[Instrument, Timeframe] = {}
        self._req_id_to_instrument: dict[int, Instrument] = {}
        self._next_req_id = 10_000

    async def connect(self) -> None:
        if not self._client.is_ready():
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                self._client.connect_and_run,
                self._host,
                self._port,
                self._client_id,
                loop,
            )

    async def disconnect(self) -> None:
        for instrument in list(self._subscriptions):
            await self.unsubscribe(instrument)

    def is_connected(self) -> bool:
        try:
            return bool(self._client.is_ready() and self._client.isConnected())
        except Exception:
            return False

    # ------------------------------------------------------------------
    # StreamingDataProvider protocol
    # ------------------------------------------------------------------

    async def subscribe(self, instrument: Instrument, timeframe: Timeframe) -> None:
        """Subscribe to 5s real-time bars for `instrument`.
        timeframe is ignored — IBKR always streams at 5s; DataManager/BarBuilder handles upsampling.
        """
        if instrument in self._subscriptions:
            self._subscription_timeframes[instrument] = timeframe
            return
        req_id = self._next_req_id
        self._next_req_id += 1
        contract = instrument_to_contract(instrument)
        what_to_show = "TRADES" if instrument.asset_class != "fx" else "MIDPOINT"
        self._client.reqRealTimeBars(req_id, contract, 5, what_to_show, True, [])
        self._subscriptions[instrument] = req_id
        self._subscription_timeframes[instrument] = timeframe
        self._req_id_to_instrument[req_id] = instrument
        log.info("Subscribed realtime bars: %s (req_id=%d)", instrument.symbol, req_id)

    async def unsubscribe(self, instrument: Instrument) -> None:
        req_id = self._subscriptions.pop(instrument, None)
        if req_id is not None:
            self._req_id_to_instrument.pop(req_id, None)
            self._subscription_timeframes.pop(instrument, None)
            try:
                self._client.cancelRealTimeBars(req_id)
            except Exception as exc:
                log.debug("cancelRealTimeBars failed during unsubscribe: %s", exc)

    async def resubscribe_all(self) -> None:
        subscriptions = dict(self._subscription_timeframes)
        self._subscriptions.clear()
        self._req_id_to_instrument.clear()
        for instrument, timeframe in subscriptions.items():
            await self.subscribe(instrument, timeframe)

    async def bars(self) -> AsyncIterator[Bar]:
        """Drain bar_queue; yield one Bar per 5s IBKR callback.

        Volume scaling: STK ×100 (IBKR normalises to 100-share lots).
        """
        while True:
            item = await self._client.bar_queue.get()
            req_id = item.get("req_id")
            instrument = self._req_id_to_instrument.get(req_id)
            if instrument is None:
                continue

            yield Bar(
                instrument=instrument,
                timeframe=TF_5S,
                timestamp=item["timestamp"],
                open=item["open"],
                high=item["high"],
                low=item["low"],
                close=item["close"],
                volume=float(item["volume"]) * _volume_scale(instrument),
                is_closed=True,
                source="ibkr",
            )

    # ------------------------------------------------------------------
    # HistoricalDataProvider protocol
    # ------------------------------------------------------------------

    async def fetch(
        self,
        instrument: Instrument,
        timeframe: Timeframe,
        start: datetime,
        end: datetime,
    ) -> list[Bar]:
        """Request historical 1-min bars from IBKR."""
        duration_days = max(1, (end - start).days + 1)
        duration_str = f"{min(duration_days, _MAX_1M_HISTORICAL_DAYS)} D"

        end_dt = (
            end.astimezone(timezone.utc)
            if end.tzinfo
            else end.replace(tzinfo=timezone.utc)
        )
        end_str = end_dt.strftime("%Y%m%d %H:%M:%S UTC")

        contract = instrument_to_contract(instrument)
        what_to_show = "MIDPOINT" if instrument.asset_class == "fx" else "TRADES"

        req_id = self._next_req_id
        self._next_req_id += 1

        self._client.reqHistoricalData(
            req_id, contract, end_str, duration_str, "1 min",
            what_to_show, 1, 2, False, []
        )

        bars: list[Bar] = []
        try:
            while True:
                item = await asyncio.wait_for(self._client.hist_queue.get(), timeout=30)
                if item.get("req_id") != req_id:
                    await self._client.hist_queue.put(item)
                    continue
                if item.get("done"):
                    break
                raw_date = str(item["date"])
                ts = _parse_ibkr_date(raw_date)
                if ts is None:
                    log.warning(
                        "Skipping historical bar for %s with unparsable IBKR date %r",
                        instrument.symbol,
                        raw_date,
                    )
                    continue
                bars.append(Bar(
                    instrument=instrument,
                    timeframe=TF_1M,
                    timestamp=ts,
                    open=float(item["open"]),
                    high=float(item["high"]),
                    low=float(item["low"]),
                    close=float(item["close"]),
                    volume=float(item["volume"]) * _volume_scale(instrument),
                    is_closed=True,
                    source="ibkr",
                ))
        except asyncio.TimeoutError:
            log.warning("Historical data request timed out for %s", instrument.symbol)

        return bars


def _volume_scale(instrument: Instrument) -> float:
    return 100.0 if instrument.asset_class == "equity" else 1.0


def _parse_ibkr_date(date_str: str) -> datetime | None:
    """Parse IBKR date string to tz-aware UTC datetime."""
    try:
        if date_str.isdigit() and len(date_str) > 8:
            return datetime.fromtimestamp(int(date_str), tz=timezone.utc)
        if len(date_str) > 8:
            # "20260501 09:30:00 US/Eastern" or "20260501 13:30:00"
            parts = date_str.split()
            dt = datetime.strptime(f"{parts[0]} {parts[1]}", "%Y%m%d %H:%M:%S")
            tz_str = parts[2] if len(parts) > 2 else "US/Eastern"
            import pytz
            local_tz = pytz.timezone(tz_str)
            return local_tz.localize(dt).astimezone(pytz.utc)
        else:
            # "20260501"
            dt = datetime.strptime(date_str, "%Y%m%d")
            return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None
