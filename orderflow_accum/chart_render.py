from __future__ import annotations

from pathlib import Path
import logging
import uuid

import matplotlib
matplotlib.use("Agg")
import mplfinance as mpf
import pandas as pd

logger = logging.getLogger("Accum.Chart")


def _prepare_plot_df(df: pd.DataFrame) -> pd.DataFrame:
    plot_df = df.copy()
    if plot_df.empty:
        return plot_df
    time_col = "start" if "start" in plot_df.columns else "time"
    plot_df[time_col] = pd.to_datetime(pd.to_numeric(plot_df[time_col], errors="coerce"), unit="ms", utc=True).dt.tz_localize(None)
    plot_df = plot_df.set_index(time_col)
    rename_map = {"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"}
    plot_df = plot_df.rename(columns=rename_map)
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in plot_df.columns:
            plot_df[col] = pd.to_numeric(plot_df[col], errors="coerce")
    return plot_df[[c for c in ["Open", "High", "Low", "Close", "Volume"] if c in plot_df.columns]].dropna()


def render_signal_chart(df: pd.DataFrame, symbol: str, kind: str, support: float | None, resistance: float | None, entry: float, stop_loss: float, take_profit_1: float, take_profit_2: float, output_dir: str = "charts") -> str | None:
    if df.empty:
        return None
    plot_df = _prepare_plot_df(df)
    if plot_df.empty:
        return None

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    out_path = Path(output_dir) / f"{symbol}_{uuid.uuid4().hex[:10]}.png"

    hlines_vals = [entry, stop_loss, take_profit_1, take_profit_2]
    hline_colors = ["#1f77b4", "#d62728", "#2ca02c", "#2ca02c"]
    if support is not None:
        hlines_vals.append(float(support))
        hline_colors.append("#0b8043")
    if resistance is not None:
        hlines_vals.append(float(resistance))
        hline_colors.append("#b71c1c")

    hlines = dict(hlines=hlines_vals, colors=hline_colors, linestyle="-.", linewidths=[1.0] * len(hlines_vals))

    title = f"{symbol} {kind}"
    style = mpf.make_mpf_style(base_mpf_style="charles", gridstyle=":")
    try:
        mpf.plot(
            plot_df.tail(120),
            type="candle",
            style=style,
            title=title,
            volume=True,
            ylabel="Price",
            ylabel_lower="Volume",
            hlines=hlines,
            tight_layout=True,
            savefig=dict(fname=str(out_path), dpi=130, bbox_inches="tight", pad_inches=0.2),
            figratio=(10, 6),
            figscale=1.2,
        )
        return str(out_path)
    except Exception as exc:
        logger.warning("Chart render failed for %s: %r", symbol, exc)
        return None
