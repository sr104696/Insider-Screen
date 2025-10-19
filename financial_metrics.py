# Financial metrics: growth rates and CAGR calculations
# Provides helper functions to compute YoY growth, QoQ growth, CAGR, and rolling growth from time-series data.

from typing import List, Dict, Optional, Union
import math

Number = Union[int, float]

def percent_growth(previous: Number, current: Number) -> Optional[float]:
    if previous is None:
        return None
    try:
        previous_val = float(previous)
        current_val = float(current)
        if previous_val == 0.0:
            return None
        return (current_val - previous_val) / abs(previous_val) * 100.0
    except Exception:
        return None

def yoy_growth(series: List[Optional[Number]]) -> List[Optional[float]]:
    # Year over Year growth assuming equally spaced annual values
    out = []
    for i, v in enumerate(series):
        if i == 0:
            out.append(None)
        else:
            out.append(percent_growth(series[i-1], v))
    return out

def qoq_growth(series: List[Optional[Number]], period: int = 1) -> List[Optional[float]]:
    # Quarter over Quarter (or generic period) growth with lag = period
    out = []
    for i, v in enumerate(series):
        j = i - period
        if j < 0:
            out.append(None)
        else:
            out.append(percent_growth(series[j], v))
    return out

def cagr(start: Number, end: Number, periods: Number) -> Optional[float]:
    # Compound Annual Growth Rate as percentage
    try:
        start_val = float(start)
        end_val = float(end)
        periods_val = float(periods)
        if start_val <= 0.0 or periods_val <= 0.0:
            return None
        return (math.pow(end_val / start_val, 1.0 / periods_val) - 1.0) * 100.0
    except Exception:
        return None

def cagr_from_series(series: List[Optional[Number]], years: Optional[float] = None) -> Optional[float]:
    vals = [float(x) for x in series if x is not None]
    if len(vals) < 2:
        return None
    start = vals[0]
    end = vals[-1]
    n = years if years is not None else (len(vals) - 1)
    return cagr(start, end, n)

def rolling_cagr(series: List[Optional[Number]], window: int) -> List[Optional[float]]:
    # Rolling CAGR over a moving window length
    out = []
    for i in range(len(series)):
        j = i - window
        if j < 0:
            out.append(None)
        else:
            start = series[j]
            end = series[i]
            out.append(cagr(start, end, window))
    return out

def safe_pct(value: Optional[float]) -> Optional[str]:
    if value is None:
        return None
    try:
        return str(round(float(value), 2)) + "%"
    except Exception:
        return None