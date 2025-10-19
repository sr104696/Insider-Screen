# Financial tabulation and normalization utilities
# Converts heterogeneous scraped financial dicts into tidy tables with consistent schemas.

from typing import List, Dict, Any, Optional
import pandas as pd

CANONICAL_FIELDS = {
    "ticker": "ticker",
    "company": "company",
    "fiscal_year": "year",
    "fiscal_quarter": "quarter",
    "period_end": "date",
    "revenue": "revenue",
    "gross_profit": "gross_profit",
    "operating_income": "operating_income",
    "net_income": "net_income",
    "eps_basic": "eps_basic",
    "eps_diluted": "eps_diluted",
    "free_cash_flow": "free_cash_flow",
    "operating_cash_flow": "operating_cash_flow",
    "capex": "capex",
    "total_assets": "total_assets",
    "total_liabilities": "total_liabilities",
    "shares_outstanding": "shares_outstanding",
    "currency": "currency"
}

ALIASES = {
    "fy": "fiscal_year",
    "year": "fiscal_year",
    "q": "fiscal_quarter",
    "quarter": "fiscal_quarter",
    "date": "period_end",
    "period": "period_end",
    "rev": "revenue",
    "sales": "revenue",
    "gp": "gross_profit",
    "op_income": "operating_income",
    "ebit": "operating_income",
    "net": "net_income",
    "net_profit": "net_income",
    "ocf": "operating_cash_flow",
    "fcf": "free_cash_flow",
    "shares": "shares_outstanding"
}

NUMERIC_FIELDS = set([
    "revenue", "gross_profit", "operating_income", "net_income", "eps_basic", "eps_diluted",
    "free_cash_flow", "operating_cash_flow", "capex", "total_assets", "total_liabilities", "shares_outstanding"
])

ORDER_COLS = [
    "ticker", "company", "fiscal_year", "fiscal_quarter", "period_end", "currency",
    "revenue", "gross_profit", "operating_income", "net_income",
    "eps_basic", "eps_diluted", "operating_cash_flow", "capex", "free_cash_flow",
    "total_assets", "total_liabilities", "shares_outstanding"
]


def normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    # Map aliases to canonical names and cast numeric fields
    norm = {}
    for k, v in record.items():
        key = k
        if key in ALIASES:
            key = ALIASES[key]
        if key in CANONICAL_FIELDS:
            norm[key] = v
    # Ensure presence of all canonical
    for k in CANONICAL_FIELDS.keys():
        if k not in norm:
            norm[k] = None
    # Numeric casts
    for k in list(norm.keys()):
        if k in NUMERIC_FIELDS and norm[k] is not None:
            try:
                s = str(norm[k]).replace(",", "").replace(" ", "")
                if s.endswith("M"):
                    val = float(s[:-1]) * 1e6
                elif s.endswith("B"):
                    val = float(s[:-1]) * 1e9
                elif s.endswith("K"):
                    val = float(s[:-1]) * 1e3
                else:
                    val = float(s)
                norm[k] = val
            except Exception:
                norm[k] = None
    return norm


def tabulate(records: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = [normalize_record(r) for r in records]
    df = pd.DataFrame(rows)
    # Order columns
    cols = [c for c in ORDER_COLS if c in df.columns] + [c for c in df.columns if c not in ORDER_COLS]
    df = df[cols]
    # Sort by ticker, year, quarter/date
    if "ticker" in df.columns and "fiscal_year" in df.columns:
        df = df.sort_values(by=["ticker", "fiscal_year", "fiscal_quarter", "period_end"], na_position="last")
    return df.reset_index(drop=True)


def add_growth_columns(df: pd.DataFrame, value_cols: Optional[List[str]] = None, group_col: str = "ticker") -> pd.DataFrame:
    import numpy as np
    if value_cols is None:
        value_cols = ["revenue", "gross_profit", "operating_income", "net_income", "free_cash_flow"]
    df = df.copy()
    df["period_index"] = range(len(df))
    # Group by ticker and compute YoY and QoQ if quarterly data exists
    def compute(group):
        group = group.sort_values(by=["fiscal_year", "fiscal_quarter", "period_end"])
        for col in value_cols:
            prev = group[col].shift(1)
            with np.errstate(divide='ignore', invalid='ignore'):
                yoy = (group[col] - prev) / abs(prev) * 100.0
            group[col + "_growth_pct"] = yoy.replace([np.inf, -np.inf], np.nan)
        return group
    df = df.groupby(group_col, dropna=False).apply(compute).reset_index(drop=True)
    return df.drop(columns=["period_index"]) if "period_index" in df.columns else df


def add_cagr_columns(df: pd.DataFrame, value_cols: Optional[List[str]] = None, group_col: str = "ticker", years_col: Optional[str] = None, window: Optional[int] = None) -> pd.DataFrame:
    import numpy as np
    from financial_metrics import cagr
    if value_cols is None:
        value_cols = ["revenue", "net_income", "free_cash_flow"]
    df = df.copy()
    def compute(group):
        group = group.sort_values(by=["fiscal_year", "fiscal_quarter", "period_end"])
        n = None
        if years_col and years_col in group.columns:
            try:
                n = float(group[years_col].iloc[-1]) - float(group[years_col].iloc[0])
                if n <= 0:
                    n = None
            except Exception:
                n = None
        for col in value_cols:
            vals = group[col].astype(float).tolist()
            if len(vals) >= 2:
                periods = n if n is not None else max(1, len(vals) - 1)
                try:
                    cg = cagr(vals[0], vals[-1], periods)
                except Exception:
                    cg = None
            else:
                cg = None
            group[col + "_CAGR_pct_total"] = cg
            if window is not None and window >= 2:
                out = []
                for i in range(len(vals)):
                    j = i - window
                    if j < 0:
                        out.append(None)
                    else:
                        try:
                            out.append(cagr(vals[j], vals[i], window))
                        except Exception:
                            out.append(None)
                group[col + "_CAGR_pct_" + str(window) + "y"] = out
        return group
    return df.groupby(group_col, dropna=False).apply(compute).reset_index(drop=True)