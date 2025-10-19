# Final integration pipeline combining scraped data with offline EDGAR datasets, with validation and deduping.

from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np
from financial_tabulator import tabulate, add_growth_columns, add_cagr_columns

KEY = ["ticker", "fiscal_year", "fiscal_quarter", "period_end", "currency"]

class IntegrationPipeline:
    def __init__(self, value_cols: Optional[List[str]] = None):
        self.value_cols = value_cols or ["revenue", "gross_profit", "operating_income", "net_income", "operating_cash_flow", "capex", "free_cash_flow"]

    def dataframe_from_scraped(self, scraped_records: List[Dict[str, Any]]) -> pd.DataFrame:
        df = tabulate(scraped_records)
        return self._post(df)

    def dataframe_from_offline_edgar(self, edgar_records: List[Dict[str, Any]]) -> pd.DataFrame:
        df = tabulate(edgar_records)
        return self._post(df)

    def _post(self, df: pd.DataFrame) -> pd.DataFrame:
        # Basic cleaning: drop full-empty rows in value columns
        if not len(df):
            return df
        valcols = [c for c in self.value_cols if c in df.columns]
        if valcols:
            df = df.dropna(axis=0, how="all", subset=valcols)
        # Remove duplicates by max non-null count heuristic
        df["_nonnull"] = df[valcols].notna().sum(axis=1) if valcols else 0
        df = df.sort_values(by=KEY + ["_nonnull"], na_position="last").drop_duplicates(subset=KEY, keep="last").drop(columns=["_nonnull"]) if len(df) else df
        return df.reset_index(drop=True)

    def combine(self, scraped_df: pd.DataFrame, edgar_df: pd.DataFrame, prefer: str = "edgar") -> pd.DataFrame:
        s = scraped_df.copy()
        e = edgar_df.copy()
        s.columns = [c + "__scraped" if c not in KEY else c for c in s.columns]
        e.columns = [c + "__edgar" if c not in KEY else c for c in e.columns]
        merged = pd.merge(s, e, on=KEY, how="outer")
        # Resolve value columns
        for col in self.value_cols:
            sc = col + "__scraped"
            ec = col + "__edgar"
            if sc in merged.columns or ec in merged.columns:
                if prefer == "edgar":
                    merged[col] = merged.get(ec).combine_first(merged.get(sc))
                else:
                    merged[col] = merged.get(sc).combine_first(merged.get(ec))
        # Resolve other fields
        for c in ["company", "shares_outstanding", "total_assets", "total_liabilities", "eps_basic", "eps_diluted"]:
            sc = c + "__scraped"
            ec = c + "__edgar"
            if sc in merged.columns or ec in merged.columns:
                merged[c] = merged.get(ec).combine_first(merged.get(sc)) if prefer == "edgar" else merged.get(sc).combine_first(merged.get(ec))
        keep = [c for c in merged.columns if c in KEY or c in self.value_cols or c in ["company", "shares_outstanding", "total_assets", "total_liabilities", "eps_basic", "eps_diluted"]]
        merged = merged[keep]
        merged = merged.sort_values(by=KEY, na_position="last").reset_index(drop=True)
        # Enrich
        merged = add_growth_columns(merged, value_cols=[c for c in self.value_cols if c in merged.columns])
        merged = add_cagr_columns(merged, value_cols=[c for c in self.value_cols if c in merged.columns], window=3)
        return merged