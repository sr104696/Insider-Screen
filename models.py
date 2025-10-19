"""
Canonical Financial Data Schema - Single Source of Truth
All processors must normalize to this schema before returning data
Enforced with Pydantic for runtime validation and clear error messages
"""

from typing import List, Optional, Literal
from pydantic import BaseModel, Field
from datetime import datetime

SchemaVersion = Literal["1.0"]

class PeriodBase(BaseModel):
    """Base financial period with core metrics"""
    fiscal_year: int
    revenue: Optional[float] = None
    net_income: Optional[float] = None
    eps: Optional[float] = None
    assets: Optional[float] = None
    liabilities: Optional[float] = None
    cash_flow: Optional[float] = None
    gross_profit: Optional[float] = None
    operating_income: Optional[float] = None

class QuarterlyPeriod(PeriodBase):
    """Quarterly period with quarter specification"""
    fiscal_quarter: Literal["Q1", "Q2", "Q3", "Q4"]

class Periods(BaseModel):
    """Container for all financial periods"""
    annual: List[PeriodBase] = Field(default_factory=list)
    quarterly: List[QuarterlyPeriod] = Field(default_factory=list)

class Metadata(BaseModel):
    """Processing metadata and source tracking"""
    source: str
    processed_at: datetime
    schema_version: SchemaVersion = "1.0"

class FinancialData(BaseModel):
    """
    CANONICAL SCHEMA - All processors must emit this exact structure
    Growth calculator and all downstream components depend ONLY on this
    """
    ticker: str
    company_name: str
    periods: Periods
    metadata: Metadata
    
    class Config:
        """Pydantic configuration for strict validation"""
        validate_assignment = True
        extra = "forbid"  # Reject unknown fields

def safe_float(value) -> Optional[float]:
    """Helper to safely convert SEC raw data to float"""
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError, AttributeError):
        return None

def normalize_quarter(quarter_str) -> Optional[str]:
    """Helper to normalize quarter formats to Q1, Q2, Q3, Q4"""
    if not quarter_str:
        return None
    
    quarter_str = str(quarter_str).upper().strip()
    
    # Handle various formats
    if quarter_str in ["Q1", "Q2", "Q3", "Q4"]:
        return quarter_str
    elif quarter_str in ["1", "01"]:
        return "Q1"
    elif quarter_str in ["2", "02"]:
        return "Q2"
    elif quarter_str in ["3", "03"]:
        return "Q3"
    elif quarter_str in ["4", "04"]:
        return "Q4"
    elif "Q1" in quarter_str:
        return "Q1"
    elif "Q2" in quarter_str:
        return "Q2"
    elif "Q3" in quarter_str:
        return "Q3"
    elif "Q4" in quarter_str:
        return "Q4"
    
    return None