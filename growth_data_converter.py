"""
Growth Data Converter - Prepares canonical data for growth calculations
Converts normalized PeriodBase and QuarterlyPeriod objects to format expected by GrowthCalculator
"""

from typing import List, Dict, Any
from models import PeriodBase, QuarterlyPeriod

def prepare_growth_calculation_data(annual_periods: List[PeriodBase], quarterly_periods: List[QuarterlyPeriod]) -> Dict[str, Any]:
    """
    Convert canonical period objects to format expected by GrowthCalculator
    
    Growth calculator expects:
    {
        'annual_data': {
            '2023': {'revenue': 1000000, 'net_income': 50000},
            '2022': {'revenue': 900000, 'net_income': 45000}
        },
        'quarterly_data': {
            '2023-Q1': {'revenue': 250000},
            '2023-Q2': {'revenue': 260000}
        }
    }
    """
    
    # Convert annual periods
    annual_data = {}
    for period in annual_periods:
        year_key = str(period.fiscal_year)
        annual_data[year_key] = {
            'revenue': period.revenue,
            'net_income': period.net_income,
            'eps': period.eps,
            'assets': period.assets,
            'liabilities': period.liabilities,
            'cash_flow': period.cash_flow,
            'gross_profit': period.gross_profit,
            'operating_income': period.operating_income
        }
        # Remove None values to avoid growth calculation issues
        annual_data[year_key] = {k: v for k, v in annual_data[year_key].items() if v is not None}
    
    # Convert quarterly periods
    quarterly_data = {}
    for period in quarterly_periods:
        quarter_key = f"{period.fiscal_year}-{period.fiscal_quarter}"
        quarterly_data[quarter_key] = {
            'revenue': period.revenue,
            'net_income': period.net_income,
            'eps': period.eps,
            'assets': period.assets,
            'liabilities': period.liabilities,
            'cash_flow': period.cash_flow,
            'gross_profit': period.gross_profit,
            'operating_income': period.operating_income
        }
        # Remove None values to avoid growth calculation issues
        quarterly_data[quarter_key] = {k: v for k, v in quarterly_data[quarter_key].items() if v is not None}
    
    return {
        'annual_data': annual_data,
        'quarterly_data': quarterly_data
    }