from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
from data_processor import FinancialDataProcessor

class EnhancedDataProcessor(FinancialDataProcessor):
    """Enhanced processor with industry-specific metrics and comprehensive fact mappings"""
    
    def __init__(self):
        super().__init__()
        
        # Add industry-specific metrics for payment companies
        self.industry_specific_mappings = {
            'payment_volume': [
                'PaymentVolume', 'TotalPaymentVolume', 'ProcessedPaymentVolume',
                'TransactionVolume', 'GrossPaymentVolume'
            ],
            'transaction_count': [
                'TransactionCount', 'NumberOfTransactionsProcessed', 
                'TotalTransactionCount'
            ],
            'processing_fees': [
                'ProcessingAndServiceFees', 'TransactionProcessingFees',
                'PaymentProcessingFees', 'ServiceFees'
            ],
            'cost_of_services': [
                'CostOfGoodsAndServicesSold', 'CostOfServices',
                'CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization'
            ]
        }
        
        # Merge industry-specific mappings
        self.fact_mappings.update(self.industry_specific_mappings)
    
    def analyze_missing_data(self, raw_data: Dict[str, Any]) -> Dict[str, List[str]]:
        """Analyze what data is missing and suggest potential sources"""
        facts = raw_data.get('facts', {}).get('us-gaap', {})
        
        missing_analysis = {
            'missing_core_metrics': [],
            'available_alternatives': [],
            'industry_specific_available': [],
            'data_supplementation_needed': []
        }
        
        # Check core metrics
        core_metrics = ['revenue', 'net_income', 'operating_income', 'eps']
        for metric in core_metrics:
            found = False
            for fact_name in self.fact_mappings[metric]:
                if fact_name in facts:
                    found = True
                    break
            if not found:
                missing_analysis['missing_core_metrics'].append(metric)
        
        # Check for industry-specific metrics
        industry_metrics = ['payment_volume', 'processing_fees', 'transaction_count']
        for metric in industry_metrics:
            if metric in self.fact_mappings:
                for fact_name in self.fact_mappings[metric]:
                    if fact_name in facts:
                        missing_analysis['industry_specific_available'].append(metric)
                        break
        
        return missing_analysis
    
    def suggest_data_sources(self, ticker: str, company_name: str) -> Dict[str, List[str]]:
        """Suggest alternative data sources for missing financial data"""
        
        suggestions = {
            'sec_sources': [
                f"10-K Annual Reports for {company_name}",
                f"10-Q Quarterly Reports for {company_name}",
                f"8-K Current Reports for {company_name}",
                "Proxy statements (DEF 14A)"
            ],
            'free_apis': [
                f"Alpha Vantage API (free tier) - {ticker}",
                f"Financial Modeling Prep (free tier) - {ticker}",
                f"Yahoo Finance API - {ticker}",
                f"Quandl/Nasdaq Data Link - {ticker}"
            ],
            'web_scraping': [
                f"{company_name} Investor Relations page",
                f"Yahoo Finance {ticker} financials page",
                f"MarketWatch {ticker} financials",
                f"Google Finance {ticker}",
                f"SEC EDGAR search for {company_name}"
            ],
            'industry_sources': [
                "Payment industry reports (Nilson Report)",
                "Credit card processing industry data",
                "FinTech industry benchmarks",
                "Payment volume industry statistics"
            ]
        }
        
        return suggestions