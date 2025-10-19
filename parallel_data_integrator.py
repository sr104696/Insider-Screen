"""
Parallel Data Integrator - Combines SEC data with web scraping for complete financial tables
Inspired by proven patterns from attached quarterly reports scraper and annual reports scraper
"""

from typing import Dict, List, Any, Optional
from web_financial_scraper import WebFinancialScraper
from enhanced_web_scraper import EnhancedFinancialScraper
from real_data_financial_scraper import RealDataFinancialScraper
from revenue_fallback_system import MultiTierRevenueFallback
import time

class ParallelDataIntegrator:
    def __init__(self, logger=None):
        self.logger = logger
        self.web_scraper = WebFinancialScraper(logger)
        self.enhanced_scraper = EnhancedFinancialScraper(logger)
        self.real_data_scraper = RealDataFinancialScraper(logger)  # NEW: Real live data scraper
        self.fallback_system = MultiTierRevenueFallback()

    def get_complete_financial_data(self, ticker: str) -> Dict[str, Any]:
        """
        MASTER INTEGRATION: Combine SEC data + Web scraping for complete tables
        Based on quarterly reports scraper pattern from attached materials
        """
        if self.logger:
            self.logger.log_comprehensive('parallel_integration_start',
                                        {'ticker': ticker},
                                        ticker=ticker,
                                        agent_context="Starting parallel SEC + Web data integration")
        
        try:
            # Step 1: Get SEC data (existing functionality)
            sec_data = self.fallback_system.safe_extract_revenue({}, ticker)
            
            # Step 2: Get web scraped data (basic functionality)
            web_data = self.web_scraper.scrape_financial_data(ticker)
            
            # Step 3: Get ENTERPRISE web scraped data (ALL advanced capabilities)
            enhanced_data = self.enhanced_scraper.enterprise_scrape_financial_data(ticker)
            
            # Step 4: Get REAL DATA from live sources (Yahoo Finance, MarketWatch, etc.)
            real_data = self.real_data_scraper.extract_real_financial_data(ticker)
            
            # Step 5: Get PDF annual reports data
            pdf_data = self.web_scraper.scrape_annual_reports_pdfs(ticker)
            
            # Step 6: CRITICAL INTEGRATION - Merge all data sources
            integrated_data = self._merge_multiple_data_sources(sec_data, web_data, enhanced_data, real_data, pdf_data, ticker)
            
            # Step 7: Fill gaps and enhance coverage
            final_data = self._fill_data_gaps(integrated_data, ticker)
            
            if self.logger:
                total_annual = len(final_data.get('annual', []))
                total_quarterly = len(final_data.get('quarterly', []))
                self.logger.log_comprehensive('parallel_integration_success',
                                            {'annual_periods': total_annual,
                                             'quarterly_periods': total_quarterly,
                                             'data_sources': len(final_data.get('sources', []))},
                                            ticker=ticker,
                                            agent_context="COMPLETE parallel data integration with REAL LIVE DATA from multiple sources")
            
            return final_data
            
        except Exception as e:
            if self.logger:
                self.logger.log_comprehensive('parallel_integration_error',
                                            {'ticker': ticker, 'error': str(e)},
                                            e, ticker=ticker,
                                            agent_context="Parallel data integration failed")
            return {'annual': [], 'quarterly': [], 'error': f"Integration failed: {str(e)}"}

    def _merge_multiple_data_sources(self, sec_data: Dict, web_data: Dict, enhanced_data: Dict, real_data: Dict, pdf_data: Dict, ticker: str) -> Dict:
        """
        CORE INTEGRATION LOGIC: Merge SEC + Web + PDF data into unified structure
        Inspired by quarterly reports scraper data organization pattern
        """
        merged_data = {
            'annual': [],
            'quarterly': [],
            'sources': [],
            'extraction_methods': [],
            'data_quality': {'completeness': 0, 'coverage_years': set()}
        }
        
        # Merge annual data from all sources INCLUDING REAL LIVE DATA
        all_annual_sources = [
            (sec_data.get('annual', []), 'SEC_EDGAR'),
            (web_data.get('annual', []), 'Web_Scraping'),
            (enhanced_data.get('annual', []), 'Enterprise_Web_Scraping'),
            (real_data.get('annual', []), 'REAL_LIVE_DATA'),  # NEW: Live data from Yahoo Finance, etc.
            (pdf_data.get('annual', []), 'PDF_Reports')
        ]
        
        annual_by_year = {}  # Organize by fiscal year for smart merging
        
        for source_data, source_type in all_annual_sources:
            for period in source_data:
                fiscal_year = period.get('fiscal_year', 0)
                if fiscal_year not in annual_by_year:
                    annual_by_year[fiscal_year] = {
                        'fiscal_year': fiscal_year,
                        'revenue': None,
                        'net_income': None,
                        'sources': [],
                        'extraction_methods': []
                    }
                
                # Merge data intelligently - prefer higher quality sources
                current = annual_by_year[fiscal_year]
                
                # Revenue merging with source priority
                if period.get('value') and period.get('metric') in ['revenue', 'revenues']:
                    if current['revenue'] is None or source_type == 'SEC_EDGAR':
                        current['revenue'] = period.get('value')
                        current['sources'].append(source_type)
                        current['extraction_methods'].append(period.get('extraction_method', source_type))
                
                # Net income merging
                if period.get('value') and period.get('metric') == 'net_income':
                    if current['net_income'] is None or source_type == 'SEC_EDGAR':
                        current['net_income'] = period.get('value')
                
                merged_data['data_quality']['coverage_years'].add(fiscal_year)
        
        # Convert back to list format
        merged_data['annual'] = list(annual_by_year.values())
        
        # Similar process for quarterly data
        quarterly_by_period = {}
        all_quarterly_sources = [
            (sec_data.get('quarterly', []), 'SEC_EDGAR'),
            (web_data.get('quarterly', []), 'Web_Scraping'),
            (enhanced_data.get('quarterly', []), 'Enterprise_Web_Scraping'),
            (real_data.get('quarterly', []), 'REAL_LIVE_DATA'),  # NEW: Live quarterly data
            (pdf_data.get('quarterly', []), 'PDF_Reports')
        ]
        
        for source_data, source_type in all_quarterly_sources:
            for period in source_data:
                fiscal_year = period.get('fiscal_year', 0)
                fiscal_quarter = period.get('fiscal_quarter', 'Q1')
                period_key = f"{fiscal_year}-{fiscal_quarter}"
                
                if period_key not in quarterly_by_period:
                    quarterly_by_period[period_key] = {
                        'fiscal_year': fiscal_year,
                        'fiscal_quarter': fiscal_quarter,
                        'revenue': None,
                        'net_income': None,
                        'sources': [],
                        'extraction_methods': []
                    }
                
                current = quarterly_by_period[period_key]
                
                if period.get('value') and period.get('metric') in ['revenue', 'revenues']:
                    if current['revenue'] is None or source_type == 'SEC_EDGAR':
                        current['revenue'] = period.get('value')
                        current['sources'].append(source_type)
                        current['extraction_methods'].append(period.get('extraction_method', source_type))
        
        merged_data['quarterly'] = list(quarterly_by_period.values())
        
        # Collect all sources INCLUDING REAL LIVE DATA SOURCES
        all_sources = (sec_data.get('sources', []) + 
                      web_data.get('sources', []) + 
                      enhanced_data.get('sources', []) + 
                      real_data.get('sources', []) +  # NEW: Real live data sources
                      pdf_data.get('sources', []))
        merged_data['sources'] = list(set(all_sources))  # Remove duplicates
        
        # Calculate data quality score
        expected_years = 5
        actual_years = len(merged_data['data_quality']['coverage_years'])
        merged_data['data_quality']['completeness'] = min(100, (actual_years / expected_years) * 100)
        
        return merged_data

    def _fill_data_gaps(self, data: Dict, ticker: str) -> Dict:
        """
        SMART GAP FILLING: Use multiple techniques to complete missing data
        Based on patterns from attached materials
        """
        # Sort data by fiscal year (descending)
        data['annual'].sort(key=lambda x: x.get('fiscal_year', 0), reverse=True)
        data['quarterly'].sort(key=lambda x: (x.get('fiscal_year', 0), x.get('fiscal_quarter', 'Q1')), reverse=True)
        
        # Fill revenue gaps using interpolation and estimation
        annual_with_revenue = [p for p in data['annual'] if p.get('revenue')]
        
        if len(annual_with_revenue) >= 2:
            # Calculate growth trends for estimation
            revenues = [p['revenue'] for p in annual_with_revenue[:3]]  # Last 3 years
            if len(revenues) >= 2:
                growth_rate = (revenues[0] / revenues[1] - 1) if revenues[1] != 0 else 0
                
                # Estimate missing data points
                for period in data['annual']:
                    if not period.get('revenue') and annual_with_revenue:
                        latest_revenue = annual_with_revenue[0]['revenue']
                        years_diff = annual_with_revenue[0]['fiscal_year'] - period['fiscal_year']
                        
                        if years_diff > 0:  # Historical data
                            estimated_revenue = latest_revenue / ((1 + growth_rate) ** years_diff)
                            period['revenue'] = int(estimated_revenue)
                            period['sources'] = period.get('sources', []) + ['Estimated']
                            period['extraction_methods'] = period.get('extraction_methods', []) + ['Growth_Trend_Estimation']
        
        # Enhanced quarterly gap filling
        quarterly_with_revenue = [p for p in data['quarterly'] if p.get('revenue')]
        
        # Use annual data to estimate quarterly splits
        for annual_period in data['annual']:
            if annual_period.get('revenue'):
                fiscal_year = annual_period['fiscal_year']
                year_quarters = [q for q in data['quarterly'] if q.get('fiscal_year') == fiscal_year]
                
                if len(year_quarters) < 4:  # Missing quarters
                    quarterly_revenue = annual_period['revenue'] / 4  # Simple equal split
                    
                    for quarter in ['Q1', 'Q2', 'Q3', 'Q4']:
                        existing = next((q for q in year_quarters if q.get('fiscal_quarter') == quarter), None)
                        if not existing:
                            data['quarterly'].append({
                                'fiscal_year': fiscal_year,
                                'fiscal_quarter': quarter,
                                'revenue': int(quarterly_revenue),
                                'sources': ['Annual_Split_Estimation'],
                                'extraction_methods': ['Annual_to_Quarterly_Split']
                            })
        
        return data