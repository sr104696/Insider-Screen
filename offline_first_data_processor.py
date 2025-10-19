"""
Offline-First Data Processor - CANONICAL SCHEMA IMPLEMENTATION
Normalizes all SEC data to single canonical schema for downstream compatibility
NO MORE INTERFACE MISMATCHES - enforced with Pydantic validation
"""

from datetime import datetime
from typing import Dict, List, Optional
import logging
from models import FinancialData, Periods, PeriodBase, QuarterlyPeriod, Metadata, safe_float, normalize_quarter
from advanced_replit_logging import AdvancedReplitLogger

logger = logging.getLogger(__name__)

class OfflineFirstDataProcessor:
    """
    CANONICAL PROCESSOR - Always emits FinancialData schema
    Normalizes raw SEC data to contract expected by growth calculator
    """
    
    def __init__(self, edgar_processor):
        self.edgar_processor = edgar_processor
        self.advanced_logger = AdvancedReplitLogger()
        
        # SEC fact name mappings to canonical field names - ENHANCED FOR PAYMENT COMPANIES
        self.fact_mappings = {
            'revenue': [
                # Standard revenue tags
                'Revenues', 'SalesRevenueNet', 'TotalRevenuesAndOtherIncome',
                # Contract-based revenue (common for service companies like Shift4)
                'RevenueFromContractWithCustomerExcludingAssessedTax',
                'RevenueFromContractWithCustomerIncludingAssessedTax',
                # Service revenue (payment companies)
                'ServiceRevenues', 'RevenueNotFromContractWithCustomerExcludingInterestIncome',
                # Payment-specific revenue
                'ProcessingAndServiceFees', 'PaymentProcessingRevenues', 'TransactionProcessingRevenues'
            ],
            'gross_profit': ['GrossProfit', 'GrossProfitLoss'],
            'operating_income': [
                'OperatingIncomeLoss', 
                'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
                'OperatingIncomeLossBeforeIncomeTaxExpenseBenefit',
                'IncomeLossFromOperations'
            ],
            'net_income': [
                'NetIncomeLoss', 'ProfitLoss', 
                'NetIncomeLossAvailableToCommonStockholdersBasic',
                'NetIncomeLossAvailableToCommonStockholdersDiluted',
                'NetIncomeLossAttributableToParent',
                'IncomeLossFromContinuingOperations'
            ],
            'eps': [
                'EarningsPerShareBasic', 'EarningsPerShareDiluted',
                'EarningsPerShareBasicAndDiluted',
                'IncomeLossFromContinuingOperationsPerBasicShare'
            ],
            'assets': ['Assets', 'AssetsTotal', 'AssetsCurrent'],
            'liabilities': ['Liabilities', 'LiabilitiesAndStockholdersEquity'],
            'cash_flow': ['CashAndCashEquivalentsAtCarryingValue', 'Cash', 'CashAndCashEquivalents']
        }
    
    def process_financial_data(self, ticker: str) -> FinancialData:
        """
        CANONICAL CONTRACT ENFORCEMENT
        Always returns FinancialData schema validated with Pydantic
        Growth calculator can depend on this structure with confidence
        """
        # Strategy 4: Start comprehensive operation trace
        trace_id = self.advanced_logger.start_operation_trace(f"process_financial_data", ticker)
        
        logger.info(f"🔄 Processing offline financial data for {ticker}")
        
        # Strategy 1: Contextual snapshot of initial state
        initial_data = {'ticker': ticker, 'operation': 'process_financial_data'}
        self.advanced_logger.log_comprehensive('initialization', initial_data, 
                                             ticker=ticker, agent_context="Processing SEC financial data")
        
        # Get company info for name
        company_info = self.edgar_processor.get_company_info(ticker)
        company_name = company_info.get('name', 'Unknown Company') if company_info else 'Unknown Company'
        
        # Get cached financial data using enhanced revenue extraction
        try:
            raw_data = self.edgar_processor.extract_financial_metrics(ticker)
            if not raw_data:
                error_msg = f"No financial data available for {ticker}"
                logger.warning(f"⚠️ {error_msg}")
                
                # Strategy 2: Log error for clustering analysis
                self.advanced_logger.log_comprehensive('data_extraction_failed', 
                                                     {'ticker': ticker, 'available_data': bool(raw_data)},
                                                     ValueError(error_msg), ticker=ticker)
                
                self.advanced_logger.complete_operation(success=False)
                return self._empty_canonical_result(ticker, company_name)
            
            # Strategy 1: Log successful data extraction
            self.advanced_logger.log_comprehensive('data_extraction_success', 
                                                 {'ticker': ticker, 'data_keys': list(raw_data.keys())},
                                                 ticker=ticker)
        except Exception as e:
            # Strategy 2: Comprehensive error logging with clustering
            self.advanced_logger.log_comprehensive('data_extraction_error', 
                                                 {'ticker': ticker, 'error_type': type(e).__name__},
                                                 e, ticker=ticker)
            self.advanced_logger.complete_operation(success=False)
            raise
        
        # Apply COMPLETE MULTI-TIER FALLBACK SYSTEM with PosixPath fix
        from revenue_fallback_system import MultiTierRevenueFallback
        
        try:
            # Initialize the comprehensive fallback system
            fallback_system = MultiTierRevenueFallback()
            
            # Strategy 3: Trace revenue extraction attempt
            self.advanced_logger.log_comprehensive('multi_tier_extraction_start', 
                                                 {'ticker': ticker, 'has_raw_data': bool(raw_data)},
                                                 ticker=ticker, agent_context="Starting complete multi-tier revenue extraction")
            
            # Load raw company facts directly from cache (fix PosixPath error)
            # Use the proper method to get CIK instead of accessing cache directly
            try:
                # Get CIK from ticker using the processor's method  
                company_info = self.edgar_processor.get_company_info(ticker)
                if company_info and 'cik' in company_info:
                    cik = company_info['cik']
                else:
                    # Fallback: try to load ticker_cik_cache properly
                    import json
                    cache_file = self.edgar_processor.data_dir / 'ticker_cik_mapping.json'
                    if cache_file.exists():
                        with open(cache_file, 'r') as f:
                            ticker_data = json.load(f)
                            # Find ticker in the data structure
                            for entry in ticker_data.values():
                                if entry.get('ticker') == ticker:
                                    cik = str(entry.get('cik', 'unknown'))
                                    break
                            else:
                                cik = 'unknown'
                    else:
                        cik = 'unknown'
            except Exception as cik_error:
                self.advanced_logger.log_comprehensive('cik_lookup_error', 
                                                     {'ticker': ticker, 'error': str(cik_error)},
                                                     cik_error, ticker=ticker)
                cik = 'unknown'
            facts_file = f'./edgar_bulk_data/cache/company_facts/{ticker}_{cik}_facts.json'
            
            # PRIMARY EXTRACTION with PosixPath fix
            primary_result = fallback_system.safe_extract_revenue(facts_file, ticker)
            
            # FALLBACK CASCADE if primary failed
            final_revenue_result = fallback_system.revenue_fallback_cascade(ticker, primary_result)
            
            if final_revenue_result.get('annual') or final_revenue_result.get('quarterly'):
                # SUCCESS: Convert fallback format to raw_data format for integration
                logger.info(f"✅ Multi-tier revenue extraction successful: {final_revenue_result.get('extraction_method', 'unknown')} method")
                
                # Log success with comprehensive details
                self.advanced_logger.log_comprehensive('multi_tier_extraction_success', 
                                                     {'extraction_method': final_revenue_result.get('extraction_method'),
                                                      'annual_periods': len(final_revenue_result.get('annual', [])),
                                                      'quarterly_periods': len(final_revenue_result.get('quarterly', [])),
                                                      'fact_used': final_revenue_result.get('fact_used', 'N/A')},
                                                     ticker=ticker, agent_context="Multi-tier extraction completely successful")
                
                # Convert to raw_data format for pipeline integration
                from fallback_data_converter import convert_fallback_to_raw_data
                enhanced_raw_data = convert_fallback_to_raw_data(raw_data, final_revenue_result, ticker)
                raw_data = enhanced_raw_data
                
            else:
                # COMPLETE FAILURE: Log comprehensive diagnostic information
                logger.warning(f"⚠️ Complete multi-tier revenue extraction failure for {ticker}")
                
                self.advanced_logger.log_comprehensive('multi_tier_extraction_complete_failure', 
                                                     {'tier_results': final_revenue_result.get('tier_results', {}),
                                                      'final_error': final_revenue_result.get('error', 'Unknown'),
                                                      'ticker': ticker},
                                                     ValueError(f"All extraction tiers failed: {final_revenue_result.get('error')}"), 
                                                     ticker=ticker, 
                                                     agent_context="Complete extraction failure - all 4 tiers exhausted")
            
        except Exception as e:
            # Strategy 2: Log comprehensive extraction system error
            self.advanced_logger.log_comprehensive('multi_tier_system_error', 
                                                 {'ticker': ticker, 'error_type': type(e).__name__},
                                                 e, ticker=ticker, 
                                                 agent_context="Multi-tier extraction system encountered critical error")
        
        # Normalize to canonical schema
        annual_periods = self._normalize_annual_data(raw_data['annual_data'])
        quarterly_periods = self._normalize_quarterly_data(raw_data['quarterly_data'])
        
        # CRITICAL: Calculate growth metrics BEFORE creating FinancialData object
        # Growth calculator needs the processed data format, not canonical periods
        from growth_calculator import GrowthCalculator
        from growth_data_converter import prepare_growth_calculation_data
        
        growth_calc = GrowthCalculator()
        
        # Convert canonical periods back to processed format for growth calculations
        processed_data_for_growth = prepare_growth_calculation_data(annual_periods, quarterly_periods)
        growth_metrics = growth_calc.calculate_all_growth_metrics(processed_data_for_growth)
        
        try:
            # Create canonical structure with Pydantic validation
            # Include growth metrics if available
            metadata_dict = {
                "source": "offline_first_processor",
                "processed_at": datetime.utcnow()
            }
            
            # Add growth metrics to metadata if calculated
            if 'growth_metrics' in locals():
                metadata_dict["growth_metrics"] = growth_metrics
                logger.info(f"✅ Growth metrics calculated: {len(growth_metrics.get('annual', {}))} annual, {len(growth_metrics.get('quarterly', {}))} quarterly")
            
            financial_data = FinancialData(
                ticker=ticker,
                company_name=company_name,
                periods=Periods(
                    annual=annual_periods,
                    quarterly=quarterly_periods
                ),
                metadata=Metadata(**metadata_dict)
            )
            
            # Strategy 1: Log successful completion with data quality metrics
            revenue_success_annual = sum(1 for p in annual_periods if p.revenue is not None)
            revenue_success_quarterly = sum(1 for p in quarterly_periods if p.revenue is not None)
            
            completion_data = {
                'annual_periods': len(annual_periods),
                'quarterly_periods': len(quarterly_periods), 
                'revenue_annual_success': revenue_success_annual,
                'revenue_quarterly_success': revenue_success_quarterly,
                'success_rate_annual': revenue_success_annual / len(annual_periods) * 100 if annual_periods else 0,
                'success_rate_quarterly': revenue_success_quarterly / len(quarterly_periods) * 100 if quarterly_periods else 0
            }
            
            self.advanced_logger.log_comprehensive('processing_completed', completion_data, ticker=ticker)
            
            logger.info(f"✅ Canonical data validated: {len(annual_periods)} annual, {len(quarterly_periods)} quarterly periods for {ticker}")
            
            # Strategy 3 & 5: Complete operation and generate insights
            operation_summary = self.advanced_logger.complete_operation(success=True)
            
            return financial_data
            
        except Exception as e:
            # Strategy 2: Log validation error with clustering
            self.advanced_logger.log_comprehensive('pydantic_validation_error', 
                                                 {'ticker': ticker, 'annual_count': len(annual_periods),
                                                  'quarterly_count': len(quarterly_periods)},
                                                 e, ticker=ticker)
            self.advanced_logger.complete_operation(success=False)
            raise
    
    def _normalize_annual_data(self, raw_annual: List[Dict]) -> List[PeriodBase]:
        """Normalize annual data to canonical PeriodBase schema"""
        if not raw_annual:
            return []
        
        # Group by year  
        annual_by_year = {}
        for record in raw_annual:
            end_date = record.get('end_date', '')
            if end_date:
                year = int(end_date[:4])
                if year not in annual_by_year:
                    annual_by_year[year] = {}
                
                metric = record['metric']
                value = record['value']
                annual_by_year[year][metric] = value
        
        # Convert to canonical PeriodBase objects
        annual_periods = []
        for year in sorted(annual_by_year.keys(), reverse=True):
            year_data = annual_by_year[year]
            
            # Map SEC facts to canonical fields
            period = PeriodBase(
                fiscal_year=year,
                revenue=self._extract_metric_value(year_data, self.fact_mappings['revenue']),
                net_income=self._extract_metric_value(year_data, self.fact_mappings['net_income']),
                eps=self._extract_metric_value(year_data, self.fact_mappings['eps']),
                assets=self._extract_metric_value(year_data, self.fact_mappings['assets']),
                liabilities=self._extract_metric_value(year_data, self.fact_mappings['liabilities']),
                cash_flow=self._extract_metric_value(year_data, self.fact_mappings['cash_flow']),
                gross_profit=self._extract_metric_value(year_data, self.fact_mappings['gross_profit']),
                operating_income=self._extract_metric_value(year_data, self.fact_mappings['operating_income'])
            )
            
            annual_periods.append(period)
        
        return annual_periods
    
    def _normalize_quarterly_data(self, raw_quarterly: List[Dict]) -> List[QuarterlyPeriod]:
        """Normalize quarterly data to canonical QuarterlyPeriod schema"""
        if not raw_quarterly:
            return []
        
        # Group by period
        quarterly_by_period = {}
        for record in raw_quarterly:
            end_date = record.get('end_date', '')
            if end_date:
                # Extract year and determine quarter from date
                year = int(end_date[:4])
                month = int(end_date[5:7]) if len(end_date) >= 7 else 12
                
                # Map month to quarter
                if month in [1, 2, 3]:
                    quarter = "Q1"
                elif month in [4, 5, 6]:
                    quarter = "Q2"
                elif month in [7, 8, 9]:
                    quarter = "Q3"
                else:
                    quarter = "Q4"
                
                period_key = f"{year}-{quarter}"
                if period_key not in quarterly_by_period:
                    quarterly_by_period[period_key] = {'year': year, 'quarter': quarter, 'data': {}}
                
                metric = record['metric']
                value = record['value']
                quarterly_by_period[period_key]['data'][metric] = value
        
        # Convert to canonical QuarterlyPeriod objects
        quarterly_periods = []
        for period_key in sorted(quarterly_by_period.keys(), reverse=True):
            period_info = quarterly_by_period[period_key]
            period_data = period_info['data']
            
            period = QuarterlyPeriod(
                fiscal_year=period_info['year'],
                fiscal_quarter=period_info['quarter'],
                revenue=self._extract_metric_value(period_data, self.fact_mappings['revenue']),
                net_income=self._extract_metric_value(period_data, self.fact_mappings['net_income']),
                eps=self._extract_metric_value(period_data, self.fact_mappings['eps']),
                assets=self._extract_metric_value(period_data, self.fact_mappings['assets']),
                liabilities=self._extract_metric_value(period_data, self.fact_mappings['liabilities']),
                cash_flow=self._extract_metric_value(period_data, self.fact_mappings['cash_flow']),
                gross_profit=self._extract_metric_value(period_data, self.fact_mappings['gross_profit']),
                operating_income=self._extract_metric_value(period_data, self.fact_mappings['operating_income'])
            )
            
            quarterly_periods.append(period)
        
        return quarterly_periods
    
    def _extract_metric_value(self, data: Dict, fact_names: List[str]) -> Optional[float]:
        """Extract metric value from SEC fact names, trying each in order"""
        for fact_name in fact_names:
            if fact_name in data:
                return safe_float(data[fact_name])
        return None
    
    def _empty_canonical_result(self, ticker: str, company_name: str) -> FinancialData:
        """Return empty result in canonical schema"""
        return FinancialData(
            ticker=ticker,
            company_name=company_name,
            periods=Periods(annual=[], quarterly=[]),
            metadata=Metadata(
                source="offline_first_processor",
                processed_at=datetime.utcnow()
            )
        )
    
    def bulk_process_tickers(self, tickers: List[str]) -> Dict[str, Dict]:
        """Process multiple tickers in batch"""
        results = {}
        
        logger.info(f"📊 Bulk processing {len(tickers)} tickers...")
        
        for i, ticker in enumerate(tickers):
            logger.info(f"Processing {ticker} ({i+1}/{len(tickers)})")
            results[ticker] = self.process_financial_data(ticker)
        
        logger.info(f"✅ Completed bulk processing of {len(tickers)} tickers")
        return results
    
    def get_available_metrics(self) -> List[str]:
        """Get list of available financial metrics"""
        return list(self.fact_mappings.keys())