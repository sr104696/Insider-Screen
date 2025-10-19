from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

class FinancialDataProcessor:
    """Process raw SEC financial data into organized format"""
    
    def __init__(self):
        # Map SEC fact names to our standardized names
        self.fact_mappings = {
            'revenue': [
                # Standard revenue tags
                'Revenues', 'SalesRevenueNet', 'TotalRevenuesAndOtherIncome',
                # Contract-based revenue (common for service companies)
                'RevenueFromContractWithCustomerExcludingAssessedTax',
                'RevenueFromContractWithCustomerIncludingAssessedTax',
                # Service revenue (payment companies)
                'ServiceRevenues', 'RevenueNotFromContractWithCustomerExcludingInterestIncome',
                # Payment-specific revenue
                'ProcessingAndServiceFees', 'PaymentProcessingRevenues', 'TransactionProcessingRevenues'
            ],
            'gross_profit': [
                'GrossProfit', 'GrossProfitLoss'
            ],
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
            ]
        }
    
    def process_financial_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process raw SEC company facts into organized annual and quarterly data"""
        try:
            # Extract US-GAAP facts
            facts = raw_data.get('facts', {}).get('us-gaap', {})
            if not facts:
                logging.warning("No US-GAAP facts found in data")
                return {
                    'annual_data': {},
                    'quarterly_data': {},
                    'data_quality': self._assess_data_quality({}, {})
                }
            
            # Process each metric
            annual_data = {}
            quarterly_data = {}
            
            for metric, fact_names in self.fact_mappings.items():
                # Find the best fact name for this metric
                fact_data = None
                for fact_name in fact_names:
                    if fact_name in facts:
                        fact_data = facts[fact_name]
                        break
                
                if not fact_data:
                    continue
                
                # Extract annual and quarterly values
                annual_values = self._extract_annual_data(fact_data)
                quarterly_values = self._extract_quarterly_data(fact_data)
                
                # Merge into main data structures
                for year, value in annual_values.items():
                    if year not in annual_data:
                        annual_data[year] = {}
                    annual_data[year][metric] = value
                
                for quarter, value in quarterly_values.items():
                    if quarter not in quarterly_data:
                        quarterly_data[quarter] = {}
                    quarterly_data[quarter][metric] = value
            
            # Assess data quality
            data_quality = self._assess_data_quality(annual_data, quarterly_data)
            
            return {
                'annual_data': annual_data,
                'quarterly_data': quarterly_data,
                'data_quality': data_quality
            }
            
        except Exception as e:
            logging.error(f"Error processing financial data: {str(e)}")
            return {
                'annual_data': {},
                'quarterly_data': {},
                'data_quality': self._assess_data_quality({}, {})
            }
    
    def _extract_annual_data(self, fact_data: Dict[str, Any]) -> Dict[str, float]:
        """Extract annual data from a fact"""
        annual_values = {}
        
        try:
            units = fact_data.get('units', {})
            
            # Try different unit types (USD, shares, etc.)
            for unit_type, unit_data in units.items():
                if not isinstance(unit_data, list):
                    continue
                
                for entry in unit_data:
                    # Look for annual data (10-K forms typically)
                    form = entry.get('form', '')
                    if '10-K' not in form and '10-Q' in form:
                        continue  # Skip quarterly forms for annual data
                    
                    # Get fiscal year
                    fy = entry.get('fy')
                    if not fy:
                        continue
                    
                    # Get value
                    val = entry.get('val')
                    if val is None:
                        continue
                    
                    try:
                        value = float(val)
                        year = str(fy)
                        
                        # Use the most recent value for each year
                        if year not in annual_values or self._is_more_recent(entry, annual_values.get(f'{year}_meta', {})):
                            annual_values[year] = value
                            annual_values[f'{year}_meta'] = entry
                    except (ValueError, TypeError):
                        continue
            
            # Clean up metadata
            annual_values = {k: v for k, v in annual_values.items() if not k.endswith('_meta')}
            
        except Exception as e:
            logging.error(f"Error extracting annual data: {str(e)}")
        
        return annual_values
    
    def _extract_quarterly_data(self, fact_data: Dict[str, Any]) -> Dict[str, float]:
        """Extract quarterly data from a fact"""
        quarterly_values = {}
        
        try:
            units = fact_data.get('units', {})
            
            # Try different unit types
            for unit_type, unit_data in units.items():
                if not isinstance(unit_data, list):
                    continue
                
                for entry in unit_data:
                    # Get fiscal year and period
                    fy = entry.get('fy')
                    fp = entry.get('fp')
                    
                    if not fy or not fp:
                        continue
                    
                    # Skip annual data
                    if fp == 'FY':
                        continue
                    
                    # Get value
                    val = entry.get('val')
                    if val is None:
                        continue
                    
                    try:
                        value = float(val)
                        quarter = f"{fy}-{fp}"
                        
                        # Use the most recent value for each quarter
                        if quarter not in quarterly_values or self._is_more_recent(entry, quarterly_values.get(f'{quarter}_meta', {})):
                            quarterly_values[quarter] = value
                            quarterly_values[f'{quarter}_meta'] = entry
                    except (ValueError, TypeError):
                        continue
            
            # Clean up metadata
            quarterly_values = {k: v for k, v in quarterly_values.items() if not k.endswith('_meta')}
            
        except Exception as e:
            logging.error(f"Error extracting quarterly data: {str(e)}")
        
        return quarterly_values
    
    def _is_more_recent(self, entry1: Dict[str, Any], entry2: Dict[str, Any]) -> bool:
        """Check if entry1 is more recent than entry2"""
        try:
            date1 = entry1.get('filed', entry1.get('end', ''))
            date2 = entry2.get('filed', entry2.get('end', ''))
            return date1 > date2
        except:
            return True
    
    def _assess_data_quality(self, annual_data: Dict, quarterly_data: Dict) -> Dict[str, Any]:
        """Assess the quality and completeness of the processed data"""
        quality = {
            'annual_completeness': 0,
            'quarterly_completeness': 0,
            'warnings': [],
            'missing_metrics': []
        }
        
        # Check annual data completeness
        expected_metrics = ['revenue', 'net_income', 'eps']
        current_year = datetime.now().year
        expected_years = [str(y) for y in range(current_year - 5, current_year)]
        
        annual_score = 0
        for year in expected_years:
            if year in annual_data:
                year_score = sum(1 for metric in expected_metrics if annual_data[year].get(metric) is not None)
                annual_score += year_score / len(expected_metrics)
        
        quality['annual_completeness'] = annual_score / len(expected_years) if expected_years else 0
        
        # Check quarterly data completeness
        expected_quarters = 20  # Last 5 years * 4 quarters
        actual_quarters = len(quarterly_data)
        quality['quarterly_completeness'] = min(actual_quarters / expected_quarters, 1.0) if expected_quarters > 0 else 0
        
        # Generate warnings
        if quality['annual_completeness'] < 0.6:
            quality['warnings'].append("Limited annual data available - some calculations may be incomplete")
        
        if quality['quarterly_completeness'] < 0.6:
            quality['warnings'].append("Limited quarterly data available - QoQ analysis may be incomplete")
        
        # Check for missing key metrics
        all_metrics = set()
        for year_data in annual_data.values():
            all_metrics.update(year_data.keys())
        
        for metric in expected_metrics:
            if metric not in all_metrics:
                quality['missing_metrics'].append(metric)
        
        if quality['missing_metrics']:
            quality['warnings'].append(f"Missing key metrics: {', '.join(quality['missing_metrics'])}")
        
        return quality
