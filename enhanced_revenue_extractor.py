"""
Enhanced Revenue Extractor using Agent-Enforced Error Boundaries
Implements holistic debugging paradigms for robust SEC data extraction
"""

from typing import Dict, Any, List, Optional
import logging
from replit_safe_error_handler import SECPipelineSafetyManager

logger = logging.getLogger(__name__)

class EnhancedRevenueExtractor:
    """
    Agent-friendly revenue extractor with comprehensive error boundaries
    Implements the paradigms from user's debugging guidance
    """
    
    def __init__(self):
        self.safety_manager = SECPipelineSafetyManager()
    
    def extract_robust_revenue_data(self, company_facts: Dict, ticker: str) -> Dict[str, Any]:
        """
        Extract revenue data using Agent-Enforced Error Boundaries
        Returns structured annual and quarterly revenue with data quality assessment
        """
        
        # Log the extraction attempt
        self.safety_manager.logger.log_pipeline_event(
            "enhanced_revenue_extraction_start", ticker, {"facts_available": bool(company_facts)}
        )
        
        try:
            # Use the proven safety boundary method
            revenue_result = self.safety_manager.error_boundaries.safe_extract_revenue(company_facts)
            
            if not revenue_result['success']:
                self.safety_manager.logger.log_pipeline_event(
                    "revenue_extraction_failed", ticker, revenue_result, revenue_result['error']
                )
                return {
                    'success': False,
                    'error': revenue_result['error'],
                    'annual_revenue': [],
                    'quarterly_revenue': [],
                    'data_quality': 'poor'
                }
            
            # Process the successful extraction
            revenue_entries = revenue_result['result']['revenue_entries']
            fact_name = revenue_result['result']['fact_name']
            
            # Separate into annual and quarterly with deduplication
            annual_revenue = []
            quarterly_revenue = []
            
            # Track processed periods to avoid duplicates (SEC data has overlapping entries)
            processed_annual = set()
            processed_quarterly = set()
            
            for entry in revenue_entries:
                frame = entry.get('frame')
                if frame:  # Skip framed entries (contextual/cumulative)
                    continue
                    
                fiscal_period = entry.get('fp')
                fiscal_year = entry.get('fy')
                value = entry.get('val')
                end_date = entry.get('end')
                
                if not all([fiscal_period, fiscal_year, value, end_date]):
                    continue
                
                # Annual data (FY periods)
                if fiscal_period == 'FY':
                    period_key = f"{fiscal_year}-FY-{end_date}"
                    if period_key not in processed_annual:
                        annual_revenue.append({
                            'fiscal_year': fiscal_year,
                            'value': value,
                            'end_date': end_date,
                            'fact_name': fact_name
                        })
                        processed_annual.add(period_key)
                
                # Quarterly data (Q1, Q2, Q3, Q4 periods)  
                elif fiscal_period in ['Q1', 'Q2', 'Q3', 'Q4']:
                    period_key = f"{fiscal_year}-{fiscal_period}-{end_date}"
                    if period_key not in processed_quarterly:
                        quarterly_revenue.append({
                            'fiscal_year': fiscal_year,
                            'fiscal_quarter': fiscal_period,
                            'value': value,
                            'end_date': end_date,
                            'fact_name': fact_name
                        })
                        processed_quarterly.add(period_key)
            
            # Sort by fiscal year (most recent first)
            annual_revenue.sort(key=lambda x: x['fiscal_year'], reverse=True)
            quarterly_revenue.sort(key=lambda x: (x['fiscal_year'], x['fiscal_quarter']), reverse=True)
            
            # Assess data quality
            data_quality = self._assess_revenue_quality(annual_revenue, quarterly_revenue)
            
            result = {
                'success': True,
                'annual_revenue': annual_revenue,
                'quarterly_revenue': quarterly_revenue,
                'data_quality': data_quality,
                'fact_used': fact_name,
                'total_periods': len(annual_revenue) + len(quarterly_revenue)
            }
            
            self.safety_manager.logger.log_pipeline_event(
                "enhanced_revenue_extraction_success", ticker, 
                {
                    'annual_periods': len(annual_revenue),
                    'quarterly_periods': len(quarterly_revenue),
                    'fact_used': fact_name
                }
            )
            
            return result
            
        except Exception as e:
            error_msg = f"Enhanced revenue extraction failed: {str(e)}"
            logger.error(error_msg)
            
            self.safety_manager.logger.log_pipeline_event(
                "enhanced_revenue_extraction_error", ticker, {"error": str(e)}, str(e)
            )
            
            return {
                'success': False,
                'error': error_msg,
                'annual_revenue': [],
                'quarterly_revenue': [],
                'data_quality': 'failed'
            }
    
    def _assess_revenue_quality(self, annual_revenue: List[Dict], quarterly_revenue: List[Dict]) -> str:
        """Assess the quality of extracted revenue data"""
        
        if not annual_revenue and not quarterly_revenue:
            return 'no_data'
        
        if len(annual_revenue) >= 5 and len(quarterly_revenue) >= 12:
            return 'excellent'
        elif len(annual_revenue) >= 3 and len(quarterly_revenue) >= 8:
            return 'good' 
        elif len(annual_revenue) >= 2 or len(quarterly_revenue) >= 4:
            return 'fair'
        else:
            return 'poor'