"""
Helper methods for integrating enhanced revenue extraction
Implements Agent-friendly data merging patterns
"""

from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

def merge_enhanced_revenue_data(raw_data: Dict[str, Any], enhanced_revenue: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge enhanced revenue extraction results into existing raw data structure
    Follows Agent-Enforced Error Boundaries paradigm
    """
    
    if not enhanced_revenue['success']:
        logger.warning("Cannot merge failed revenue extraction")
        return raw_data
    
    try:
        # Get annual and quarterly data from enhanced extraction
        enhanced_annual = enhanced_revenue['annual_revenue']
        enhanced_quarterly = enhanced_revenue['quarterly_revenue']
        
        # Convert enhanced data to raw_data format
        enhanced_annual_formatted = []
        for revenue_entry in enhanced_annual:
            enhanced_annual_formatted.append({
                'metric': 'RevenueFromContractWithCustomerExcludingAssessedTax',
                'value': revenue_entry['value'],
                'fiscal_year': revenue_entry['fiscal_year'],
                'end_date': revenue_entry['end_date']
            })
        
        enhanced_quarterly_formatted = []
        for revenue_entry in enhanced_quarterly:
            enhanced_quarterly_formatted.append({
                'metric': 'RevenueFromContractWithCustomerExcludingAssessedTax', 
                'value': revenue_entry['value'],
                'fiscal_year': revenue_entry['fiscal_year'],
                'fiscal_quarter': revenue_entry['fiscal_quarter'],
                'end_date': revenue_entry['end_date']
            })
        
        # Create enhanced raw_data structure
        enhanced_raw_data = raw_data.copy()
        
        # Replace revenue entries in annual data
        if 'annual_data' in enhanced_raw_data:
            # Remove existing revenue entries
            enhanced_raw_data['annual_data'] = [
                entry for entry in enhanced_raw_data['annual_data'] 
                if 'revenue' not in entry.get('metric', '').lower()
            ]
            # Add enhanced revenue entries
            enhanced_raw_data['annual_data'].extend(enhanced_annual_formatted)
        else:
            enhanced_raw_data['annual_data'] = enhanced_annual_formatted
        
        # Replace revenue entries in quarterly data
        if 'quarterly_data' in enhanced_raw_data:
            # Remove existing revenue entries
            enhanced_raw_data['quarterly_data'] = [
                entry for entry in enhanced_raw_data['quarterly_data']
                if 'revenue' not in entry.get('metric', '').lower()
            ]
            # Add enhanced revenue entries
            enhanced_raw_data['quarterly_data'].extend(enhanced_quarterly_formatted)
        else:
            enhanced_raw_data['quarterly_data'] = enhanced_quarterly_formatted
        
        logger.info(f"✅ Successfully merged enhanced revenue data: {len(enhanced_annual)} annual, {len(enhanced_quarterly)} quarterly periods")
        return enhanced_raw_data
        
    except Exception as e:
        logger.error(f"❌ Failed to merge enhanced revenue data: {str(e)}")
        return raw_data  # Return original data on merge failure