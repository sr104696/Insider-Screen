"""
Data Format Converter for Multi-Tier Fallback System
Converts various fallback extraction formats to pipeline-compatible raw_data format
"""

from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)

def convert_fallback_to_raw_data(original_raw_data: Dict, fallback_result: Dict, ticker: str) -> Dict:
    """
    Convert multi-tier fallback results to raw_data format expected by pipeline
    Maintains compatibility with existing normalization functions
    """
    
    if not fallback_result or not (fallback_result.get('annual') or fallback_result.get('quarterly')):
        logger.warning(f"No fallback data to convert for {ticker}")
        return original_raw_data
    
    try:
        # Create enhanced raw_data structure
        enhanced_raw_data = original_raw_data.copy()
        
        # Convert annual fallback data to raw_data format
        if fallback_result.get('annual'):
            annual_entries = []
            for period in fallback_result['annual']:
                annual_entries.append({
                    'metric': 'Revenues',  # Use standard SEC fact name that normalization recognizes
                    'value': period.get('value', 0),
                    'fiscal_year': period.get('fiscal_year'),
                    'end_date': period.get('end_date', f"{period.get('fiscal_year', 2023)}-12-31"),
                    'extraction_method': period.get('extraction_method', 'fallback'),
                    'source': fallback_result.get('extraction_method', 'multi_tier_fallback')
                })
            
            # Replace existing revenue entries in annual data
            if 'annual_data' in enhanced_raw_data:
                # Remove existing revenue entries
                enhanced_raw_data['annual_data'] = [
                    entry for entry in enhanced_raw_data['annual_data'] 
                    if 'revenue' not in entry.get('metric', '').lower()
                ]
                # Add fallback revenue entries
                enhanced_raw_data['annual_data'].extend(annual_entries)
            else:
                enhanced_raw_data['annual_data'] = annual_entries
        
        # Convert quarterly fallback data to raw_data format  
        if fallback_result.get('quarterly'):
            quarterly_entries = []
            for period in fallback_result['quarterly']:
                quarterly_entries.append({
                    'metric': 'Revenues',  # Use standard SEC fact name that normalization recognizes
                    'value': period.get('value', 0),
                    'fiscal_year': period.get('fiscal_year'),
                    'fiscal_quarter': period.get('fiscal_quarter', 'Q1'),
                    'end_date': period.get('end_date', f"{period.get('fiscal_year', 2023)}-03-31"),
                    'extraction_method': period.get('extraction_method', 'fallback'),
                    'source': fallback_result.get('extraction_method', 'multi_tier_fallback')
                })
            
            # Replace existing revenue entries in quarterly data
            if 'quarterly_data' in enhanced_raw_data:
                # Remove existing revenue entries
                enhanced_raw_data['quarterly_data'] = [
                    entry for entry in enhanced_raw_data['quarterly_data']
                    if 'revenue' not in entry.get('metric', '').lower()
                ]
                # Add fallback revenue entries
                enhanced_raw_data['quarterly_data'].extend(quarterly_entries)
            else:
                enhanced_raw_data['quarterly_data'] = quarterly_entries
        
        logger.info(f"✅ Successfully converted fallback data for {ticker}: "
                   f"{len(fallback_result.get('annual', []))} annual, "
                   f"{len(fallback_result.get('quarterly', []))} quarterly periods")
        
        return enhanced_raw_data
        
    except Exception as e:
        logger.error(f"❌ Failed to convert fallback data for {ticker}: {str(e)}")
        return original_raw_data  # Return original data on conversion failure