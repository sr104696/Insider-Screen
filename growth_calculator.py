from typing import Dict, Any, Optional
import math

class GrowthCalculator:
    """Calculate various growth metrics for financial data"""
    
    def calculate_cagr(self, start_value: float, end_value: float, periods: int) -> Dict[str, Any]:
        """
        Calculate Compound Annual Growth Rate with edge case handling
        """
        try:
            # Handle edge cases
            if periods <= 0:
                return {'value': None, 'display': 'N/A', 'note': 'Invalid period count'}
            
            if start_value == 0:
                if end_value > 0:
                    return {'value': None, 'display': 'N/A', 'note': 'Cannot calculate CAGR from zero base'}
                else:
                    return {'value': 0, 'display': '0.0%', 'note': 'No growth from zero base'}
            
            if start_value < 0 and end_value > 0:
                return {'value': None, 'display': 'Turnaround', 'note': f'From loss to ${end_value:,.0f} profit'}
            
            if start_value > 0 and end_value < 0:
                return {'value': None, 'display': 'Negative', 'note': f'From ${start_value:,.0f} profit to loss'}
            
            if start_value < 0 and end_value < 0:
                # Both negative - calculate based on absolute values but note it's loss reduction/increase
                abs_start = abs(start_value)
                abs_end = abs(end_value)
                if abs_end < abs_start:
                    # Loss is reducing
                    cagr = ((abs_start / abs_end) ** (1/periods)) - 1
                    return {'value': cagr, 'display': f'{cagr*100:.1f}%', 'note': 'Loss reduction rate'}
                else:
                    # Loss is increasing
                    cagr = -(((abs_end / abs_start) ** (1/periods)) - 1)
                    return {'value': cagr, 'display': f'{cagr*100:.1f}%', 'note': 'Loss increase rate'}
            
            # Standard CAGR calculation
            cagr = ((end_value / start_value) ** (1/periods)) - 1
            return {'value': cagr, 'display': f'{cagr*100:.1f}%', 'note': ''}
            
        except (ZeroDivisionError, ValueError, OverflowError):
            return {'value': None, 'display': 'N/A', 'note': 'Calculation error'}
    
    def calculate_yoy_growth(self, current: float, previous: float) -> Dict[str, Any]:
        """Calculate year-over-year growth rate"""
        try:
            if previous == 0:
                if current > 0:
                    return {'value': None, 'display': 'N/A', 'note': 'Cannot calculate YoY from zero base'}
                else:
                    return {'value': 0, 'display': '0.0%', 'note': 'No change from zero'}
            
            if previous < 0 and current > 0:
                return {'value': None, 'display': 'Turnaround', 'note': 'From loss to profit'}
            
            if previous > 0 and current < 0:
                return {'value': None, 'display': 'Negative', 'note': 'From profit to loss'}
            
            growth = (current - previous) / abs(previous)
            return {'value': growth, 'display': f'{growth*100:.1f}%', 'note': ''}
            
        except (ZeroDivisionError, ValueError):
            return {'value': None, 'display': 'N/A', 'note': 'Calculation error'}
    
    def calculate_qoq_growth(self, current: float, previous: float) -> Dict[str, Any]:
        """Calculate quarter-over-quarter growth rate"""
        # Similar logic to YoY but for quarterly data
        return self.calculate_yoy_growth(current, previous)
    
    def calculate_all_growth_metrics(self, processed_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate all growth metrics for the processed financial data"""
        growth_metrics = {
            'annual': {},
            'quarterly': {}
        }
        
        # Calculate annual growth metrics
        annual_data = processed_data.get('annual_data', {})
        if annual_data:
            years = sorted(annual_data.keys())
            
            for metric in ['revenue', 'gross_profit', 'operating_income', 'net_income', 'eps']:
                metric_data = []
                valid_years = []
                
                # Collect valid data points
                for year in years:
                    value = annual_data[year].get(metric)
                    if value is not None:
                        metric_data.append(value)
                        valid_years.append(year)
                
                if len(metric_data) >= 2:
                    # Calculate 5-year CAGR if we have enough data
                    if len(metric_data) >= 5:
                        start_value = metric_data[0]
                        end_value = metric_data[-1]
                        periods = len(metric_data) - 1
                        cagr = self.calculate_cagr(start_value, end_value, periods)
                        growth_metrics['annual'][f'{metric}_cagr'] = cagr
                    
                    # Calculate YoY growth for recent years
                    yoy_growth = []
                    for i in range(1, len(metric_data)):
                        yoy = self.calculate_yoy_growth(metric_data[i], metric_data[i-1])
                        yoy_growth.append({'year': valid_years[i], 'growth': yoy})
                    
                    growth_metrics['annual'][f'{metric}_yoy'] = yoy_growth
        
        # Calculate quarterly growth metrics
        quarterly_data = processed_data.get('quarterly_data', {})
        if quarterly_data:
            quarters = sorted(quarterly_data.keys())
            
            for metric in ['revenue', 'gross_profit', 'operating_income', 'net_income', 'eps']:
                qoq_growth = []
                
                # Get values for this metric across quarters
                values = []
                valid_quarters = []
                
                for quarter in quarters:
                    value = quarterly_data[quarter].get(metric)
                    if value is not None:
                        values.append(value)
                        valid_quarters.append(quarter)
                
                # Calculate QoQ growth
                for i in range(1, len(values)):
                    qoq = self.calculate_qoq_growth(values[i], values[i-1])
                    qoq_growth.append({'quarter': valid_quarters[i], 'growth': qoq})
                
                if qoq_growth:
                    growth_metrics['quarterly'][f'{metric}_qoq'] = qoq_growth
        
        return growth_metrics
