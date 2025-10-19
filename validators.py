import re
from typing import Tuple, List, Dict, Any

class TickerValidator:
    """Validate and normalize ticker input"""
    
    def __init__(self):
        # Valid ticker pattern: 1-5 letters, optional dash and letter, optional dot and letter
        self.valid_pattern = re.compile(r'^[A-Z]{1,5}(-[A-Z])?(\.[A-Z])?$')
        
        # Common ticker corrections
        self.corrections = {
            'BRK.A': 'BRK-A',
            'BRK.B': 'BRK-B',
            'BF.A': 'BF-A',
            'BF.B': 'BF-B'
        }
    
    def normalize_ticker(self, raw_input: str) -> Tuple[str, List[str]]:
        """
        Normalize ticker input and return warnings
        Returns: (normalized_ticker, warnings)
        """
        warnings = []
        
        # Basic cleaning
        ticker = raw_input.strip().upper()
        
        # Handle empty input
        if not ticker:
            raise ValueError("Ticker symbol required")
        
        # Handle obvious non-ticker input
        if len(ticker) > 10 or ' ' in ticker:
            raise ValueError(f"'{raw_input}' doesn't look like a ticker symbol. Try 'AAPL' or 'MSFT'")
        
        # Apply known corrections
        if ticker in self.corrections:
            old_ticker = ticker
            ticker = self.corrections[ticker]
            warnings.append(f"Converted {old_ticker} to {ticker}")
        
        # Validate format
        if not self.valid_pattern.match(ticker):
            raise ValueError(f"'{ticker}' is not a valid ticker format")
        
        return ticker, warnings
    
    def validate_ticker(self, ticker: str) -> Dict[str, Any]:
        """
        Comprehensive ticker validation
        Returns validation result with user feedback
        """
        try:
            normalized_ticker, warnings = self.normalize_ticker(ticker)
            return {
                'valid': True,
                'ticker': normalized_ticker,
                'warnings': warnings,
                'message': f"Analyzing {normalized_ticker}..."
            }
        except ValueError as e:
            return {
                'valid': False,
                'ticker': None,
                'warnings': [],
                'message': str(e),
                'suggestions': self._get_suggestions(ticker)
            }
    
    def _get_suggestions(self, invalid_ticker: str) -> List[str]:
        """Provide helpful suggestions for common mistakes"""
        suggestions = []
        
        # If it contains spaces, suggest it might be a company name
        if ' ' in invalid_ticker:
            suggestions.append("Use ticker symbol (e.g., 'AAPL') not company name")
        
        # If it's too long, suggest common tickers
        if len(invalid_ticker) > 5:
            suggestions.append("Most ticker symbols are 1-4 letters (AAPL, MSFT, GOOGL)")
        
        return suggestions
