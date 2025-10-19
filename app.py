import os
import logging
from flask import Flask, render_template, request, jsonify, make_response, redirect, url_for
from sec_client import SECClient
from validators import TickerValidator
from data_processor import FinancialDataProcessor
from growth_calculator import GrowthCalculator
from error_logger import error_logger, ErrorCategory, ErrorLevel, DetailLevel, log_user_error, log_critical
from replit_safe_monitor import replit_monitor
from session_manager import session_manager
import csv
import io
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.DEBUG)

# Create Flask app
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "dev-secret-key-change-in-production")

# Initialize components
sec_client = SECClient()
validator = TickerValidator()
data_processor = FinancialDataProcessor()
growth_calculator = GrowthCalculator()

@app.route('/')
def index():
    """Main analysis page"""
    return render_template('index.html')

@app.route('/analyze', methods=['POST'])
def analyze():
    """Analyze a company's financial data"""
    try:
        # Get and validate ticker
        ticker = request.form.get('ticker', '').strip()
        if not ticker:
            log_user_error(
                "Empty ticker submission",
                {"form_data": dict(request.form), "user_ip": request.remote_addr},
                DetailLevel.STANDARD
            )
            return render_template('error.html', 
                                 error="Please enter a ticker symbol",
                                 ticker="")
        
        # Validate ticker format
        validation_result = validator.validate_ticker(ticker)
        if not validation_result['valid']:
            log_user_error(
                f"Invalid ticker format: {ticker}",
                {
                    "ticker": ticker,
                    "validation_result": validation_result,
                    "user_ip": request.remote_addr
                },
                DetailLevel.DETAILED
            )
            return render_template('error.html',
                                 error=validation_result['message'],
                                 suggestions=validation_result.get('suggestions', []),
                                 ticker=ticker)
        
        normalized_ticker = validation_result['ticker']
        
        # Get company information
        error_logger.log_error(
            f"Starting company lookup for ticker: {normalized_ticker}",
            ErrorCategory.SEC_API,
            ErrorLevel.INFO,
            DetailLevel.DETAILED,
            {"normalized_ticker": normalized_ticker, "original_ticker": ticker, "user_ip": request.remote_addr}
        )
        
        company_info = sec_client.get_company_info(normalized_ticker)
        if not company_info:
            error_logger.log_error(
                f"Company not found for ticker: {normalized_ticker}",
                ErrorCategory.SEC_API,
                ErrorLevel.ERROR,
                DetailLevel.DETAILED,
                {"ticker": normalized_ticker, "client_stats": sec_client.session_stats}
            )
            return render_template('error.html',
                                 error=f"Company not found for ticker '{normalized_ticker}'. Please verify the ticker symbol.",
                                 ticker=normalized_ticker)
        
        # Get financial data
        error_logger.log_error(
            f"Fetching financial data for company: {company_info['name']}",
            ErrorCategory.SEC_API,
            ErrorLevel.INFO,
            DetailLevel.DETAILED,
            {"cik": company_info['cik'], "company_name": company_info['name'], "ticker": normalized_ticker}
        )
        
        financial_data = sec_client.get_company_facts(company_info['cik'])
        if not financial_data:
            error_logger.log_error(
                f"No financial data available for company: {company_info['name']}",
                ErrorCategory.SEC_API,
                ErrorLevel.ERROR,
                DetailLevel.DETAILED,
                {"cik": company_info['cik'], "company_name": company_info['name'], "client_stats": sec_client.session_stats}
            )
            return render_template('error.html',
                                 error=f"No financial data available for {company_info['name']}. This may be a newer company or have limited SEC filings.",
                                 ticker=normalized_ticker)
        
        # Process and organize the data
        error_logger.log_error(
            f"Processing financial data for {company_info['name']}",
            ErrorCategory.DATA_PROCESSING,
            ErrorLevel.INFO,
            DetailLevel.STANDARD,
            {"data_size": len(str(financial_data)), "company": company_info['name']}
        )
        
        processed_data = data_processor.process_financial_data(financial_data)
        
        if not processed_data['annual_data'] and not processed_data['quarterly_data']:
            error_logger.log_data_processing_error(
                "No meaningful financial data extracted",
                "financial_statements",
                f"Both annual and quarterly data empty for {company_info['name']}",
                processed_data,
                DetailLevel.FORENSIC
            )
            return render_template('error.html',
                                 error=f"Unable to extract meaningful financial data for {company_info['name']}. The company may have non-standard reporting or insufficient data.",
                                 ticker=normalized_ticker)
        
        # Calculate growth metrics
        error_logger.log_error(
            f"Calculating growth metrics for {company_info['name']}",
            ErrorCategory.CALCULATION,
            ErrorLevel.INFO,
            DetailLevel.STANDARD,
            {
                "annual_periods": len(processed_data['annual_data']),
                "quarterly_periods": len(processed_data['quarterly_data']),
                "data_quality": processed_data['data_quality']
            }
        )
        
        growth_metrics = growth_calculator.calculate_all_growth_metrics(processed_data)
        
        # Prepare data for display
        display_data = {
            'company_info': company_info,
            'annual_data': processed_data['annual_data'],
            'quarterly_data': processed_data['quarterly_data'],
            'growth_metrics': growth_metrics,
            'data_quality': processed_data['data_quality'],
            'ticker': normalized_ticker
        }
        
        return render_template('results.html', **display_data)
        
    except Exception as e:
        log_critical(
            f"Critical application error during analysis of {ticker}",
            ErrorCategory.SYSTEM,
            {
                "ticker": ticker,
                "normalized_ticker": normalized_ticker if 'normalized_ticker' in locals() else None,
                "user_ip": request.remote_addr,
                "error_summary": error_logger.get_error_summary()
            }
        )
        return render_template('error.html',
                             error=f"An error occurred while analyzing {ticker}. Please try again.",
                             ticker=ticker)

@app.route('/export/<data_type>/<ticker>')
def export_csv(data_type, ticker):
    """Export data to CSV"""
    try:
        # Re-fetch and process data for export
        company_info = sec_client.get_company_info(ticker)
        if not company_info:
            return "Company not found", 404
            
        financial_data = sec_client.get_company_facts(company_info['cik'])
        if not financial_data:
            return "No financial data available", 404
            
        processed_data = data_processor.process_financial_data(financial_data)
        
        # Create CSV content
        output = io.StringIO()
        writer = csv.writer(output)
        
        if data_type == 'annual':
            # Annual data CSV
            writer.writerow(['Year', 'Revenue', 'Gross Profit', 'Operating Income', 'Net Income', 'EPS'])
            for year, data in processed_data['annual_data'].items():
                writer.writerow([
                    year,
                    data.get('revenue', ''),
                    data.get('gross_profit', ''),
                    data.get('operating_income', ''),
                    data.get('net_income', ''),
                    data.get('eps', '')
                ])
        elif data_type == 'quarterly':
            # Quarterly data CSV
            writer.writerow(['Quarter', 'Revenue', 'Gross Profit', 'Operating Income', 'Net Income', 'EPS'])
            for quarter, data in processed_data['quarterly_data'].items():
                writer.writerow([
                    quarter,
                    data.get('revenue', ''),
                    data.get('gross_profit', ''),
                    data.get('operating_income', ''),
                    data.get('net_income', ''),
                    data.get('eps', '')
                ])
        
        # Create response
        response = make_response(output.getvalue())
        response.headers['Content-Type'] = 'text/csv'
        response.headers['Content-Disposition'] = f'attachment; filename={ticker}_{data_type}_data.csv'
        
        return response
        
    except Exception as e:
        log_critical(
            f"Critical error during CSV export",
            ErrorCategory.SYSTEM,
            {
                "data_type": data_type,
                "ticker": ticker,
                "error": str(e)
            }
        )
        return "Export failed", 500

@app.errorhandler(404)
def not_found(error):
    return render_template('error.html', 
                         error="Page not found",
                         ticker=""), 404

@app.errorhandler(500)
def internal_error(error):
    return render_template('error.html',
                         error="Internal server error. Please try again.",
                         ticker=""), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
