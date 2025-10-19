import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from utils import SECFilingFetcher
import io
import base64
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration must be the first Streamlit command
st.set_page_config(
    page_title="SEC Filing Scraper",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Debug logging
st.write("Python version:", sys.version)
st.write("Pandas version:", pd.__version__)

# Initialize session state
if 'fetcher' not in st.session_state:
    st.session_state.fetcher = SECFilingFetcher()
if 'filings' not in st.session_state:
    st.session_state.filings = None

# Title and description
st.title("SEC Filing Scraper")
st.markdown("""
This application allows you to search and download SEC filings for publicly traded companies.
Enter a ticker symbol and select the filing parameters below.
""")

# Input section in a form
with st.form("search_form"):
    col1, col2 = st.columns(2)

    with col1:
        ticker = st.text_input("Company Ticker Symbol", 
                             help="Enter the company's ticker symbol (e.g., AAPL for Apple)")

        filing_type = st.selectbox("Filing Type",
                                 ["10-K", "10-Q", "8-K", "S-1", "424B"],
                                 help="Select the type of SEC filing you want to retrieve")

    with col2:
        end_date = st.date_input("End Date",
                               datetime.now().date(),
                               help="Select the end date for the filing search")

        start_date = st.date_input("Start Date",
                                 end_date - timedelta(days=365),
                                 help="Select the start date for the filing search")

    submitted = st.form_submit_button("Search Filings")

# Process form submission
if submitted:
    if not ticker:
        st.error("Please enter a ticker symbol")
    else:
        with st.spinner('Fetching SEC filings...'):
            try:
                st.session_state.filings = st.session_state.fetcher.get_filings(
                    ticker.upper(),
                    filing_type,
                    start_date,
                    end_date
                )

                if st.session_state.filings.empty:
                    st.warning("No filings found for the specified criteria")
                else:
                    st.success(f"Found {len(st.session_state.filings)} filings")
                    logger.info(f"Successfully fetched {len(st.session_state.filings)} filings for {ticker}")

            except Exception as e:
                st.error(f"Error: {str(e)}")
                logger.error(f"Error fetching filings: {str(e)}")
                st.write("Debug info:", str(e.__class__))

# Display filings if available
if st.session_state.filings is not None and not st.session_state.filings.empty:
    try:
        st.subheader("Available Filings")

        # Log the data types for debugging
        logger.debug(f"DataFrame types before processing: {st.session_state.filings.dtypes}")

        # Ensure date column is datetime
        if 'date' in st.session_state.filings.columns:
            if not pd.api.types.is_datetime64_any_dtype(st.session_state.filings['date']):
                logger.warning("Date column is not in datetime format, attempting conversion")
                try:
                    st.session_state.filings['date'] = pd.to_datetime(st.session_state.filings['date'])
                except Exception as e:
                    logger.error(f"Error converting dates: {str(e)}")
                    st.error("Error processing dates in the filing data")
                    raise

        # Create a selection column
        st.session_state.filings['Select'] = False

        # Configure the data editor with proper column types
        selected_filings = st.data_editor(
            st.session_state.filings,
            column_config={
                "Select": st.column_config.CheckboxColumn(
                    "Select",
                    help="Select filings to download",
                    default=False,
                ),
                "date": st.column_config.DatetimeColumn(
                    "Filing Date",
                    help="Date when the filing was submitted",
                    format="YYYY-MM-DD",
                ),
                "url": st.column_config.LinkColumn(
                    "Filing URL",
                    help="Click to view the filing on SEC website",
                ),
            },
            hide_index=True,
        )

        # Add format selection
        download_format = st.selectbox(
            "Select Download Format",
            options=["HTML", "PDF"],
            help="Choose the format for downloading the selected filings"
        )

        # Download selected filings
        if st.button("Download Selected Filings"):
            selected_rows = selected_filings[selected_filings['Select']]

            if len(selected_rows) == 0:
                st.warning("Please select at least one filing to download")
            else:
                with st.spinner('Downloading selected filings...'):
                    for _, filing in selected_rows.iterrows():
                        try:
                            content = st.session_state.fetcher.download_filing(
                                filing['url'],
                                format=download_format.lower()
                            )

                            # Create download button for each filing
                            b64 = base64.b64encode(content).decode()
                            extension = ".pdf" if download_format.lower() == 'pdf' else ".html"
                            filename = f"{ticker}_{filing['type']}_{filing['date'].strftime('%Y-%m-%d')}{extension}"
                            mime_type = "application/pdf" if download_format.lower() == 'pdf' else "text/html"
                            href = f'<a href="data:{mime_type};base64,{b64}" download="{filename}">Download {filename}</a>'
                            st.markdown(href, unsafe_allow_html=True)

                        except Exception as e:
                            logger.error(f"Error downloading filing: {str(e)}")
                            st.error(f"Error downloading {filing['date']} {filing['type']}: {str(e)}")

                st.success("Downloads ready!")

    except Exception as e:
        logger.error(f"Error displaying filings: {str(e)}")
        st.error(f"Error displaying filings: {str(e)}")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center'>
    <p>Built with Streamlit • Data from SEC EDGAR</p>
</div>
""", unsafe_allow_html=True)