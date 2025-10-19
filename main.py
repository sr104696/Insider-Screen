import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from sec_scraper import SECFilingScraper
from utils import download_filings

st.set_page_config(
    page_title="SEC Filing Scraper",
    page_icon="📊",
    layout="wide"
)

def main():
    st.title("SEC Filing Scraper")
    st.markdown("""
    This application allows you to search and download SEC filings for publicly traded companies.
    Enter a company's ticker symbol and select the filing types and date range to get started.
    """)

    # Input section
    col1, col2 = st.columns(2)
    
    with col1:
        ticker = st.text_input("Company Ticker Symbol", value="", help="Enter the stock ticker symbol (e.g., AAPL)")
        
        filing_types = st.multiselect(
            "Filing Types",
            ["10-K", "10-Q", "8-K", "20-F", "6-K"],
            default=["10-K"],
            help="Select one or more filing types"
        )

    with col2:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        
        date_range = st.date_input(
            "Date Range",
            value=(start_date, end_date),
            help="Select the date range for the filings"
        )

    if st.button("Search Filings"):
        if not ticker:
            st.error("Please enter a ticker symbol")
            return

        try:
            with st.spinner("Searching for filings..."):
                scraper = SECFilingScraper()
                filings = scraper.get_filings(
                    ticker.upper(),
                    filing_types,
                    start_date=date_range[0],
                    end_date=date_range[1]
                )

                if not filings:
                    st.warning("No filings found for the specified criteria")
                    return

                st.success(f"Found {len(filings)} filings")
                
                # Display filings in a table
                df = pd.DataFrame(filings)
                df['Select'] = False
                selected_filings = st.data_editor(
                    df,
                    column_config={
                        "Select": st.column_config.CheckboxColumn(
                            "Select",
                            help="Select filings to download"
                        )
                    },
                    hide_index=True
                )

                # Download selected filings
                selected_rows = selected_filings[selected_filings['Select']]
                if not selected_rows.empty:
                    if st.button("Download Selected Filings"):
                        with st.spinner("Downloading selected filings..."):
                            downloaded_files = download_filings(selected_rows)
                            for file_name, file_content in downloaded_files:
                                st.download_button(
                                    label=f"Download {file_name}",
                                    data=file_content,
                                    file_name=file_name,
                                    mime="application/pdf"
                                )

        except Exception as e:
            st.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
