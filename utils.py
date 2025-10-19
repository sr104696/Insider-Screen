import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
from io import BytesIO, StringIO
import time
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SECFilingFetcher:
    def __init__(self):
        self.sec_base_url = "https://www.sec.gov/cgi-bin/browse-edgar"
        self.headers = {
            'User-Agent': 'My SEC Filing Fetcher/1.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'www.sec.gov',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }

    def _convert_to_pdf(self, html_content):
        """Convert HTML content to a simple PDF format."""
        try:
            # Parse HTML to extract text
            soup = BeautifulSoup(html_content, 'html.parser')
            text_content = soup.get_text(separator='\n\n')

            # Create PDF content
            pdf_content = f"%PDF-1.4\n"
            pdf_content += "1 0 obj\n<<\n/Type /Catalog\n/Pages 2 0 R\n>>\nendobj\n"
            pdf_content += "2 0 obj\n<<\n/Type /Pages\n/Kids [3 0 R]\n/Count 1\n>>\nendobj\n"
            pdf_content += "3 0 obj\n<<\n/Type /Page\n/Parent 2 0 R\n/Resources <<\n/Font <<\n/F1 4 0 R\n>>\n>>\n"
            pdf_content += "/MediaBox [0 0 612 792]\n/Contents 5 0 R\n>>\nendobj\n"
            pdf_content += "4 0 obj\n<<\n/Type /Font\n/Subtype /Type1\n/BaseFont /Helvetica\n>>\nendobj\n"
            pdf_content += "5 0 obj\n<<\n/Length " + str(len(text_content)) + "\n>>\nstream\n"
            pdf_content += "BT\n/F1 12 Tf\n72 720 Td\n(" + text_content.replace('\n', '\\n') + ") Tj\nET\n"
            pdf_content += "endstream\nendobj\n"
            pdf_content += "xref\n0 6\n0000000000 65535 f\n"
            pdf_content += "trailer\n<<\n/Size 6\n/Root 1 0 R\n>>\nstartxref\n"
            pdf_content += "%%EOF"

            return pdf_content.encode('utf-8')
        except Exception as e:
            logger.error(f"Error converting to PDF: {str(e)}")
            raise Exception(f"Error converting to PDF: {str(e)}")

    def download_filing(self, url, format='html'):
        """Download a specific SEC filing in the specified format."""
        try:
            time.sleep(1)  # Rate limit
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()

            # Parse HTML to find the actual document link
            soup = BeautifulSoup(response.content, 'html.parser')
            doc_link = soup.find('a', {'id': 'documentsbutton'})

            if doc_link and 'href' in doc_link.attrs:
                actual_url = f"https://www.sec.gov{doc_link['href']}"
                time.sleep(1)  # Rate limit
                doc_response = requests.get(actual_url, headers=self.headers)
                doc_response.raise_for_status()
                content = doc_response.content
            else:
                logger.warning("No document link found, using original response content")
                content = response.content

            if format.lower() == 'pdf':
                return self._convert_to_pdf(content)
            return content

        except requests.RequestException as e:
            logger.error(f"Error downloading filing: {str(e)}")
            raise Exception(f"Error downloading filing: {str(e)}")

    def _get_cik(self, ticker):
        """Get the CIK number for a ticker symbol."""
        try:
            # First try direct CIK lookup
            url = f"https://data.sec.gov/submissions/{ticker}.json"
            response = requests.get(url, headers=self.headers)

            if response.status_code == 200:
                return ticker

            # If that fails, try company lookup
            params = {
                'company': ticker,
                'owner': 'exclude',
                'output': 'atom'
            }
            response = requests.get(self.sec_base_url, params=params, headers=self.headers)
            response.raise_for_status()

            df = pd.read_xml(BytesIO(response.content))
            if not df.empty:
                # Extract CIK from title
                title = df.iloc[0]['title']
                cik_match = re.search(r'CIK#:\s*(\d+)', title)
                if cik_match:
                    return cik_match.group(1).zfill(10)

            return ticker

        except Exception as e:
            logger.error(f"Error looking up CIK: {str(e)}")
            return ticker

    def get_filings(self, ticker, filing_type, start_date, end_date):
        """Fetch SEC filings for a given company using web scraping."""
        try:
            params = {
                'action': 'getcompany',
                'CIK': ticker,
                'type': filing_type,
                'dateb': end_date.strftime('%Y%m%d'),
                'datea': start_date.strftime('%Y%m%d'),
                'owner': 'exclude',
                'count': '100'
            }

            logger.info(f"Fetching filings for ticker: {ticker}")
            time.sleep(1)  # Increased rate limit to 1 second
            response = requests.get(self.sec_base_url, params=params, headers=self.headers)
            response.raise_for_status()

            # Parse HTML content
            soup = BeautifulSoup(response.content, 'html.parser')

            # Find the table containing filing information
            filing_table = soup.find('table', class_='tableFile2')
            if not filing_table:
                logger.warning("No filing table found in the response")
                return pd.DataFrame()

            # Extract filings
            filings = []
            rows = filing_table.find_all('tr')[1:]  # Skip header row
            for row in rows:
                try:
                    cols = row.find_all('td')
                    if len(cols) >= 4:  # Ensure we have enough columns
                        # Extract filing date and convert to datetime
                        filing_date_str = cols[3].text.strip()
                        try:
                            filing_date = datetime.strptime(filing_date_str, '%Y-%m-%d')
                            logger.debug(f"Parsed date: {filing_date} from string: {filing_date_str}")
                        except ValueError as e:
                            logger.error(f"Error parsing date {filing_date_str}: {str(e)}")
                            continue

                        # Extract filing type and URL
                        filing_link = cols[1].find('a')
                        if filing_link:
                            doc_link = f"https://www.sec.gov{filing_link['href']}"

                            # Extract document title
                            title = cols[2].text.strip()

                            filings.append({
                                'date': filing_date,
                                'type': filing_type,
                                'url': doc_link,
                                'title': title
                            })
                except Exception as e:
                    logger.error(f"Error processing row: {str(e)}")
                    continue

            logger.info(f"Successfully processed {len(filings)} filings")
            return pd.DataFrame(filings)

        except requests.RequestException as e:
            logger.error(f"Error fetching SEC filings: {str(e)}")
            raise Exception(f"Error fetching SEC filings: {str(e)}")
        except Exception as e:
            logger.error(f"Error processing SEC data: {str(e)}")
            raise Exception(f"Error processing SEC data: {str(e)}")