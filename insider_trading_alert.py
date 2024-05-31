import requests
from bs4 import BeautifulSoup
import lxml
import os
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def extract_tickers(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    table = soup.find('table', {'class': 'table-dark-row-cp'})
    if not table:
        return set()

    tickers = set()
    for row in table.find_all('tr'):
        ticker = row.find_all('td')[2].text.strip() 
        tickers.add(ticker)
    return tickers

def send_email(matches):
    email = os.environ["EMAIL"]
    password = os.environ["EMAIL_PASSWORD"]

    message = MIMEMultipart("alternative")
    message["Subject"] = "Insider Trading & 52-Week Low Matches Found!"
    message["From"] = email
    message["To"] = email

    text = f"The following matching tickers were found:\n\n" + "\n".join(f"- {ticker}" for ticker in sorted(matches))

    part1 = MIMEText(text, "plain")
    message.attach(part1)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(email, password)
        server.sendmail(
            email, email, message.as_string()
        )

# Fetch tickers from Finviz
finviz_insider_buy = extract_tickers("https://finviz.com/insidertrading.ashx?tc=1")
finviz_insider_sell = extract_tickers("https://finviz.com/insidertrading.ashx?or=-10&tv=100000&tc=1&o=-transactionValue")

# Fetch tickers from Yahoo Finance (52-week lows)
yahoo_52week_low = extract_tickers("https://finance.yahoo.com/u/yahoo-finance/watchlists/fiftytwo-wk-low/")
yahoo_52week_loss = extract_tickers("https://finance.yahoo.com/u/yahoo-finance/watchlists/fiftytwo-wk-loss/") 

# Cross-reference and find matches
matches = finviz_insider_buy.intersection(yahoo_52week_low).union(
    finviz_insider_sell.intersection(yahoo_52week_loss)
)

# Send email alert if matches are found
if matches:
    send_email(matches)
