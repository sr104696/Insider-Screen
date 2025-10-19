import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json

class WebScraper:
    def __init__(self, start_url, max_pages=50):
        self.start_url = start_url
        self.base_url = f"{urlparse(start_url).scheme}://{urlparse(start_url).netloc}"
        self.visited_urls = set()
        self.max_pages = max_pages
        self.scraped_content = {}

    def scrape_site(self):
        self.crawl(self.start_url)

    def crawl(self, url):
        if url in self.visited_urls or len(self.visited_urls) >= self.max_pages:
            return
        self.visited_urls.add(url)
        print(f"Scraping: {url}")
        try:
            response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            text_content = self.extract_text(soup)
            self.scraped_content[url] = text_content
            links = soup.find_all('a', href=True)
            for link in links:
                href = link['href']
                full_url = urljoin(self.base_url, href)
                if self.base_url in full_url and full_url not in self.visited_urls:
                    self.crawl(full_url)
        except requests.RequestException as e:
            print(f"Error scraping {url}: {str(e)}")

    def extract_text(self, soup):
        for script in soup(["script", "style"]):
            script.decompose()
        text = soup.get_text()
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split(" "))
        text = '\n'.join(chunk for chunk in chunks if chunk)
        return text

    def save_content(self):
        try:
            with open('scraped_content.json', 'w', encoding='utf-8') as f:
                json.dump(self.scraped_content, f, ensure_ascii=False, indent=4)
            print("Scraping completed. Content saved in 'scraped_content.json'")
        except IOError as e:
            print(f"Error saving scraped content: {str(e)}")

def main():
    try:
        start_url = input("Enter the URL to scrape: ")
        max_pages = int(input("Enter the maximum number of pages to scrape: "))

        scraper = WebScraper(start_url, max_pages)
        scraper.scrape_site()
        scraper.save_content()
    except ValueError as e:
        print(f"Invalid input: {str(e)}")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    main()