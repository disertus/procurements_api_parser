import csv
import re
import requests

from bs4 import BeautifulSoup
from tqdm import tqdm


class Parser:
    """Parses the page and saves the data that has been collected into the mysqldb"""

    def __init__(self, url: str, tag: str):
        self.page_url = url
        self.tag = tag

    def parse_a_page(self):
        """Parses the page, pretending to be a user due to page_headers parameter"""

        # headers are necessary to emulate a 'live user' connection, otherwise produces an error
        page_headers = {
            'User-Agent':
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) snap Chromium/81.0.4044.138 Chrome/81.0.4044.138 Safari/537.36"
        }
        open_url = requests.get(self.page_url, headers=page_headers).text
        soup = BeautifulSoup(open_url, 'lxml')
        return soup

def parse_request(page_url):
    if breach := re.findall(r'Причини розірвання договору:', Parser(page_url, "test").parse_a_page().get_text()):
        print(page_url, " ", breach)


def run():
    with open("tender_links.csv", "r") as link_list:
         for link in tqdm(link_list):
             parse_request(link)

if __name__ == "__main__":
    run()
# print(inst.parse_a_page().td['class'])