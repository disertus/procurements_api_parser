import aiohttp
import asyncio
import csv
from tqdm import tqdm
from datetime import datetime
import requests
import json
import logging as log
import time
import parser_utils.sqlite_database_utils as db
import parser_utils.config as cfg


log.basicConfig(filename="logs.txt", level=log.INFO)

class ProzorroCronScrapper:
    base_url = 'https://api.openprocurement.org/api/2.5/'
    alternative_base_url = 'https://public-api.prozorro.gov.ua/api/2.5/'
    
    def __init__(self, date_offset: str, dk_code: str, category: str, csv_output_filename: str):
        self.start_date_offset = date_offset        # timestamp which serves as a starting point for parsing
        self.dk_code = dk_code                      # procurement category code for filtering
        self.interval = 1                           # interval between requests in seconds
        self.category = category                    # api category
        self.output_filename = csv_output_filename  #
        self.batch_size = 20                        # number of elements inside a page at /tenders endpoint. Prozorro's ratelimit seems to be set at ~50 rps, throttles afterwards


    def prozorro_request(self, params):
        try:
            return requests.get(f"{self.base_url}/{self.category}{params}").json()
        except Exception as err:
            log.warning(err)

    
    def retrieve_next_page_offset(self, request_response):
        """Gets the offset timestamp which marks the beginning of the next batch of tenders (each batch contains ~100 tender IDs)"""
        return request_response['next_page']['offset']

    
    def write_tender(self, values_list: list):
        """Write the tender_id into a local database and appends the value to a csv file (for those unwilling to dig into sqlite)"""
        db.insert_tender_id_into_db(values_list[0])

        #temporary - write data into a local csv file apart from database
        with open(f'{self.output_filename}', 'a+') as writefile:
            writer = csv.writer(writefile)
            writer.writerow([values_list[0], datetime.now()])


    def parse_tender(self, response_body):
        """Extract the tender_id of a tender from the response body"""
        data = response_body['data']
        values_list = [data['id']]
        print(values_list)
        self.write_tender(values_list)


    def filter_tenders_by_dk(self, response_body):
        """Reads into the items object of each tender and checks if the first item's ID corresponds to one that is defined in the filter"""
        try:
            if response_body.get('data').get('items')[0].get('classification').get('id') in self.dk_code:
                print('\nFound a provider services procurement:')
                self.parse_tender(response_body)
        except Exception as err:
            log.error(err)


    async def loop_through_tenders(self, response_dict):
        """Go through tenders"""
        results = []
        async with aiohttp.ClientSession() as session:
            tasks = [session.get(self.base_url + self.category + f"/{i['id']}", ssl=False)
                     for i in tqdm(response_dict['data'])]
            responses = await asyncio.gather(*tasks)
            for response in responses:
                results.append(await response.json())

            for resp in results:
                self.filter_tenders_by_dk(resp)

    def write_last_offset(self, offset):
        with open("last_offset.csv", "w+") as file:
            if type(offset) == str:
                file.write(offset)
                file.flush()
            elif type(offset) == float:
                file.write(str(datetime.fromtimestamp(offset)).replace(" ", "T") + "+03:00")
                file.flush()


    def loop_through_pages(self):
        """Infinite loop which continually checks for updates in API data"""
        offset = self.start_date_offset  # offset marks the beginning of a new batch of tenders
        old_offset = 0
        while 1:
            try:
                old_offset = offset
                self.write_last_offset(offset)
                response_body = requests.get(f"{self.base_url}/{self.category}?offset={offset}&limit={self.batch_size}")
                response_body = json.loads(response_body.text)
                offset = self.retrieve_next_page_offset(response_body)
                print('--------')
                print(datetime.fromtimestamp(offset))
                asyncio.run(self.loop_through_tenders(response_body))
                time.sleep(self.interval)
            except Exception as e:
                offset = old_offset
                time.sleep(cfg.sleep_time_if_disconnected)
                log.error(f"Failed to loop over the tender_id stream with the following err message: {e}")


if __name__ == '__main__':
        print('Beginning to retrieve the API data')
        db.create_database()
        dk_codes_tuple = ('72410000-7', '72411000-4')
        try:
            with open("last_offset.csv", "r") as offset_file:
                latest_offset = offset_file.read()
                print(latest_offset)
        except:
            latest_offset = '2022-09-09T16:42:43.836550+03:00'

        parser = ProzorroCronScrapper(date_offset=latest_offset,
                                      category='tenders',
                                      dk_code=dk_codes_tuple,
                                      csv_output_filename='data.csv')
        parser.loop_through_pages()


