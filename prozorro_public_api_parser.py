import csv
from tqdm import tqdm
import requests
import json
import logging as log
import time
import parser_utils.sqlite_database_utils as db
import parser_utils.config as cfg


#todo: store the timestamp of the last offset for each iteration, execution should start from it


class ProzorroCronScrapper:
    base_url = 'https://api.openprocurement.org/api/2.5/'
    alternative_base_url = 'https://public-api.prozorro.gov.ua/api/2.5/'
    
    def __init__(self, date_offset: str, dk_code: str, category: str, csv_output_filename: str, interval: float = 0.7):
        self.start_date_offset = date_offset        #timestamp which serves as a starting point for parsing
        self.dk_code = dk_code                      #procurement category code for filtering
        self.interval = interval                    #interval between requests in seconds
        self.category = category                    #api category
        self.output_filename = csv_output_filename  #


    def prozorro_request(self, params):
        try:
            response_body = requests.get(f"{self.base_url}/{self.category}{params}")
            return json.loads(response_body.text)
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
            writer.writerow(values_list)


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

    
    def loop_through_tenders(self, response_dict):
        """Go through tenders"""
        for i in tqdm(response_dict['data']):
            time.sleep(self.interval)
            get_response = self.prozorro_request(f"/{i['id']}")
            self.filter_tenders_by_dk(get_response)


    def loop_through_pages(self):
        """Infinite loop which continually checks for updates in API data"""
        offset = self.start_date_offset #offset corresponds marks the beginning of a new batch of tenders
        while 1:
            try:
                response_body = self.prozorro_request(f'?offset={offset}')
                offset = self.retrieve_next_page_offset(response_body)
                print('--------')
                print(offset)
                self.loop_through_tenders(response_body)
                time.sleep(self.interval)
            except:
                time.sleep(cfg.sleep_time_if_disconnected) #todo: import this value from a config file


if __name__ == '__main__':
        print('Beginning to retrieve the API data')
        db.create_database()
        dk_codes_tuple = ('72410000-7', '72411000-4')
        parser = ProzorroCronScrapper(date_offset='2021-10-04T08:45:00.813088+03:00',
                                      category='tenders',
                                      dk_code=dk_codes_tuple,
                                      csv_output_filename='data.csv')
        parser.loop_through_pages()


