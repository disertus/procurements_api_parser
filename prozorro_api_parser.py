import csv
from tqdm import tqdm
import requests
import json
import time
import parser_utils.sqlite_database_utils as db


#todo: store the timestamp of the last offset for each iteration, execution should start from it


class RequestParser:
    base_url = 'https://api.openprocurement.org/api/2.5/'
    
    def __init__(self, date_offset: str, dk_code: str, category: str, csv_output_filename:str, interval: float) -> None:
        self.start_date_offset = date_offset        #timestamp which serves as a starting point for parsing
        self.dk_code = dk_code                      #procurement category code for filtering
        self.interval = interval                    #interval between requests in seconds
        self.category = category                    #api category
        self.output_filename = csv_output_filename  #


    def jsonify_request(self, response_body):
        return json.loads(response_body.text)

    
    def retrieve_next_page_offset(self, request_response):
        """Gets the offset timestamp which marks the beginning of the next batch of tenders (each batch contains ~100 tender IDs)"""
        return request_response['next_page']['offset']

    
    def write_tender(self, values_list: list):
        """Write the tender_id into a local database and appends the value to a csv file (for those unwilling to dig into sqlite)"""
        db.insert_tender_id_into_db(values_list[0])

        #temporary - write data into a local csv file
        with open(f'{self.output_filename}', 'a+') as writefile:
            writer = csv.writer(writefile)
            writer.writerow(values_list)


    def parse_tender(self, response_body):
        """Extract the tender_id of a tender from the response body"""
        data = response_body['data']
        values_list = [data['id']]
        print(values_list)
        self.write_tender(values_list) #move out of this function, return values_list instead


    def filter_tenders_by_dk(self, response_body):
        """Reads into the items object of each tender and checks if the first item's ID corresponds to one that is defined in the filter"""
        try:
            if response_body.get('data').get('items')[0].get('classification').get('id') in self.dk_code:
                print('Found a provider services procurement:')
                self.parse_tender(response_body)
        except Exception as err:
            print(err)

    
    def loop_through_tenders(self, response_in_json):
        """"""
        try:
            for i in tqdm(response_in_json['data']):
                time.sleep(self.interval)
                get_response = self.jsonify_request(requests.get(f"{self.base_url}/{self.category}/{i['id']}"))
                self.filter_tenders_by_dk(get_response)
        except Exception as err:
            print('There was a problem while looping through tenders. Here is the error message:')
            print(err)
            raise


    def loop_through_pages(self):
        """Infinite loop which continually checks for updates in API data"""
        offset = self.start_date_offset #offset corresponds marks the beginning of a new batch of tenders
        while 1:
            try:
                request_body = self.jsonify_request(requests.get(f'{self.base_url}/{self.category}?offset={offset}'))
                offset = self.retrieve_next_page_offset(request_body)
                print('--------')
                print(offset)
                self.loop_through_tenders(request_body)
                time.sleep(self.interval)
            except:
                time.sleep(5)


if __name__ == '__main__':
        print('Beginning to parse the API data')
        dk_codes_tuple = ('72410000-7', '72411000-4')
        parser = RequestParser(date_offset='2021-09-25T12:45:00.813088+03:00', category='tenders', dk_code=dk_codes_tuple, csv_output_filename='data.csv', interval=0.7)
        parser.loop_through_pages()


