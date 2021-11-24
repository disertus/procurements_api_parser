from prozorro_public_api_parser import ProzorroCronScrapper
import procurements_api_parser.sqlite_database_utils as db
import logging as log
import csv

# docs available here: https://prozorro-audit-api.readthedocs.io/uk/latest/monitoring/tutorial/creation.html

class AuditCronScrapper(ProzorroCronScrapper):
    def filter_tenders_by_dk(self, response_body):
        try:
            if check_if_in_parsed_tenders(tender := response_body.get('data').get('tender_id')):
                print('\nFound an audit for a provider services procurement:')
                print(tender)
        except Exception as err:
            log.error(err)

    def write_tender(self, values_list: list):
        """Write the tender_id into a local database and appends the value to a csv file (for those unwilling to dig into sqlite)"""
        # db.insert_tender_id_into_db(values_list[0])

        #temporary - write data into a local csv file apart from database
        with open(f'{self.output_filename}', 'a+') as writefile:
            writer = csv.writer(writefile)
            writer.writerow(values_list)


def check_if_in_parsed_tenders(checked_tender_id):
    for tender in db.fetch_from_database():
        if checked_tender_id == tender[0]:
            return True


AuditCronScrapper.base_url = 'https://audit-api.prozorro.gov.ua/api/2.5/'

inst = AuditCronScrapper(date_offset='2021-10-07T08:45:00.813088+03:00',
                         category='monitorings',
                         dk_code=('72410000-7', '72411000-4'),
                         csv_output_filename='audit_data.csv',
                         interval=0.3)

inst.loop_through_pages()
