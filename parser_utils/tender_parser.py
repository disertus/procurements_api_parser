import logging as log
from parser_utils.awards_parser import error_handler


@error_handler
def get_tender_id(data):
    '''Return the ID of a tender which is used to retrieve tender data from Prozorro API'''
    return data['id']


@error_handler
def get_tender_region(data):
    '''Return the name of the region where the procuring entity is situated'''
    return data['procuringEntity']['address']['region']


@error_handler
def get_tender_locality(data):
    return data['procuringEntity']['address']['locality']


@error_handler
def get_tender_postal_code(data):
    return f"'{data['procuringEntity']['address']['postalCode']}"


@error_handler
def get_tender_link(data):
    return f"https://prozorro.gov.ua/tender/{data['tenderID']}"


@error_handler
def get_tender_status(data):
    return data['status']


@error_handler
def get_tender_title(data):
    return data['title']


@error_handler
def get_tender_procurement_category(data):
    return data['mainProcurementCategory']


@error_handler
def get_tender_first_document_link(data):
    return data['documents'][0]['url']


@error_handler
def get_tender_publication_timestamp(data):
    return f"{data['tenderID'][3:13]}T00:01:01.636613+03:00"


@error_handler
def get_tender_description(data):
    return data['items'][0]['classification']['description']


@error_handler
def get_tender_items_count(data):
    return len(data['items'])


@error_handler
def get_tender_delivery_end_date(data):
    return data['items'][0]['deliveryDate']['endDate']


@error_handler
def get_tender_price(data):
    return data['value']['amount']


@error_handler
def get_tender_VAT(data):
    return data['value']['valueAddedTaxIncluded']


@error_handler
def get_tender_procurement_method(data):
    return data['procurementMethod']


@error_handler
def get_tender_procuring_entity_name(data):
    return data['procuringEntity']['name']


@error_handler
def get_tender_procuring_entity_id(data):
    return data['procuringEntity']['identifier']['id']


@error_handler
def get_tender_contact_name(data):
    return data['procuringEntity']['contactPoint']['name']


@error_handler
def get_tender_contact_email(data):
    return data['procuringEntity']['contactPoint']['email']


@error_handler
def get_tender_contact_phone(data):
    return data['procuringEntity']['contactPoint']['telephone']


@error_handler
def get_tender_period_start(data):
    return data['tenderPeriod']['startDate']


@error_handler
def get_tender_plan_id(data):
    return data['plans'][0]['id']


@error_handler
def get_tender_lots_count(data):
    return len(data['lots'])


@error_handler
def get_tender_auction_start_date(data):
    try:
        return data['lots'][0]['auctionPeriod']['startDate']
    except KeyError as err:
        log.debug(f'Auction date is absent in lots info: {err}')
    try:
        return data['auctionPeriod']['startDate']
    except KeyError as err:
        log.debug(f'Auction date is absent in tender info: {err}')
    try:
        return data['awardPeriod']['startDate']
    except KeyError as err:
        log.debug(f'No data about auction date was found: {err}')
        return "NaN"

@error_handler
def get_number_of_bids(data):
    return data['numberOfBids']

