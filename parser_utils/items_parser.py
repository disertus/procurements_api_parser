from parser_utils.awards_parser import error_handler


@error_handler
def get_item_id(tender_item):
    return tender_item['id']


@error_handler
def get_item_description(tender_item):
    return tender_item['description']


@error_handler
def get_item_quantity(tender_item):
    return tender_item['quantity']


@error_handler
def get_item_related_lot_id(tender_item):
    return tender_item['relatedLot']


@error_handler
def get_item_description(tender_item):
    return tender_item['classification']['description']


@error_handler
def get_item_dk_code(tender_item):
    return tender_item['classification']['id']


@error_handler
def get_item_scheme(tender_item):
    return tender_item['classification']['scheme']


@error_handler
def get_item_delivery_end_date(tender_item):
    return tender_item['deliveryDate']['endDate']


@error_handler
def get_item_city(tender_item):
    return tender_item['deliveryAddress']['locality']


@error_handler
def get_item_region(tender_item):
    return tender_item['deliveryAddress']['region']


@error_handler
def get_item_postal_code(tender_item):
    return tender_item['deliveryAddress']['postalCode']

