from parser_utils.awards_parser import error_handler


@error_handler
def get_bid_id(bid):
    return bid['id']


@error_handler
def get_bid_status(bid):
    return bid['status']


@error_handler
def get_bid_tenderer_name(bid):
    return bid['tenderers'][0]['name']


@error_handler
def get_bid_tenderer_id(bid):
    return bid['tenderers'][0]['identifier']['id']


@error_handler
def get_bid_timestamp(bid):
    return bid['date']


@error_handler
def get_bid_value_amount(bid):
    return bid['value']['amount']


@error_handler
def get_bid_value_VAT(bid):
    return bid['value']['valueAddedTaxIncluded']


@error_handler
def get_bid_participation_url(bid):
    return bid['participationUrl']


@error_handler
def get_bid_lot_value_amount(lot_value):
    return lot_value['value']['amount']


@error_handler
def get_bid_lot_value_VAT(lot_value):
    return lot_value['value']['valueAddedTaxIncluded']


@error_handler
def get_bid_related_lot_id(lot_value):
    return lot_value['relatedLot']

@error_handler
def get_bid_related_participation_url(lot_value):
    return lot_value['participationUrl']

