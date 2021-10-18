def error_handler(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            return "NaN"
    return wrapper



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
def get_bid_lot_value_amount(lot_value):
    return lot_value['value']['amount']


@error_handler
def get_bid_lot_value_VAT(lot_value):
    return lot_value['value']['valueAddedTaxIncluded']


@error_handler
def get_bid_lot_value_related_lot_id(lot_value):
    return lot_value['relatedLot']


@error_handler
def get_bid_lot_value_participation_url(lot_value):
    return lot_value['participationUrl']




if __name__ == "__main__":
    print("Initialized")