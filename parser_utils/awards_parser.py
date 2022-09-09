

def error_handler(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyError:
            return None
    return wrapper


@error_handler
def get_award_id(award):
    return award['id']


@error_handler
def get_award_status(award):
    return award['status']


@error_handler
def get_award_price(award):
    return award['value']['amount']


@error_handler
def get_award_supplier_name(award):
    return award['suppliers'][0]['name']


@error_handler
def get_award_supplier_id(award):
    return award['suppliers'][0]['identifier']['id']


@error_handler
def get_award_supplier_contact_name(award):
    return award['suppliers'][0]['contactPoint']['name']


@error_handler
def get_award_supplier_contact_phone(award):
    return award['suppliers'][0]['contactPoint']['telephone']


@error_handler
def get_award_supplier_contact_email(award):
    return award['suppliers'][0]['contactPoint']['email']


@error_handler
def get_award_lot_id(award):
    return award['lotID']

@error_handler
def get_award_bid_id(award):
    return award['bid_id']
