from procurements_api_parser.awards_parser import error_handler


@error_handler
def get_contract_id(contract):
    return contract['id']


@error_handler
def get_contract_readable_id(contract):
    return contract['contractID']


@error_handler
def get_contract_status(contract):
    return contract['status']


@error_handler
def get_contract_date_end(contract):
    return contract['period']['endDate']


@error_handler
def get_contract_date_signed(contract):
    return contract['dateSigned']


@error_handler
def get_contract_supplier_name(contract):
    return contract['suppliers'][0]['name']


@error_handler
def get_contract_supplier_id(contract):
    return contract['suppliers'][0]['identifier']['id']


@error_handler
def get_contract_supplier_contact_name(contract):
    return contract['suppliers'][0]['contactPoint']['name']


@error_handler
def get_contract_supplier_contact_phone(contract):
    return contract['suppliers'][0]['contactPoint']['telephone']


@error_handler
def get_contract_supplier_contact_email(contract):
    return contract['suppliers'][0]['contactPoint']['email']


@error_handler
def get_contract_supplier_scale(contract):
    return contract['suppliers'][0]['scale']



@error_handler
def get_contract_price_grosso(contract):
    return contract['value']['amount']


@error_handler
def get_contract_price_netto(contract):
    return contract['value']['amountNet']


@error_handler
def get_contract_award_id(contract):
    return contract['awardID']
