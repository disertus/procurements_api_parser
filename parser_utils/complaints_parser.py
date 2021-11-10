from parser_utils.awards_parser import error_handler


@error_handler
def get_complaint_id(complaint):
    return complaint['id']


@error_handler
def get_complaint_readable_id(complaint):
    return complaint['complaintID']


@error_handler
def get_complaint_type(complaint):
    return complaint['relatedLot']


@error_handler
def get_complaint_date_submitted(complaint):
    return complaint['dateSubmitted']


@error_handler
def get_complaint_type(complaint):
    return complaint['type']


@error_handler
def get_complaint_status(complaint):
    return complaint['status']


@error_handler
def get_complaint_author_name(complaint):
    return complaint['author']['name']


@error_handler
def get_complaint_author_contact_name(complaint):
    return complaint['author']['contactPoint']['name']


@error_handler
def get_complaint_author_contact_phone(complaint):
    return complaint['author']['contactPoint']['telephone']


@error_handler
def get_complaint_author_contact_phone(complaint):
    return complaint['author']['identifier']['id']


@error_handler
def get_complaint_resolution_type(complaint):
    return complaint['resolutionType']


@error_handler
def get_complaint_date_answered(complaint):
    return complaint['dateAnswered']

