from parser_utils.awards_parser import error_handler

@error_handler
def get_milestone_id(milestone):
    return milestone['id']

@error_handler
def get_prepayment_percentage(milestone):
    return milestone['percentage']

@error_handler
def get_prepayment_duration(milestone):
    return milestone['duration']['days']

@error_handler
def get_prepayment_related_lot(milestone):
    return milestone['relatedLot']