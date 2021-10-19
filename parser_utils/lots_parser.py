

def error_handler(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyError:
            return "NaN"
    return wrapper


@error_handler
def get_lot_id(lot):
    return lot['id']


@error_handler
def get_lot_title(lot):
    return lot['title']


@error_handler
def get_lot_status(lot):
    return lot['status']


@error_handler
def get_lot_price(lot):
    return lot['value']['amount']


@error_handler
def get_lot_auction_period(lot):
    return lot['auctionPeriod']['startDate']




if __name__ == "__main__":
    print("Initialized")
