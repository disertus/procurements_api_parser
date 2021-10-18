from prozorro_api_parser import RequestParser
import requests
import pandas as pd
import time
import parser_utils.awards_parser as awards_parser
import parser_utils.bids_parser as bids_parser
import parser_utils.contracts_parser as contracts_parser
import parser_utils.items_parser as items_parser
import parser_utils.lots_parser as lots_parser
import parser_utils.tender_parser as tender_parser
import parser_utils.sqlite_database_utils as db



output_filename = 'tender_details.csv'


def parse_lots(response_body):
    try:
        data = response_body['data']

        for lot in data['lots']:
            values_list = []
            values_list.append(tender_parser.get_tender_id(data))
            values_list.append(lots_parser.get_lot_id(lot))
            values_list.append(lots_parser.get_lot_title(lot))
            values_list.append(lots_parser.get_lot_status(lot))
            values_list.append(lots_parser.get_lot_price(lot))
            values_list.append(lots_parser.get_lot_auction_period(lot))
        
            yield values_list

    except:
        raise


def parse_items(response_body):
    try:
        data = response_body['data']

        for item in data['items']:
            values_list = []
            values_list.append(tender_parser.get_tender_id(data))
            values_list.append(items_parser.get_item_related_lot_id(item))
            values_list.append(items_parser.get_item_id(item))
            values_list.append(items_parser.get_item_dk_code(item))
            values_list.append(items_parser.get_item_delivery_end_date(item))
            values_list.append(items_parser.get_item_quantity(item))
            values_list.append(items_parser.get_item_scheme(item))
            values_list.append(items_parser.get_item_city(item))
            values_list.append(items_parser.get_item_region(item))
            values_list.append(f"'{items_parser.get_item_postal_code(item)}")

            yield values_list

    except:
        raise


def parse_bids(response_body):
    try:
        data = response_body['data']

        for bid in data['bids']:
            for lot_value in bid['lotValues']:
                values_list = []
                values_list.append(tender_parser.get_tender_id(data))
                values_list.append(bids_parser.get_bid_lot_value_related_lot_id(lot_value))
                values_list.append(bids_parser.get_bid_id(bid))
                values_list.append(bids_parser.get_bid_status(bid))
                values_list.append(bids_parser.get_bid_tenderer_id(bid))
                values_list.append(bids_parser.get_bid_tenderer_name(bid))
                values_list.append(bids_parser.get_bid_timestamp(bid))
                values_list.append(bids_parser.get_bid_lot_value_amount(lot_value))
                values_list.append(bids_parser.get_bid_lot_value_VAT(lot_value))
                values_list.append(bids_parser.get_bid_lot_value_participation_url(lot_value))


                yield values_list

    except:
        raise   


def parse_awards(response_body):
    try:
        data = response_body['data']

        for award in data['awards']:
            values_list = []
            values_list.append(tender_parser.get_tender_id(data))
            values_list.append(awards_parser.get_award_id(award))
            values_list.append(awards_parser.get_award_status(award))
            values_list.append(awards_parser.get_award_price(award))
            values_list.append(awards_parser.get_award_supplier_name(award))
            values_list.append(awards_parser.get_award_supplier_id(award))
            values_list.append(awards_parser.get_award_supplier_contact_name(award))
            values_list.append(awards_parser.get_award_supplier_contact_phone(award))
            values_list.append(awards_parser.get_award_supplier_contact_email(award))
            values_list.append(awards_parser.get_award_lot_id(award))
            values_list.append(awards_parser.get_award_bid_id(award))
        
            yield values_list

    except:
        raise


def parse_contracts(response_body):
    try:
        data = response_body['data']

        for contract in data['contracts']:
            values_list = []
            values_list.append(tender_parser.get_tender_id(data))
            values_list.append(contracts_parser.get_contract_award_id(contract))
            values_list.append(contracts_parser.get_contract_id(contract))
            values_list.append(contracts_parser.get_contract_status(contract))
            values_list.append(contracts_parser.get_contract_date_signed(contract))
            values_list.append(contracts_parser.get_contract_date_end(contract))

            values_list.append(contracts_parser.get_contract_supplier_name(contract))
            values_list.append(contracts_parser.get_contract_supplier_id(contract))
            values_list.append(contracts_parser.get_contract_supplier_contact_name(contract))
            values_list.append(contracts_parser.get_contract_supplier_contact_phone(contract))
            values_list.append(contracts_parser.get_contract_supplier_contact_email(contract))

            values_list.append(contracts_parser.get_contract_supplier_scale(contract))
            values_list.append(contracts_parser.get_contract_price_grosso(contract))
            values_list.append(contracts_parser.get_contract_price_netto(contract))
        
            yield values_list

    except:
        raise


def parse_tender(response_body):
    try:
        data = response_body['data']
        values_list = []

        values_list.append(tender_parser.get_tender_id(data))
        values_list.append(tender_parser.get_tender_region(data))
        values_list.append(tender_parser.get_tender_locality(data))
        values_list.append(tender_parser.get_tender_postal_code(data))
        values_list.append(tender_parser.get_tender_link(data))

        values_list.append(tender_parser.get_tender_status(data))
        values_list.append(tender_parser.get_tender_title(data))
        values_list.append(tender_parser.get_tender_procurement_category(data))

        values_list.append(tender_parser.get_tender_first_document_link(data))
        values_list.append(tender_parser.get_tender_publication_timestamp(data))
        values_list.append(tender_parser.get_tender_description(data))
        values_list.append(tender_parser.get_tender_items_count(data))
        values_list.append(tender_parser.get_tender_delivery_end_date(data))
        values_list.append(tender_parser.get_tender_price(data))
        values_list.append(tender_parser.get_tender_VAT(data))
        values_list.append(tender_parser.get_tender_procurement_method(data))

        values_list.append(tender_parser.get_tender_procuring_entity_name(data))
        values_list.append(tender_parser.get_tender_procuring_entity_id(data))
        values_list.append(tender_parser.get_tender_contact_name(data))
        values_list.append(tender_parser.get_tender_contact_email(data))
        values_list.append(tender_parser.get_tender_contact_phone(data))

        values_list.append(tender_parser.get_tender_period_start(data))
        values_list.append(tender_parser.get_tender_plan_id(data))
        values_list.append(tender_parser.get_tender_lots_count(data))
        values_list.append(tender_parser.get_tender_auction_start_date(data))
            
        print(values_list)
        return values_list
    except:
        raise


def write_to_csv(list_of_lists, output_name):
    df = pd.DataFrame(list_of_lists)
    df.reset_index(drop=True, inplace=True)
    df.to_csv(output_name, index=False, header=False)



def loop_through_ids():
    tender_details = []
    lots_details = []
    contracts_details = []
    awards_details = []
    items_details = []
    bids_details = []

    for row in db.fetch_from_database().fetchall():
        print(row)
        try:
            response_body = requests.get(f'{inst.base_url}/{inst.category}/{row[0]}?opt_pretty=1')
            jsonified_response = inst.jsonify_request(response_body)
            tender_details.append(inst.parse_tender(jsonified_response))
            
            try:
                for list_item in parse_lots(jsonified_response):
                    lots_details.append(list_item)
            except:
                pass

            try:
                for list_item in parse_contracts(jsonified_response):
                    contracts_details.append(list_item)
            except:
                pass

            try:
                for list_item in parse_awards(jsonified_response):
                    awards_details.append(list_item)
            except:
                pass

            try:
                for list_item in parse_items(jsonified_response):
                    items_details.append(list_item)
            except:
                pass

            try:
                for list_item in parse_bids(jsonified_response):
                    bids_details.append(list_item)
            except:
                pass

        except Exception as err:
            continue
    
    write_to_csv(tender_details, "tender_details.csv")
    write_to_csv(lots_details, "lots_details.csv")
    write_to_csv(contracts_details, "contracts_details.csv")
    write_to_csv(awards_details, "awards_details.csv")
    write_to_csv(items_details, "items_details.csv")
    write_to_csv(bids_details, "bids_details.csv")

    print('\n--------------------')
    print('Finished parsing the existing batch of data')
    t = time.localtime()
    print(time.strftime("%H:%M:%S", t))
    print('--------------------')


if __name__ == '__main__':
    dk_codes_tuple = ('72410000-7', '72411000-4')
    inst = RequestParser(date_offset='2021-07-20', category='tenders', dk_code=dk_codes_tuple, csv_output_filename=output_filename, interval=3)
    inst.parse_tender = parse_tender
    while 1:
        try:
            loop_through_ids()
            time.sleep(3600)
        except Exception as err:
            print("This should have never happened")
            print(err)
            break


