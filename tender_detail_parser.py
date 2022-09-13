import asyncio
import datetime
import logging as log
import os

import pandas as pd
from prozorro_public_api_parser import ProzorroCronScrapper
import time
import parser_utils.awards_parser as awards_parser
import parser_utils.bids_parser as bids_parser
import parser_utils.complaints_parser as complaints_parser
import parser_utils.contracts_parser as contracts_parser
import parser_utils.items_parser as items_parser
import parser_utils.lots_parser as lots_parser
import parser_utils.milestones_parser as milestones_parser
import parser_utils.tender_parser as tender_parser
import parser_utils.sqlite_database_utils as db

# todo: write output files to a "resulting_data" folder

output_filename = 'tender_details.csv'


def parse_milestones(response_body):
    data = response_body['data']
    tender_id = tender_parser.get_tender_id(data)

    for milestone in data['milestones']:
        values_list = []
        try:
            if milestone['code'] == "prepayment":
                values_list.append(tender_id)
                values_list.append(milestones_parser.get_milestone_id(milestone))
                values_list.append(milestones_parser.get_prepayment_related_lot(milestone))
                values_list.append(milestones_parser.get_prepayment_percentage(milestone))
                values_list.append(milestones_parser.get_prepayment_duration(milestone))

                yield values_list

        except KeyError as err:
            log.debug(err)


def parse_complaints(response_body):
    data = response_body['data']
    tender_id = tender_parser.get_tender_id(data)

    for award in data['awards']:
        try:
            for complaint in award['complaints']:
                values_list = []
                values_list.append((tender_id))
                values_list.append(complaints_parser.get_complaint_id(complaint))
                values_list.append(complaints_parser.get_complaint_readable_id(complaint))
                values_list.append(complaints_parser.get_complaint_related_lot(complaint))
                values_list.append(complaints_parser.get_complaint_date_submitted(complaint))
                values_list.append(complaints_parser.get_complaint_type(complaint))
                values_list.append(complaints_parser.get_complaint_status(complaint))
                values_list.append(complaints_parser.get_complaint_author_id(complaint))
                values_list.append(complaints_parser.get_complaint_author_name(complaint))
                values_list.append(complaints_parser.get_complaint_author_contact_name(complaint))
                values_list.append(complaints_parser.get_complaint_author_contact_phone(complaint))
                values_list.append(complaints_parser.get_complaint_resolution_type(complaint))
                values_list.append(complaints_parser.get_complaint_date_answered(complaint))

                yield values_list

        except KeyError as err:
            log.debug(err)


def parse_lots(response_body):
    data = response_body['data']
    tender_id = tender_parser.get_tender_id(data)

    for lot in data['lots']:
        values_list = []
        values_list.append(tender_id)
        values_list.append(lots_parser.get_lot_id(lot))
        values_list.append(lots_parser.get_lot_title(lot))
        values_list.append(lots_parser.get_lot_status(lot))
        values_list.append(lots_parser.get_lot_price(lot))
        values_list.append(lots_parser.get_lot_auction_period(lot))

        yield values_list


def parse_items(response_body):
    data = response_body['data']
    tender_id = tender_parser.get_tender_id(data)

    for item in data['items']:
        values_list = []
        values_list.append(tender_id)
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


def parse_bids(response_body):
    data = response_body['data']
    tender_id = tender_parser.get_tender_id(data)

    for bid in data['bids']:
        try:
            for lot_value in bid['lotValues']:
                values_list = []
                values_list.append(tender_id)
                values_list.append(bids_parser.get_bid_related_lot_id(lot_value))
                values_list.append(bids_parser.get_bid_id(bid))
                values_list.append(bids_parser.get_bid_status(bid))
                values_list.append(bids_parser.get_bid_tenderer_id(bid))
                values_list.append(bids_parser.get_bid_tenderer_name(bid))
                values_list.append(bids_parser.get_bid_timestamp(bid))
                values_list.append(bids_parser.get_bid_lot_value_amount(lot_value))
                values_list.append(bids_parser.get_bid_lot_value_VAT(lot_value))
                values_list.append(bids_parser.get_bid_related_participation_url(lot_value))

                yield values_list
        except KeyError as err:
            log.debug(err)

        try:
            values_list = []
            values_list.append(tender_id)
            values_list.append(None)
            values_list.append(bids_parser.get_bid_id(bid))
            values_list.append(bids_parser.get_bid_status(bid))
            values_list.append(bids_parser.get_bid_tenderer_id(bid))
            values_list.append(bids_parser.get_bid_tenderer_name(bid))
            values_list.append(bids_parser.get_bid_timestamp(bid))
            values_list.append(bids_parser.get_bid_value_amount(bid))
            values_list.append(bids_parser.get_bid_value_VAT(bid))
            values_list.append(bids_parser.get_bid_participation_url(bid))

            yield values_list
        except KeyError as err:
            log.debug(err)


def parse_awards(response_body):
    data = response_body['data']
    tender_id = tender_parser.get_tender_id(data)

    for award in data['awards']:
        values_list = []
        values_list.append(tender_id)
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


def parse_contracts(response_body):
    data = response_body['data']
    tender_id = tender_parser.get_tender_id(data)

    for contract in data['contracts']:
        values_list = []
        values_list.append(tender_id)
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
        values_list.append(tender_parser.get_number_of_bids(data))

        return values_list
    except Exception as err:
        log.debug(err)


def write_to_csv(list_of_lists, output_name):
    try:
        df = pd.DataFrame(list_of_lists)
        df.reset_index(drop=True, inplace=True)
        df.to_csv(output_name, index=False, header=False)
    except Exception as err:
        log.debug(err)


def divide_into_chunks(lst, n):
    """Divide an array into successive n-sized chunks"""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def loop_through_ids():
    tender_details = []
    lots_details = []
    contracts_details = []
    awards_details = []
    items_details = []
    bids_details = []
    complaints_details = []
    milestones_details = []

    tender_list = db.fetch_from_database()
    print(f"Number of tender ids in db: {len(tender_list)}")

    for row in divide_into_chunks(tender_list, 10):
        def parse_responses(batch):
            try:
                responses = asyncio.run(inst.loop_through_tenders(batch, 0))
                for response_body in responses:
                    try:
                        tender_details.append(inst.parse_tender(response_body))
                    except Exception as err:
                        log.debug(err)

                    try:
                        for list_item in parse_lots(response_body):
                            lots_details.append(list_item)
                    except Exception as err:
                        log.debug(err)

                    try:
                        for list_item in parse_contracts(response_body):
                            contracts_details.append(list_item)
                    except Exception as err:
                        log.debug(err)

                    try:
                        for list_item in parse_awards(response_body):
                            awards_details.append(list_item)
                    except Exception as err:
                        log.debug(err)

                    try:
                        for list_item in parse_items(response_body):
                            items_details.append(list_item)
                    except Exception as err:
                        log.debug(err)

                    try:
                        for list_item in parse_bids(response_body):
                            bids_details.append(list_item)
                    except Exception as err:
                        log.debug(err)

                    try:
                        for list_item in parse_complaints(response_body):
                            complaints_details.append(list_item)
                    except Exception as err:
                        log.debug(err)

                    try:
                        for list_item in parse_milestones(response_body):
                            milestones_details.append(list_item)
                    except Exception as err:
                        log.debug(err)
            except Exception as err:
                log.error(f"Failed to retrieve a response: {err}")
                time.sleep(inst.interval)
                parse_responses(batch)
        parse_responses(row)

        # time.sleep(0.5)

    results_folder_path = f"{os.getcwd()}/resulting_data"

    os.makedirs(results_folder_path, exist_ok=True)
    write_to_csv(tender_details, f"{results_folder_path}/tender_details.csv")
    write_to_csv(lots_details, f"{results_folder_path}/lots_details.csv")
    write_to_csv(contracts_details, f"{results_folder_path}/contracts_details.csv")
    write_to_csv(awards_details, f"{results_folder_path}/awards_details.csv")
    write_to_csv(items_details, f"{results_folder_path}/items_details.csv")
    write_to_csv(bids_details, f"{results_folder_path}/bids_details.csv")
    write_to_csv(complaints_details, f"{results_folder_path}/complaints_details.csv")
    write_to_csv(milestones_details, f"{results_folder_path}/milestones_details.csv")

    print('\n--------------------')
    print('Finished parsing the existing batch of data')
    print('--------------------')


if __name__ == '__main__':
    st_time = time.time()
    print(f"Starting time:", datetime.datetime.now())
    print('Extracting tender details:')
    dk_codes_tuple = ('72410000-7', '72411000-4')
    inst = ProzorroCronScrapper(date_offset='2021-07-20',
                                category='tenders',
                                dk_code=dk_codes_tuple,
                                csv_output_filename=output_filename)
    inst.parse_tender = parse_tender

    try:
        loop_through_ids()
    except Exception as e:
        log.error("This should have never happened:")
        log.error(e)
    print(f"End time: ", datetime.datetime.now())
    print("Total processing time (min):", (time.time() - st_time) / 60)

