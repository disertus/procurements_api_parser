import logging as log
import sqlite3
from prozorro_api_parser.parser_utils.config import db_address


con = sqlite3.connect(db_address)

def create_database():
    try:
        with con:
            con.execute(('''CREATE TABLE IF NOT EXISTS tender_ids
            (tender_ids text PRIMARY KEY NOT NULL);'''))
    except sqlite3.OperationalError as err:
        log.error(err)


def insert_tender_id_into_db(tender_id: str):
    try:
        with con:
            con.execute("INSERT INTO tender_ids (tender_id) VALUES (?)", (tender_id,))
    except sqlite3.IntegrityError as err:
        log.debug(err)


def fetch_from_database():
    try:
        with con:
            return con.execute("SELECT tender_id FROM tender_ids").fetchall()
    except Exception:
        log.warning('Unable to fetch data from database.')
