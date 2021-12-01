import logging as log
import sqlite3
from parser_utils.config import db_address


con = sqlite3.connect(db_address)

def create_database():
    try:
        with con:
            con.execute(('''CREATE TABLE IF NOT EXISTS tender_ids
            (tender_id text PRIMARY KEY NOT NULL);'''))
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

def check_if_tender_in_db(tender_id):
    try:
        with con:
            return con.execute("SELECT tender_id FROM tender_ids WHERE tender_id = (?)", (tender_id,)).fetchall()
    except Exception:
        log.debug('The tender is not in the database.')