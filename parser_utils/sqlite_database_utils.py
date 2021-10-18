
import sqlite3




con = sqlite3.connect("/prozorro_api_parser/parser_utils/tenders.db")


def create_database():
    try:
        with con:
            con.execute(('''CREATE TABLE tender_ids
            (tender_ids text PRIMARY KEY NOT NULL);'''))
    except sqlite3.OperationalError as err:
        pass


def insert_tender_id_into_db(tender_id: str):
    try:
        with con:
            con.execute("INSERT INTO tender_ids (tender_id) VALUES (?)", (tender_id,))
    except sqlite3.IntegrityError as err:
        pass


def fetch_from_database(*args):
    try:
        with con:
            cur = con.execute("select tender_id from tender_ids")
            print([print(i[0]) for i in con.execute("select count(tender_id) from tender_ids")])
            return cur
    except:
        raise




if __name__ == "__main__":
    print('Db package initialized\n')
    fetch_from_database()