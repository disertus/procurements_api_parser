import logging as log
import os
import pygsheets
import pandas as pd
import time


def write_to_gs(sheet_id: str, path_to_token: str, dataframe, tab_name: str):
    """Writes a dataframe to a google sheet"""
    gc = pygsheets.authorize(service_file=path_to_token)
    google_sheet = gc.open_by_key(sheet_id)
    wks = google_sheet.worksheet_by_title(tab_name)

    #starts writing the data from 2 row and 2 column of a google sheet, which leaves the possibility to define column headers manually inside gs
    wks.set_dataframe(dataframe, (2,2)) 
    print('data written')


def read_csv_into_dataframe(csv_filename:str):
    """Reads the csv, deduplicates and cleans it, returns a dataframe"""
    df = pd.read_csv(csv_filename)
    df = df.drop_duplicates()
    print('read the csv file into dataframe')
    return df


def format_dataframe(csv_filename):
    """Resets the index on a csv file"""
    df = read_csv_into_dataframe(csv_filename=csv_filename)
    df.reset_index(drop=True, inplace=True)
    print(df)

    return df


def loop_through_sheets(source_csv_filename: str, destination_tab_name: str) -> None:
    try:
        write_to_gs(
            sheet_id=os.environ['GS_ID'],
            path_to_token=os.environ['PATH_TO_GS_TOKEN'],
            dataframe=format_dataframe(source_csv_filename),
            tab_name=destination_tab_name
            )
        print(f'Successfully written data to {destination_tab_name}.')
    except Exception as err:
        log.error(f'An error occurred while writing {source_csv_filename} data to the {destination_tab_name} GS tab: {err}')


if __name__ == "__main__":
    csv_tuple = ['tender_details.csv', 'lots_details.csv', 'awards_details.csv', 'contracts_details.csv', 'items_details.csv']
    tabs_tuple = ('Sheet1', 'lots', 'awards', 'contracts', 'items')

    try:
        for csv, tab in zip(csv_tuple, tabs_tuple):
            loop_through_sheets(csv, tab)

        print("Entering hibernaiton mode\n")

        print('--------------------')
        print('Finished writing the existing batch of data')
        t = time.localtime()
        print(time.strftime("%H:%M:%S", t))
        print('--------------------')

        time.sleep(3600)

    except Exception as err:
        log.error(err)
