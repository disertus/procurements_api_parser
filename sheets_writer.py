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



if __name__ == "__main__":
    while 1:
        try:
            try:
                write_to_gs(
                        sheet_id=os.environ['GS_ID'], 
                        path_to_token=os.environ['PATH_TO_GS_TOKEN'], 
                        dataframe=format_dataframe('tender_details.csv'),
                        tab_name='Sheet1'
                        )
                print('1/5 successfully written tenders info\n')
            except Exception as err:
                print(err)
                raise

            try:
                write_to_gs(sheet_id=os.environ['GS_ID'], 
                        path_to_token=os.environ['PATH_TO_GS_TOKEN'],
                        dataframe=format_dataframe('lots_details.csv'),
                        tab_name='lots'
                        )
                print('2/5 successfully written lots info\n')
            except Exception as err:
                print(err)
                raise
            
            try:
                write_to_gs(sheet_id=os.environ['GS_ID'], 
                        path_to_token=os.environ['PATH_TO_GS_TOKEN'],
                        dataframe=format_dataframe('awards_details.csv'),
                        tab_name='awards'
                        )
                print('3/5 successfully written awards info\n')
            except Exception as err:
                print(err)
                raise

            try:
                write_to_gs(sheet_id=os.environ['GS_ID'], 
                        path_to_token=os.environ['PATH_TO_GS_TOKEN'],
                        dataframe=format_dataframe('contracts_details.csv'),
                        tab_name='contracts'
                        )
                print('4/5 successfully written contracts info\n')
            except Exception as err:
                print(err)
                raise

            try:
                write_to_gs(sheet_id=os.environ['GS_ID'], 
                        path_to_token=os.environ['PATH_TO_GS_TOKEN'],
                        dataframe=format_dataframe('items_details.csv'),
                        tab_name='items'
                        )
                print('5/5 successfully written contracts info\n')
            except Exception as err:
                print(err)
                raise
        
            print("Entering hibernaiton mode\n")

            print('--------------------')
            print('Finished writing the existing batch of data')
            t = time.localtime()
            print(time.strftime("%H:%M:%S", t))
            print('--------------------')
        
            time.sleep(3600)
            
        except Exception as err:
            print(err)
            break
