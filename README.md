This set of ETL scripts is made to extract tender data from Prozorro's API, transform semi-structured responses into structured data, and load it into Google Sheets.

prozorro_public_api_parser.py works in an endless loop, continuously checking for updates within Prozorro's API.

tender_detail_parser.py and sheets_writer.py are both triggered by unix cron jobs with 1h interval. 
