# 11 September 2022
# v0.9
"""
To be used with "final-warranty-check.ps1"
Transform json file (output of final-warranty-check.ps1), extracting most relevant warranty information, 
then upsert into specified SQL database for further analysis.
"""

from datetime import datetime, date
from datetime import timedelta
import sys
import csv
import dateutil.parser
import pytz
import pyodbc
import json

sql_svr = r"" # SQL server (hostname)
sql_db = "" # SQL database name

def convert_date_time(arg):
    """ From ISO 8601 / UTC to local time.
    """
    if arg != "": 
        d = dateutil.parser.parse(arg)
        local_date_time = d.astimezone(pytz.timezone("Australia/Sydney"))
        return local_date_time.strftime("%Y-%m-%d") # %I:%M:%S %p")
    else:
        return ""

def error_logger(point, err):
    with open(r'.\warranty_transform_error.txt', 'a+') as f:
        f.write('\n')
        f.write('Point: ' + point)
        f.write('Date|Time: ' + datetime.today().strftime('%Y-%m-%d %H:%M:%S') + '\n')
        f.write('Error: ' + str(err) + '\n')

def odbc_connection(sql_svr, sql_db):
    try:
        conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' +
                              sql_svr + ';DATABASE=' +
                              sql_db + ';Trusted_Connection=yes;')
        return conn.cursor()
    except Exception:
        try:
            conn.close()
        except Exception:
            print('cursor already closed', sys.exc_info())
        try:
            conn.close()
        except Exception:
            print(sys.exc_info())
            print('conn already closed', sys.exc_info())

def upload_data_to_sql(cursor, vals):
    # row is dictionary
    # call parse
    try:
        parsed = f"INSERT INTO device_warranty_01 (Manufacturer, Serial, InWarranty, Purchased, Shipped, Contracts, StartDate, EndDate) VALUES (?,?,?,?,?,?,?,?)"
        cursor.execute(parsed, vals[0], vals[1], vals[2], vals[3], vals[4], vals[5], vals[6], vals[7])
        cursor.commit()
    except Exception:
        print('SQL commit:', sys.exc_info())

# mainline
raw_load = []
cleaned = []
upload_ready = []

with open(r'.\warranty_check.json') as jsonfile:
    for obj in jsonfile:
        try:
            raw_load.append(json.loads(obj))
        except Exception as e:
            error_logger("A", e)

    # convert dates to appropriate format
    for json_obj in raw_load:
        try: 
            if json_obj["purchased"] != "0001-01-01T00:00:00":
                json_obj["purchased"] = convert_date_time(json_obj["purchased"])
        except Exception as e:
            error_logger("B", e)
        try:
            json_obj["shipped"] = convert_date_time(json_obj["shipped"])
        except Exception as e:
            error_logger("C", e)
        try:
            if json_obj["contracts"] != {}:
                for contract_type in json_obj["contracts"]:
                    temp = []
                    for _date in json_obj["contracts"][contract_type]:
                        temp.append(convert_date_time(_date))
                    json_obj["contracts"][contract_type] = temp
        except Exception as e:
            error_logger("D", e)
            pass
            
        cleaned.append(json_obj)                

    # Obtain the earliest / latest or obtain based on contract type?
    for json_obj in cleaned:
        temp = dict()
        temp["device"] = json_obj["device"]
        temp["inwarranty"] = json_obj["inwarranty"]
        temp["purchased"] = json_obj["purchased"]
        temp["shipped"] = json_obj["shipped"]

        furthest_future_contract = date(1990,1,1)
        contract = ""
        earliest_start_contract = date.today()
        
        for contract_type in json_obj["contracts"]:            
            for _date in json_obj["contracts"][contract_type]:
                y, m, d = _date.split('-')
                ymd = date(int(y), int(m), int(d))
                if ymd > furthest_future_contract:
                    furthest_future_contract = ymd
                    contract = contract_type

        temp["contract"] = contract

        for contract_type in json_obj["contracts"]:            
            for _date in json_obj["contracts"][contract_type]:
                y, m, d = _date.split('-')
                ymd = date(int(y), int(m), int(d))
                if ymd < furthest_future_contract:
                    earliest_start_contract = ymd

        temp["StartDate"] = earliest_start_contract.strftime('%Y-%m-%d')
        temp["EndDate"] = furthest_future_contract.strftime('%Y-%m-%d')

        upload_ready.append(temp)

# upload to SQL
cursor = odbc_connection(sql_svr, sql_db)
for i in upload_ready:
    try: 
        temp = []
        temp.append("Lenovo")
        temp.append(i["device"])
        temp.append(i["inwarranty"])
        if i["purchased"] == "0001-01-01T00:00:00":
            temp.append(None)
        else:
            temp.append(i["purchased"])
        temp.append(i["shipped"])
        if (i["contract"]) == "":
            temp.append(None)
        else:
            temp.append(i["contract"])
        temp.append(i["StartDate"])
        temp.append(i["EndDate"])
        upload_data_to_sql(cursor=cursor, vals=temp)
    except Exception:
        print("Point: E", sys.exc_info())
