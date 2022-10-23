# 19 July 2022
# v1.2
"""
Tail any consistently growing log file on a remote (network) system, parse it and upsert to SQL database for analysis.
e.g. IIS log files on IIS server, connect to IIS server log file location, identify the new entries made since last checked 
(tail), then parse, transform and upsert to specified SQL database/table.
"""

from datetime import datetime as dt
from datetime import tzinfo
from datetime import time as t
from dateutil import tz
import pytz
import sys
import glob
import os
import time
import pyodbc

# Globals
iis_log_source = r'' # path to logs (no need to identify specific logs)
delay = 1
sql_svr = r'' # sql server
sql_db = '' # db name

def map_source_location(source):
    try:
        os.system(r'NET USE U: {source} /persistent:No'.format(source=source))
    except Exception:
        error_logger('C. ', sys.exc_info())

def identify_latest_log(path):
    ''' Get the latest log file in the directory. ''' 
    return max(glob.glob(os.path.join('{}'.format(path), '*')), key=os.path.getmtime)

def error_logger(point, err):
    with open(r'.\script_errors.txt', 'a+') as f:
        f.write('Point: ' + point)
        f.write('Date|Time: ' + dt.today().strftime('%Y-%m-%d-%H:%M:%S') + '\n')
        f.write('Error: ' + str(err) + '\n')

def parse(row): # return a complete string ready for SQL table update
    """
    parsed = {'DATE': local_date_time[0], 'TIME': local_date_time[1], 'SERVER': sr[3],
                'SERVERIP': sr[4], 'URI': sr[6], 'CIP': sr[10], 'CSVERSION': sr[11],
                'USERAGENT': sr[12], 'COOKIE': sr[13], 'CSHOST': sr[15], 'SCSTATUS': sr[16],
                'ORIGINALIP': sr[22]}
    """
    if row.startswith('#') == False: # ignore IIS comment section at start of log file      
        sr = row.split(' ') # sr (split row), space delimited as per IIS logs
        local_date_time = date_time_convert(sr).split(' ')
        # parsed, mapped as per iis column order.
        parsed = "'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'".format(
            local_date_time[0], local_date_time[1], sr[3], sr[4], sr[6], sr[10],
            sr[11], sr[12], sr[13], sr[15], sr[16], sr[22])
        return parsed
    else:
        return False

def active_time_range(): # time range during which processing occurs
    now = dt.now().time()
    if now > t(2, 0) and now < t(23, 59): # now > 2:30AM AND < 11:59PM
        return True
    else:
        return False

def date_time_convert(sr):
    dt_str = sr[0] + ' ' +sr[1] # e.g. '2022-07-1214:00:22'
    dt_format = "%Y-%m-%d %H:%M:%S" # format as per source
    dt_utc = dt.strptime(dt_str, dt_format).replace(tzinfo=pytz.UTC) # create UTC object for conversion

    local_zone = tz.tzlocal() # specify sydney time
    dt_local = dt_utc.astimezone(local_zone) # convert!

    return dt_local.strftime(dt_format)

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
            print('conn already closed', sys.exc_info())
        error_logger('ODBC', sys.exc_info())

def upload_data_to_sql(row, cursor):
    # row is dictionary
    # call parse
    p = parse(row)
    if p != False:
        try:
            cursor.execute('''
                            INSERT INTO IIS_Logs_2210 (DATE, TIME, SERVER, SERVERIP, URI, CIP,
                            CSVERSION, USERAGENT, COOKIE, CSHOST, SCSTATUS, ORIGINALIP)
                            VALUES
                            ({})
                            '''.format(p))
            cursor.commit()
        except Exception:
            error_logger('SQL commit', sys.exc_info())
    else:
        pass

if __name__ == "__main__":
    size = 0
    offset = 0
    entry_number = 0 # counter reset to count number of rows upserted
    map_source_location(iis_log_source)
    while True:
        try:
            if active_time_range(): 
                cursor = odbc_connection(sql_svr, sql_db) # establish connection
                latest_log = identify_latest_log('u:')
            else: # reset everything and snooze
                size = 0
                offset = 0
                try:
                    while True:
                        if active_time_range():
                            latest_log = identify_latest_log('u:')
                            break
                        else:
                            time.sleep(60)
                            print('Snoozing:', dt.now().strftime("%Y-%m-%d, %H:%M:%S"))
                except Exception:
                    error_logger('E. ', sys.exc_info())
        except Exception:
            error_logger('D. ', sys.exc_info())
        try:
            time.sleep(delay)
            if os.path.getsize(latest_log) > size: # log has grown
                size = os.path.getsize(latest_log) # monitor log size
                with open(latest_log) as log:
                    log.seek(offset)
                    try:
                        while True:
                            offset = log.tell()
                            line = log.readline()
                            if line == '\n' or offset == log.tell() or line.startswith('#'): # blank or no new line i.e. eof
                                time.sleep(delay)
                                if active_time_range():
                                    continue
                                else:
                                    break 
                            else:
                                upload_data_to_sql(line, cursor) # parse and upsert
                                entry_number += 1
                                if entry_number == 1000:
                                    print(str(entry_number) + ' more rows added on: ' +
                                          dt.now().strftime("%Y-%m-%d, %H:%M:%S"))
                                    entry_number = 0
                    except Exception:
                        error_logger('B. ', sys.exc_info())
            else: # stub, source log hasn't changed
                pass
        except Exception:
            error_logger('A. ', sys.exc_info())
