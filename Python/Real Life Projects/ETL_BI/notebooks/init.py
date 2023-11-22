import pandas as pd
#from datetime import datetime
import sys
import os
from sqlalchemy import create_engine
import shutil

import check_server
import log_config
import organize_files as of

try:
    tmp_fold = os.environ['tmp']
except:
    print("Cannot find temp location. Activities won't be logged")
else:
    log_vals = log_config.start_log(tmp_fold)
    init_logger = log_vals[0]

SCHEMA_REF = {
    "Client Billing Descending":"client_billing","Fee Breakdown by Dept and Fee Earner":"fee_brkdn_dept_fe","Fee Summary by Dept and Fee Earner":"fee_smry_dept_fe",
    "Fees Billed":"fees_billed","Matter Source of Business inc Matter Bills (Bill Date)":"mttr_src_ref","Total Hours by Fee Earner-With Billings All":"tot_hrs_by_fe",
    "Matters Opened by FE":"mtrs_by_fe","Payment Received Analysis":"pmt_rcv_analysis"}

def init_checks(info_file=None):
    all_details = {}
    info_dict = {}

    if info_file is not None:
        if os.path.exists(info_file):
            init_logger.info("Found [{info_file}]")
            all_details["loc"] = info_file

            opened_file = open(all_details["loc"], encoding='utf8')
            from csv import reader
            read_file = reader(opened_file)
            db_creds = list(read_file)
            opened_file.close()

            for info in db_creds:
                info_dict[info[0]] = info[1]
            
            all_details['file_loc'] = info_dict["base_loc"]
            all_details['log_file'] = all_details['file_loc']+"\\Logs"
            all_details['final_files'] = all_details['file_loc']+"\\Final_Df"
            all_details['backup'] = all_details['file_loc']+"\\Backup"
            all_details['trigger_file'] = all_details['file_loc']+"\\file_trigger\\new_data_received.txt"

            imp_folds = ['file_loc','log_file','final_files','backup','trigger_file']
            for fold in imp_folds:
                if os.path.exists(all_details[fold]):
                    init_logger.info(f"Found {all_details[fold]}")
                else:
                    init_logger.info(f"Folder {all_details[fold]} Doesn't exist")

            all_details['db'] = info_dict["db_name"]
            all_details['db_user'] = info_dict["db_user"]
            all_details['db_password'] = info_dict["db_password"]
            all_details['db_host'] = info_dict["db_host"]
            all_details['db_port'] = info_dict["db_port"]

            return all_details
        else:
            init_logger.info("Important Variables not set.")
            init_logger.info("Exiting ...")
            sys.exit()
    else:
        i1 = check_server.chk_srvr()
        if i1.chk_base_dirs() != -1 & i1.chk_creds() != -1:
    
            init_logger.info("Checking Environment Variables for information")
            srv_dirs = i1.chk_base_dirs()
            db_creds = i1.chk_creds()

            if srv_dirs:
                all_details['file_loc'] = srv_dirs[0]
                all_details['log_file'] = srv_dirs[1]
                all_details['final_files'] = srv_dirs[2]
                all_details['backup'] = srv_dirs[3]
                all_details['trigger_file'] = srv_dirs[4]

                init_logger.info("Variables Set Successfully for File Locations")
            else:
                init_logger.info("Environment Variables for Files is not set")
                init_logger.info("Checking Environment Variables for DB Credentials ")

            if db_creds:
                all_details['db'] = db_creds[0]
                all_details['db_user'] = db_creds[1]
                all_details['db_password'] = db_creds[2]
                all_details['db_host'] = db_creds[3]
                all_details['db_port'] = db_creds[4]
                init_logger.info("Variables Set successfully for DB Creds ")
                return all_details
        else:
            init_logger.info("Environment Variables not set for DB Credentials.")
            return -1

def get_consolidated_data_dict(info_file=None):
    if os.name == 'posix':
        init_logger.info("Getting Details from the Environment Variables")
        all_info = init_checks()
    elif os.name == 'nt':
        init_logger.info("Running on Windows")
        loc = info_file
        if os.path.exists(loc):
            all_info = init_checks(loc)
        else:
            init_logger.info(f"File [{loc}] not Found")
            sys.exit()

    dict_list = of.categorize_files(all_info["file_loc"],init_logger)
    all_files = of.concat_files(dict_list, all_info["file_loc"],init_logger)
    return all_files, all_info

def write_data_to_filesys(info_file_loc=None):
    ret_vals = get_consolidated_data_dict(info_file_loc)
    all_files_dict = ret_vals[0]
    all_info_dict = ret_vals[1]
    final_files = all_info_dict['final_files']
    backup = all_info_dict['backup']

    init_logger.info("*"*50)
    init_logger.info("Adding all Entries to main CSV")
    init_logger.info("*"*50)

    for file in all_files_dict.keys():
        init_logger.info(f"Converting all Columns to Lowercase for [{file}]")
        all_files_dict[file].columns = [cols.lower() for cols in all_files_dict[file].columns]

        fname = final_files+"/"+file+".csv"
        if os.path.exists(fname):
            init_logger.info(f'{fname} already exists')
            all_files_dict[file].to_csv(fname,header=False,mode="a",index=False)
        else:
            init_logger.info(f'Creating DF {fname}')
            all_files_dict[file].to_csv(fname,mode="a",index=False)
    
    init_logger.info("*"*50)
    init_logger.info("Creating Secondary Backup File")
    init_logger.info("*"*50)

    for csv_file in os.listdir(final_files):
        init_logger.info(f'Processing [{csv_file}]')
        if csv_file.endswith('.csv'):
            tmp_df = pd.read_csv(final_files+"/"+csv_file)
            tmp_df['date_added'] = pd.to_datetime(tmp_df['date_added'],format="%d-%m-%y", errors='ignore')
            fname = backup+"/"+csv_file.split(".")[0]+".parquet.gzip"
            init_logger.info(f'Name of Backup File is [{fname}]')
            if os.path.exists(fname):
                init_logger.info(f'Backup File already Exists, Removing')
                os.remove(fname)
            else:
                init_logger.info(f'Creating Backup File [{fname}]')
                tmp_df.to_parquet(fname,compression = 'gzip')
            init_logger.info("*"*50)
    return all_files_dict,all_info_dict

def write_data_to_db(file_loc=None):
    info = write_data_to_filesys(file_loc)
    local_all_info = info[1]
    local_all_files = info[0]
    try:
        engine = create_engine(f'postgresql://{local_all_info["db_user"]}:{local_all_info["db_password"]}@{local_all_info["db_host"]}:{local_all_info["db_port"]}/{local_all_info["db"]}',echo=True)
    except:
        init_logger.info("Unable to connect to the Database, Exiting.. ")
        sys.exit()
    else:
        init_logger.info("Database Connection Established")

    #final_files = local_all_info['final_files']

    for file in local_all_files.keys():
        local_all_files[file].columns = [cols.lower() for cols in local_all_files[file].columns]
                
        # Change the Column names for Fees Billed
        # Make month to tnx_month and splitamount tp split_amount
        if file == "Fees Billed":
            init_logger.info("The Values of Months before change are ")
            init_logger.info(local_all_files[file].columns)
            local_all_files[file].rename(columns = {'month':'tnx_month', 'splitamount':'split_amount'}, inplace = True)
            init_logger.info("** Renaming Columns for Fees Billed.csv **")
            init_logger.info("The Values of Months after changes are ")
            init_logger.info(local_all_files[file].columns)
        #fname = final_files+"/"+file+".csv"

        #fname = final_files+"/"+file+".csv"
        init_logger.debug(f"Appending all data in Postgresql Server for [{file}]")

        try:            
            local_all_files[file].to_sql(SCHEMA_REF[file],con=engine,if_exists='append',index=None)   # Send the data to database
        except:
            init_logger.error(f"***** Unable to write to Database for file [{file}]********* ")
        else:
            continue
        init_logger.info(f'Processed File [{file}]')
    init_logger.info(f'Moving log files from [{log_vals[1]}] to [{local_all_info["log_file"]}]')
    shutil.copy(log_vals[1],local_all_info["log_file"])

def main():
    # info_loc = "D:\Learning+Offline\db_creds.csv"
    write_data_to_db()   

if __name__ == "main":
    main()