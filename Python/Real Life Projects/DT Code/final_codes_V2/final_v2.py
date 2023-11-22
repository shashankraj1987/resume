# Setup: Imports

import requests
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import os

# Progress Bar


def progress_bar(progress, total, obj):
    percent = 100*(float(progress)/float(total))
    bar = '█'*int(percent)+'-'*(100-int(percent))
    print(f"\r|{bar}| {percent: .2f}% Processing : {obj}", end='\r')


# Setup: Get API Keys and Connection.


def read_creds(cred_loc, main=1, backup=0):
    if os.path.exists(cred_loc):
        f = open(cred_loc)
        creds = json.load(f)
        f.close()

        api_key = creds["nagmani.b@exalogic.co"]

        emp_url = creds["emp_url"]
        app_url = creds["app_url"]
        print("Received the Credentials.")
        return api_key, emp_url, app_url
    else:
        print("File not found")
        return []

# Extract: Extract the Data from Desktime app.


def get_dt_data(url: str, api: str, dt: str, emp=""):
    if emp:
        try:
            r = requests.get(
                url, params={"apiKey": api, "date": dt, "id": emp})
        except Exception as e:
            print(e)
            return ""
        else:
            # print("Desktime Connection Successful.")
            return r.json()
    else:
        try:
            # print("Desktime Connection Successful.")
            r = requests.get(url, params={"apiKey": api, "date": dt})
        except Exception as e:
            print(e)
            return ""
        else:
            return r.json()

# Transform: Final DT API Processing Function.


def process_dt_api(dt, resp, df_act_emp):
    emp_logged = []
    today = resp['employees'][dt]
    for emp in today:
        emp_logged.append(emp)

    remove_cols = ['profileUrl', 'activeProject', 'notes', 'afterWorkTime', 'beforeWorkTime',
                   'work_starts', 'work_ends', 'arrived', 'left', 'late', 'isOnline']
    cols = []

    for x in resp['employees'][dt][emp_logged[0]]:
        if not remove_cols.__contains__(x):
            cols.append(x)
    dict1 = {}
    for col in cols:
        dict1[col] = []

    for emp in emp_logged:
        tmp = resp['employees'][dt][emp]
        dict1['id'].append(str(tmp['id']))
        dict1['name'].append(tmp['name'])
        dict1['email'].append(tmp['email'])
        dict1['groupId'].append(tmp["groupId"])
        dict1['group'].append(tmp["group"])
        dict1['onlineTime'].append(tmp["onlineTime"])
        dict1['offlineTime'].append(tmp["offlineTime"])
        dict1['desktimeTime'].append(tmp["desktimeTime"])
        dict1['atWorkTime'].append(tmp["atWorkTime"])
        dict1['productiveTime'].append(tmp["productiveTime"])
        dict1['productivity'].append(tmp["productivity"])
        dict1['efficiency'].append(tmp["efficiency"])

    df1 = pd.DataFrame(dict1)
    df1.reset_index(inplace=True, drop=True)

    df_merged = df_act_emp.merge(df1,
                                 left_on="Email",
                                 right_on="email",
                                 suffixes=("_left", "_right"),
                                 how="left")
    df_merged["date"] = dt
    df_merged["count"] = 1
    print("Complete Data converted to a Dataframe.")
    return df_merged


def update_exist_emp(exist_emp_df, df_dt_usage):
    df_tmp = df_dt_usage[["Emp ID", "Reporting Manager",
                          "Department", "Division", "id"]]
    df_merged = exist_emp_df.merge(
        df_tmp,
        left_on="Emp ID",
        right_on="Emp ID",
        how="left",
        suffixes=("_existing", "_new")
    )

    return df_merged

# Transform2: Get App Usage from Desktime per employee.


df_app_list = pd.DataFrame(columns=[
                           'Date', 'Name', 'App_Name', 'App_Type', 'App_Tag', 'Time_Mins', 'Category', 'Productive'])


def get_meeting_time(x):
    if x.__contains__('teams') | x.__contains__('CALENDAR') | x.__contains__('Teams'):
        return "Teams"
    else:
        return "Work"


def get_app_usage(emp_id, emp_resp, dt):
    dict_usage = {"Date": [], "Desktime_ID": [], "emp_app_name": [], "emp_time_mins": [
    ], "emp_time_secs": [], "emp_category": [], "emp_productive": []}
    # print(emp_resp)
    app_usage = emp_resp["apps"]['1']

    for app in app_usage:
        t_spent_mins = np.round(
            int(emp_resp["apps"]["1"][app]["duration"])/60, 2)
        t_spent_secs = int(emp_resp["apps"]["1"][app]["duration"])
        app_name = emp_resp["apps"]["1"][app]["app"]
        app_type = emp_resp["apps"]["1"][app]["type"]
        dict_usage["Date"].append(dt)
        dict_usage["Desktime_ID"].append(emp_id)
        dict_usage["emp_app_name"].append(app_name)
        dict_usage["emp_time_mins"].append(t_spent_mins)
        dict_usage["emp_time_secs"].append(t_spent_secs)
        dict_usage["emp_category"].append(get_meeting_time(app_name))
        dict_usage["emp_productive"].append("Productive")

    return pd.DataFrame(dict_usage)


# Load: Main Function

def main(n=1, location="", strt_dt=""):
    '''
    n = Number of days back you want to look for the data from Desktime. Default value is 1. 

    merge_all = Create a Mega Merged file containing all data at one place. Takes a lot of storage. Default value is False. 

    location = The place where files need to be saved and from where credentials will be picked. 
    There should be two folders under the main location: 
    /Creds, which will contain a file called dt_cred.json. 
    /Data_Files, where all the data will be dumped. 

    You may specify a custom location but the API file, Existing Employee File, and folders need to be present at that location. 

    If Start Date (strt_dt) is provided, the script will walk back n days from start date and give those dates as details.
    It should be in YYYY-MM-DD format
    '''
    date_format = '%Y-%m-%d'

    # Define the location of Creds File and Variables
    if location == "":
        loc = "C:/Users/ShashankRaj/OneDrive - Exalogic Consulting/Documents/Proj Mgmt/Jira_Clockwork"
        # loc = "C:/Users/shash/OneDrive/Private Documents/My Learning Repo/Python/Proj Mgmt"
    else:
        loc = location

    cred_loc = loc+"/Creds"
    Data_loc = loc+"/Data_Files"
    cloud_loc = "G:/My Drive/Exalogic Internal Data"
    # cloud_loc="C:/Users/shash/OneDrive/Private Documents/My Learning Repo/Python/Proj Mgmt/Cloud_Drive"

    if os.path.exists(cred_loc) & os.path.exists(Data_loc):
        cred_file = cred_loc+"/dt_cred.json"
        df_emp = pd.read_csv(Data_loc+"/Act_Emp.csv")
    else:
        print("Unable to proceed without Credentials.")
        os.abort()

    DT_App_Usage = pd.DataFrame(columns=[
                                "Date",	"Desktime_ID",	"emp_app_name", "emp_time_mins", "emp_category", "emp_productive"])

    DT_EMP_List = pd.DataFrame(columns=["STATUS", "Emp ID", "Name", "Email", "Reporting Manager", "Department", "Division", "Internal/Contract", "id", "group",
                                        "onlineTime", "productiveTime", "productivity", "efficiency", "date", "count", "Last Name "])

    # Get the date range for which this code will execute
    date_range = []
    if len(strt_dt) < 2:
        curr_dt = str(datetime.today()-timedelta(days=int(0)+1)).split(" ")[0]
        date_range.append(curr_dt)
        print(f'Start date is {curr_dt}')
    else:
        for dt in np.arange(n):
            curr_dt = str(datetime.strptime(strt_dt, date_format) -
                          timedelta(days=int(dt)+1)).split(" ")[0]
            date_range.append(curr_dt)

    # Read the credentials
    api_key, emp_url, app_url = read_creds(cred_file, main=1)

    # Check if we got the API key. If yes, we proceed with getting API Data from Desktime for evety date object.
    if len(api_key) > 2:
        for dt_val in date_range:
            print("*"*30)
            print(f"\n Processing Data for {dt_val} ")
            # TODO: asyncio can be used here to read multiple dates in a go.
            json_data = get_dt_data(url=emp_url, api=api_key, dt=dt_val)

            # Gives the Employees' desktime usage
            df_stg1 = process_dt_api(
                dt=dt_val, resp=json_data, df_act_emp=df_emp)

            # Getting the employee list for all employee Data for the given date range
            DT_EMP_List = pd.concat([DT_EMP_List, df_stg1])

            # Getting all the Employees whose reference is present in Desktime.
            t_emp = df_stg1[~df_stg1["id"].isna()]["id"]

            # Enrich Employee list before Getting App Usage
            enrichd_list = update_exist_emp(df_emp, df_stg1)

            print(f"Total {len(t_emp)} employees.")

            # Getting the individual app usage for each employee.
            progress_bar(0, len(t_emp), "")
            for i, emp in enumerate(t_emp):

                # TODO: This is a Time expensive call. Should be pushed to multiprocessing module.
                app_usg_json = get_dt_data(app_url, api_key, dt_val, emp=emp)
                df_stg2 = get_app_usage(emp, app_usg_json, dt_val)

                # All Employees' usage data is here.
                DT_App_Usage = pd.concat([DT_App_Usage, df_stg2])

                progress_bar(i+1, len(t_emp), emp)

        # TODO: This is a space extensive call. Change the file format for final processing.
        # print(DT_App_Usage.columns)
        # print(enrichd_list.columns)

        DT_App_Usage = DT_App_Usage.merge(
            enrichd_list,
            left_on="Desktime_ID",
            right_on="id",
            how="left"
        )

        # print(DT_EMP_List.columns)
        # print(DT_App_Usage.columns)

        DT_EMP_List = DT_EMP_List[["STATUS", "Emp ID", "Name", "Email", "Reporting Manager", "Department", "Division", "Internal/Contract", "id", "group",
                                   "onlineTime", "productiveTime", "productivity", "efficiency", "date", "count", "Last Name "]]

        DT_App_Usage = DT_App_Usage[["Date", "Desktime_ID", "emp_app_name", "emp_category", "emp_time_secs", "Emp ID", "Name", "Last Name ",
                                     "Email", "Reporting Manager_existing", "Department_existing", "Division_existing",
                                     "Internal/Contract"]]

        # ====================== Fix Some Data Issues with DT_EMP_List ==================================
        # Concatenating the Names
        DT_EMP_List["Full_Name"] = DT_EMP_List["Name"] + \
            " "+DT_EMP_List["Last Name "]

        # Fixing the Employee IDs
        DT_EMP_List["Emp ID"].fillna(0000, inplace=True)
        DT_EMP_List["id"].fillna(0000, inplace=True)

        DT_EMP_List["Emp ID"] = DT_EMP_List["Emp ID"].astype("str")
        DT_EMP_List["id"] = DT_EMP_List["id"].astype("str")

        DT_EMP_List["Emp ID"] = DT_EMP_List["Emp ID"].str.split(".", expand=True)[
            0]
        DT_EMP_List["id"] = DT_EMP_List["id"].str.split(".", expand=True)[0]

        # Fixing Reporting Manager
        DT_EMP_List["Reporting Manager"].fillna("HOD", inplace=True)

        # If onlinetime blank -> (-1)
        DT_EMP_List["onlineTime"].fillna("-1", inplace=True)
        DT_EMP_List.drop(columns=['Name', 'Last Name '], inplace=True)
        DT_EMP_List = DT_EMP_List.iloc[:, [
            13, 15, 1, 7, 2, 0, 3, 5, 4, 6, 8, 9, 10, 11, 12, 14]]

        # ================================================================================================

        DT_App_Usage.columns = ["Date", "Desktime_ID", "emp_app_name", "emp_category", "emp_time_secs", "Emp ID", "Name", "Last Name ",
                                "Email", "Reporting Manager", "Department", "Division", "Internal/Contract"]

        if os.path.exists(Data_loc+"/Desktime_usage.csv"):
            print("File Already exists. Will Skip the header.")
            try:
                DT_EMP_List.to_csv(cloud_loc+"/Desktime_usage.csv",
                                   index=False, mode='a', header=False)
                DT_App_Usage.to_csv(
                    cloud_loc+"/DT_App_Usage.csv", index=False, mode='a', header=False)

                DT_EMP_List.to_csv(Data_loc+"/Desktime_usage.csv",
                                   index=False, mode='a', header=False)
                DT_App_Usage.to_csv(
                    Data_loc+"/DT_App_Usage.csv", index=False, mode='a', header=False)
            except FileNotFoundError:
                print("Unable to export to Excel Sheet.")
            else:
                print("Exported the data to Excel Sheets.")
        else:
            try:
                DT_EMP_List.to_csv(
                    cloud_loc+"/Desktime_usage.csv", index=False, mode='a')
                DT_App_Usage.to_csv(
                    cloud_loc+"/DT_App_Usage.csv", index=False, mode='a')

                DT_App_Usage.to_csv(
                    Data_loc+"/DT_App_Usage.csv", index=False, mode='a')
                DT_EMP_List.to_csv(
                    Data_loc+"/Desktime_usage.csv", index=False, mode='a')
            except FileNotFoundError:
                print("Unable to export to Excel Sheet.")
            else:
                print("Exported the data to Excel Sheet.")
    return "Process Successful"


# Processing the Jira Data.

def load_json_file(file_name):
    if not file_name.endswith(".json"):
        print("Only Josn file is expected. Quitting...")
        return
    else:
        try:
            f = open(file_name, encoding='utf8')
        except Exception:
            return Exception
        else:
            data = json.load(f)
            print("Loaded Json Data")
            return data


def transform_json_data(raw_json, df_act_emp, dt):
    dict = {
        "user_names": [],
        "email_add": [],
        "jira_ids": [],
        "issue_key": [],
        "issue_id": [],
        "created": [],
        "started": [],
        "updated": [],
        "time_secs": []
    }

    for x in raw_json:
        dict['user_names'].append(x["author"]["displayName"])
        dict['email_add'].append(x["author"]["emailAddress"])
        dict['jira_ids'].append(x["id"])
        dict['issue_key'].append(x["issue"]["key"])
        dict['issue_id'].append(x["issueId"])
        dict['time_secs'].append(x["timeSpentSeconds"])
        dict["created"].append(x["created"])
        dict["updated"].append(x["updated"])
        dict["started"].append(x["started"])

    df_jira = pd.DataFrame(dict)
    df_jira["created"] = pd.to_datetime(df_jira["created"])
    df_jira["started"] = pd.to_datetime(df_jira["started"])
    df_jira["updated"] = pd.to_datetime(df_jira["updated"])

    df_jira["created"] = df_jira["created"].dt.date
    df_jira["started"] = df_jira["started"].dt.date
    df_jira["updated"] = df_jira["updated"].dt.date

    df_merged_act_emp = df_act_emp.merge(
        df_jira,
        how='left',
        left_on="Email",
        right_on="email_add"
    )

    # df_merged_act_emp["date_issue"] = str(datetime.today()-timedelta(days=int(0)+1)).split(" ")[0]
    # curr_dt = str(datetime.today()-timedelta(days=int(0)+1)).split(" ")[0]
    df_merged_act_emp["date_issue"] = str(
        datetime.strptime(dt, "%Y-%m-%d").date())

    df_merged_act_emp["count"] = 1

    # ==================================== Fixing Data Issues in the Dataframe ============================

    df_merged_act_emp["Full_Name"] = df_merged_act_emp["Name"] + \
        " "+df_merged_act_emp["Last Name "]
    df_merged_act_emp.drop(
        columns=['user_names', 'email_add', 'jira_ids', 'Name', 'Last Name '], inplace=True)
    df_merged_act_emp["issue_key"].fillna("NA", inplace=True)
    df_merged_act_emp["Project"] = df_merged_act_emp["issue_key"].str.split(
        "-", expand=True)[0]
    df_merged_act_emp['Emp ID'] = df_merged_act_emp['Emp ID'].astype(
        'str').str.split(".", expand=True)[0]
    df_merged_act_emp = df_merged_act_emp.iloc[:, [
        13, 16, 15, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14]]

    # =====================================================================================================

    return df_merged_act_emp


def process_jira(proc_dt, loc=""):
    '''
    dt = Date for which this file will be processed. 
    loc = Location of the jira Folder where the log.json file is stored. 

    '''
    if os.path.exists(loc):
        location = loc
    else:
        location = "C:/Users/ShashankRaj/OneDrive - Exalogic Consulting/Documents/Proj Mgmt/Jira_Clockwork"
        # location = "C:/Users/shash/OneDrive/Private Documents/My Learning Repo/Python/Proj Mgmt"
    cloud_loc = "G:/My Drive/Exalogic Internal Data"
    df_act_emp = pd.read_csv(location+"/Data_Files/Act_Emp.csv")
    # df_act_emp = act_emp

    jira_data = load_json_file(location+"/Data_Files/log.json")
    final_df = transform_json_data(jira_data, df_act_emp, dt=proc_dt)

    if os.path.exists(location+"/Data_Files/Jira_Data.csv"):
        final_df.to_csv(location+"/Data_Files/Jira_Data.csv",
                        mode='a', header=False, index=False)
        final_df.to_csv(cloud_loc+"/Jira_Data.csv",
                        mode='a', header=False, index=False)
    else:
        final_df.to_csv(location+"/Data_Files/Jira_Data.csv",
                        mode='a', index=False)
        final_df.to_csv(cloud_loc+"/Jira_Data.csv",
                        mode='a', index=False)


if __name__ == '__main__':

    dt_extract = input("Process Desktime Data? (Y/N):")
    if dt_extract.lower() == "y":
        print(f'\n')
        print("*"*25)
        print("This Script takes the following arguments:")
        print(
            "n = [int]. By Default it is 1 and will always look at today -1 date.")
        print("merge_all = True/False. Whether to merge all the data and give one excel with all information. This is very storage heavy. ")
        print("strt_dt = YYYY-MM-DD. If we need to look back n days, then define start date and n.")
        print("For Example, if we need to look back 5 days from 24-Jan-2023, then n = 5 and start_dt = '2023-01-24'")
        print("Default is Today")
        past_dt = input("Enter Start Date: ")
        past_n = input("Enter No of Days to look back: ")

        if len(past_n):
            past_n = int(past_n)
        else:
            past_n = 1

        print(main(strt_dt=past_dt,
                   n=past_n))

    jira_extract = input("Extract Jira Data? (Y/N): ")
    if jira_extract.lower() == "y":
        dt_obj = input(
            "Date for which this file gets processed (YYYY-MM-DD) : ")
        process_jira(dt_obj)
