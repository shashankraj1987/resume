import json
import pandas as pd
import datetime


def load_json_file(file_name):
    '''
    The input file of this script is the output of a curl script that runs and extract the data from JIRA. 
    The curl script should run with ?expand=issues,authors,emails,worklogs in the url, else the script will fail. 
    Basic error checking is in place but there is still room for more. 
    '''
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


def transform_json_data(raw_json, df_act_emp):
    '''
    Takes 

    '''
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
    df_merged_act_emp["date_issue"] = str(datetime.today())
    df_merged_act_emp["count"] = 1

    return df_merged_act_emp
