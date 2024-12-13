import requests
from requests.auth import HTTPBasicAuth
import pandas as pd


def minutes(x) :
    x = str(x)
    if ';' in x :
     return (x.split(';')[-1])
    else :
        x 
def format_date(column,df) :
    df[column+"_date"] = pd.to_datetime(df[column], errors='coerce', dayfirst=True)

def extract_month(x) :
    try:
        return re.findall(r'\d{2}/\d{2}/\d{4}',x)[0].split('/')[1]
    except:
        return None

def extract_year(x) :
    try:
        return re.findall(r'\d{2}/\d{2}/\d{4}',x)[0].split('/')[2]
    except:
        return None

import requests
from requests.auth import HTTPBasicAuth
import pandas as pd

def jira_connect():
    # JIRA API Details
    jira_url = "https://ambience.atlassian.net/rest/api/2/search"
    email = "theodoros.pallis@sioufaslaw.gr"
    api_token = "ATATT3xFfGF0eWUL0LcHoUi20MB-dYjPKBjhG2M1P8m7SbnziKyIUIpS7_cC8Z-i6iwyDj9GPmLHnWMOV-2sxT5tPz45mjyic6zrWXG2NsleWrKg2oCcduDe59pm-EIJy7kuvmdYSofms2fipy2rBPO6nz3x5_L4rpq8mrcFWhalXS-Cf3vbCKs=A83FB7C4"

    # Initialize pagination parameters
    start_at = 0
    max_results = 50
    all_issues = []

    while True:
        # JQL Query
        jql_query = {
            "jql": "project = DNY",
            "startAt": start_at,
            "maxResults": max_results,
            # "fields": "*all" 
            "fields": ["id", "key", "summary", "status", "reporter", "assignee", "created","updated","timeoriginalestimate"],
        }

        # Make API Request
        response = requests.get(
            jira_url, auth=HTTPBasicAuth(email, api_token), params=jql_query
        )

        # Check Response Status
        if response.status_code != 200:
            print(f"Error: {response.status_code}, {response.text}")
            break

        # Parse Response
        issues = response.json().get("issues", [])
        all_issues.extend(issues)

        # Stop if no more issues
        if len(issues) < max_results:
            break

        # Increment startAt for the next page
        start_at += max_results

    # Initialize an empty list to store issue details
    data = []

    # Iterate through all issues and extract required fields
# Iterate through all issues and extract required fields
    log_cols = [f'Log Work.{i}' if i > 0 else 'Log Work' for i in range(99)]  # Define log_cols

    for issue in all_issues:
        fields = issue.get("fields", {})  # Safely get 'fields'
        issue_data = {
            "Issue Key": issue["key"],
            "Summary": fields.get("summary", "No Summary"),
            "Reporter": fields.get("reporter", {}).get("displayName", "No Reporter"),
            "Assignee": (fields.get("assignee") or {}).get("displayName", "No Assignee"),  # Safely handle None
            "Status": fields.get("status", {}).get("name", "No Status"),
            "Created": fields.get("created", "No Created Date"),
            "Updated": fields.get("updated", "No Updated Date"),
            "Resolved": fields.get("resolutiondate", "Not Resolved"),
            'Label': fields.get("label", "No Label"),
            'Estimate': fields.get("timeoriginalestimate", "No Estimate"),
        }

        # Populate log_cols with specific data
        for i, col_name in enumerate(log_cols):
            # Replace the logic in fields.get(f"custom_log_field_{i}", "No Data") with appropriate data source
            issue_data[col_name] = fields.get(f"custom_log_field_{i}", "No Data")

        data.append(issue_data)

    # Create a DataFrame
    df = pd.DataFrame(data)

    return df
