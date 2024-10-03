import psycopg2
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

# Database connection (Removed Credentials)
old_jira_conn_params = {
    'dbname': '',
    'user': '',
    'password': '',
    'host': '',
    'port': ''
}

# Jira Cloud API 
jira_cloud_url = "https://here-technologies.atlassian.net"
api_user = "shivam.musterya@here.com"
api_token = ""

# JIRA Connection
old_jira_conn = psycopg2.connect(**old_jira_conn_params)

# Extract table data
def extract_table_data(table_name, conn):
    query = f"SELECT * FROM {table_name};"
    return pd.read_sql(query, conn)

# Create issue in Jira Cloud
def create_issue_in_jira_cloud(issue_data):
    url = f"{jira_cloud_url}/rest/api/3/issue"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    payload = {
        "fields": {
            "project": {
                "key": issue_data["project"]
            },
            "summary": issue_data["summary"],
            "description": issue_data["description"],
            "issuetype": {
                "name": issue_data["issue_type"]
            }
        }
    }
    response = requests.post(
        url,
        headers=headers,
        json=payload,
        auth=HTTPBasicAuth(api_user, api_token)
    )
    if response.status_code == 201:
        print(f"Issue created successfully: {response.json()['key']}")
    else:
        print(f"Failed to create issue: {response.status_code}, {response.text}")

#custom fields
def migrate_custom_fields_data():
    df = extract_table_data("custom_fields_data", old_jira_conn)
    for _, row in df.iterrows():
        issue_data = {
            "project": row["project"],
            "summary": row["custom_field_name"],
            "description": f"Custom Field Value: {row['custom_field_value']}",
            "issue_type": "Task"
        }
        create_issue_in_jira_cloud(issue_data)

#labels
def migrate_labels_data():
    df = extract_table_data("labels_data", old_jira_conn)
    for _, row in df.iterrows():
        issue_data = {
            "project": row["project"],
            "summary": "Label: " + row["labels"],
            "description": f"Label for key: {row['key']}",
            "issue_type": "Task"
        }
        create_issue_in_jira_cloud(issue_data)

#linked issues
def migrate_linked_issues():
    df = extract_table_data("linked_issues", old_jira_conn)
    for _, row in df.iterrows():
        issue_data = {
            "project": row["project"],
            "summary": "Linked Issue: " + row["linked_issue_summary"],
            "description": f"Link type: {row['link_type']} | Linked issue: {row['linked_issue_key']}",
            "issue_type": "Task"
        }
        create_issue_in_jira_cloud(issue_data)

#main data
def migrate_main_data():
    df = extract_table_data("main_data", old_jira_conn)
    for _, row in df.iterrows():
        issue_data = {
            "project": row["project"],
            "summary": row["summary"],
            "description": row["description"],
            "issue_type": "Task"
        }
        create_issue_in_jira_cloud(issue_data)

#transitions data
def migrate_transitions_data():
    df = extract_table_data("transitions_data", old_jira_conn)
    for _, row in df.iterrows():
        issue_data = {
            "project": row["project"],
            "summary": f"Transitioned Field: {row['field']}",
            "description": f"From: {row['from_value']} | To: {row['to_value']} | By: {row['author']}",
            "issue_type": "Task"
        }
        create_issue_in_jira_cloud(issue_data)

#Execution
migrate_custom_fields_data()
migrate_labels_data()
migrate_linked_issues()
migrate_main_data()
migrate_transitions_data()


old_jira_conn.close()
