"""

"""

import csv
import os
import configparser
import snowflake.connector
import sqlparse
from datetime import datetime
import requests
import json
import pandas
import logging


def getConnProp():
    global configConn
    configConn = configparser.RawConfigParser()
    configConn.read("./properties/configConnection.properties")
    return


def getPassProp():
    global configPass
    getExeProp()
    try:
        password_location = configExecute.get("conditions", "password_location")
        print("Password location = ", password_location)
        configPass = configparser.RawConfigParser()
        configPass.read(password_location)
    except:
        password_location = "Not found"
        print("Could not find password in location mentioned.")
        exit()
    return


# Add Email ID and Password below


def makeSSOConnection(connectionName):
    getConnProp()
    getPassProp()
    # Fetch  all values for sso snowflake connection
    username = configConn.get(connectionName, "username")
    password = configPass.get(connectionName, "password")
    warehouse = configConn.get(connectionName, "warehouse")
    database = configConn.get(connectionName, "database")
    schema = configConn.get(connectionName, "schema")
    role = configConn.get(connectionName, "role")
    account = configConn.get(connectionName, "account")
    authenticator = configConn.get(connectionName, "authenticator")
    client_id = configConn.get(connectionName, "client_id")
    client_secret = configConn.get(connectionName, "client_secret")
    grant_type = configConn.get(connectionName, "grant_type")
    scope = configConn.get(connectionName, "scope")
    url = configConn.get(connectionName, "url")

    # Parameters to pass to get access token
    data2 = {
        "client_id": client_id,
        "client_secret": client_secret,
        "username": username,
        "password": password,
        "grant_type": grant_type,
        "scope": scope,
    }

    # Get access-token from AWS privatelink (DO NOT CHANGE)
    headers = {"Content-type": "application/x-www-form-urlencoded;charset=UTF-8"}

    response = requests.post(url, headers=headers, data=data2)

    response_json = json.loads(response.text)
    try:
        access_token = response_json["access_token"]
        print("Access token generated")
    except:
        print("Access token not received")
        exit()

    # Change user,warehouse,role,database,schema as required below
    cursor_target = snowflake.connector.connect(
        user=username,
        account=account,
        authenticator=authenticator,
        warehouse=warehouse,
        role=role,
        database=database,
        schema=schema,
        token=access_token,
    )
    print("Logging in for User: ", username)
    return cursor_target


def makeSnowflakeConnection(connectionName):
    getConnProp()
    getPassProp()

    warehouse = configConn.get(connectionName, "warehouse")
    username = configConn.get(connectionName, "username")
    password = configConn.get(connectionName, "password")
    database = configConn.get(connectionName, "database")
    account = configConn.get(connectionName, "account")
    schema = configConn.get(connectionName, "schema")
    role = configConn.get(connectionName, "role")

    cursor_target = snowflake.connector.connect(
        user=username,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role,
    )
    print("Logging in for User: ", username)
    return cursor_target


def getExeProp():
    global configExecute
    configExecute = configparser.RawConfigParser()
    configExecute.read("./properties/configExecution.properties")
    return


def getExecuteConfig():
    getExeProp()
    print(
        "-----------------------------------------------------------------------------"
    )
    try:
        sso_login = configExecute.get("conditions", "sso_login")
        print("SSO_LOGIN = ", sso_login)
    except:
        sso_login = "FALSE"
        print(
            "Could not find SSO_LOGIN parameter in configExecute.properties, Using Non SSO Connection"
        )
    try:
        continue_on_failure = configExecute.get("conditions", "continue_on_failure")
        print("Continue on Failure = ", continue_on_failure)
    except:
        continue_on_failure = "FALSE"
        print(
            "Could not find continue_on_failure parameter, Using Default (FALSE) Value. "
        )
    try:
        variable_substitution = configExecute.get("conditions", "variable_substitution")
        print("Variable Substitution = ", variable_substitution)
    except:
        variable_substitution = "FALSE"
        print(
            "Could not find variable_substitution parameter, Using Default (FALSE) Value. "
        )
    try:
        show_errors_on_console = configExecute.get(
            "conditions", "show_errors_on_console"
        )
        print("Errors on Console = ", show_errors_on_console)
    except:
        show_errors_on_console = "FALSE"
        print(
            "Could not find show_errors_on_console parameter, Using Default (FALSE) Value. "
        )
    try:
        append_master_csv_log = configExecute.get("conditions", "append_master_csv_log")
        print("Append Output to MASTER LOG = ", append_master_csv_log)
    except:
        append_master_csv_log = "FALSE"
        print(
            "Could not find append_master_csv_log parameter, Using Default (FALSE) Value. "
        )
    #    try:
    #        default_input_file_source = configExecute.get('conditions', 'default_input_file_source')
    #        print("Default source = ",default_input_file_source)
    #    except:
    #        default_input_file_source = "TRUE"
    #        print("Could not find default_input_file_source parameter, Using Default (TRUE) Value. ")
    try:
        password_location = configExecute.get("conditions", "password_location")
        print("Password location = ", password_location)
    except:
        password_location = "Location not found"
        print("Could not find Password in password location parameter.")
        exit()
    try:
        split_on_parameter = configExecute.get("conditions", "split_on_parameter")
        print("split_on_parameter = ", split_on_parameter)
    except:
        split_on_parameter = "TRUE"
        print(
            "Could not find split_on_parameter parameter, Using Default (TRUE) Value."
        )
    try:
        full_path_provided = configExecute.get("conditions", "full_path_provided")
        print("Password location = ", full_path_provided)
    except:
        full_path_provided = "FALSE"
        print(
            "Could not find full_path_provided parameter, Using Default (TRUE) Value."
        )

    try:
        default_connection = "snowflake_direct"
        default_connection = configExecute.get("conditions", "default_connection")
        print("Default Connection Name = ", default_connection)
    except:
        default_connection = "snowflake_direct"
        # print("Could not find default_connection parameter, Using Default (default_connection) Value.")
    #
    try:
        default_file_list_name = configExecute.get(
            "conditions", "default_file_list_name"
        )
        print("Default File Name = ", default_file_list_name)
    except:
        default_file_list_name = "executionFileNames.txt"
        # print("Could not find default_file_list_name parameter, Using Default (executionFileNames.txt) Value.")

    try:
        splitting_parameter = configExecute.get("conditions", "splitting_parameter")
        print("Splitting parameter set to = ", splitting_parameter)
    except:
        splitting_parameter = ";"
        print("Could not find splitting_parameter parameter, Using Default (;) Value.")

    print(
        "-----------------------------------------------------------------------------"
    )
    return (
        sso_login,
        continue_on_failure,
        variable_substitution,
        show_errors_on_console,
        append_master_csv_log,
        full_path_provided,
        split_on_parameter,
        default_connection,
        default_file_list_name,
        splitting_parameter,
    )


def substitueVariable(sql):
    with open("./properties/Mapping.csv", mode="r", encoding="utf-8-sig") as infile:
        reader = csv.reader(infile)
        # Store values in mapping dictionary
        dictSimpleFunctions = {rows[0]: rows[1] for rows in reader}
    for input_value, convert_value in dictSimpleFunctions.items():
        replace_value = "'&" + input_value.strip() + "'"
        sql = sql.replace(replace_value, convert_value.strip())
    return sql
