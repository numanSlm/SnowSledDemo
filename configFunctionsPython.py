import csv
import configparser
import snowflake.connector
import requests
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)


def get_conn_prop():
    config_conn = configparser.RawConfigParser()
    config_conn.read("./properties/configConnection.properties")
    return config_conn


def get_pass_prop(config_execute):
    try:
        password_location = config_execute.get("conditions", "password_location")
        logging.info("Password location = %s", password_location)
        config_pass = configparser.RawConfigParser()
        config_pass.read(password_location)
        return config_pass
    except configparser.NoOptionError:
        logging.error("Could not find password in location mentioned.")
        exit()


def make_sso_connection(connection_name):
    config_conn = get_conn_prop()
    config_execute = get_exe_prop()
    config_pass = get_pass_prop(config_execute)

    # Fetch all values for SSO Snowflake connection
    username = config_conn.get(connection_name, "username")
    password = config_pass.get(connection_name, "password")
    warehouse = config_conn.get(connection_name, "warehouse")
    database = config_conn.get(connection_name, "database")
    schema = config_conn.get(connection_name, "schema")
    role = config_conn.get(connection_name, "role")
    account = config_conn.get(connection_name, "account")
    authenticator = config_conn.get(connection_name, "authenticator")
    client_id = config_conn.get(connection_name, "client_id")
    client_secret = config_conn.get(connection_name, "client_secret")
    grant_type = config_conn.get(connection_name, "grant_type")
    scope = config_conn.get(connection_name, "scope")
    url = config_conn.get(connection_name, "url")

    # Parameters to pass to get access token
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "username": username,
        "password": password,
        "grant_type": grant_type,
        "scope": scope,
    }

    # Get access-token from AWS privatelink (DO NOT CHANGE)
    headers = {"Content-type": "application/x-www-form-urlencoded;charset=UTF-8"}

    response = requests.post(url, headers=headers, data=data)
    response_json = response.json()

    try:
        access_token = response_json["access_token"]
        logging.info("Access token generated")
    except KeyError:
        logging.error("Access token not received")
        exit()

    # Change user, warehouse, role, database, schema as required below
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
    logging.info("Logging in for User: %s", username)
    return cursor_target


def make_snowflake_connection(connection_name):
    config_conn = get_conn_prop()
    config_execute = get_exe_prop()
    config_pass = get_pass_prop(config_execute)

    warehouse = config_conn.get(connection_name, "warehouse")
    username = config_conn.get(connection_name, "username")
    password = config_pass.get(connection_name, "password")
    database = config_conn.get(connection_name, "database")
    account = config_conn.get(connection_name, "account")
    schema = config_conn.get(connection_name, "schema")
    role = config_conn.get(connection_name, "role")

    cursor_target = snowflake.connector.connect(
        user=username,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role,
    )
    logging.info("Logging in for User: %s", username)
    return cursor_target


def get_exe_prop():
    config_execute = configparser.RawConfigParser()
    config_execute.read("./properties/configExecution.properties")
    return config_execute


def get_execute_config():
    config_execute = get_exe_prop()
    logging.info("-----------------------------------------------------------------------------")

    try:
        sso_login = config_execute.get("conditions", "sso_login")
        logging.info("SSO_LOGIN = %s", sso_login)
    except configparser.NoOptionError:
        sso_login = "FALSE"
        logging.warning("Could not find SSO_LOGIN parameter, Using Non SSO Connection")

    try:
        continue_on_failure = config_execute.get("conditions", "continue_on_failure")
        logging.info("Continue on Failure = %s", continue_on_failure)
    except configparser.NoOptionError:
        continue_on_failure = "FALSE"
        logging.warning("Could not find continue_on_failure parameter, Using Default (FALSE) Value.")

    try:
        variable_substitution = config_execute.get("conditions", "variable_substitution")
        logging.info("Variable Substitution = %s", variable_substitution)
    except configparser.NoOptionError:
        variable_substitution = "FALSE"
        logging.warning("Could not find variable_substitution parameter, Using Default (FALSE) Value.")

    try:
        show_errors_on_console = config_execute.get("conditions", "show_errors_on_console")
        logging.info("Errors on Console = %s", show_errors_on_console)
    except configparser.NoOptionError:
        show_errors_on_console = "FALSE"
        logging.warning("Could not find show_errors_on_console parameter, Using Default (FALSE) Value.")

    try:
        append_master_csv_log = config_execute.get("conditions", "append_master_csv_log")
        logging.info("Append Output to MASTER LOG = %s", append_master_csv_log)
    except configparser.NoOptionError:
        append_master_csv_log = "FALSE"
        logging.warning("Could not find append_master_csv_log parameter, Using Default (FALSE) Value.")

    try:
        password_location = config_execute.get("conditions", "password_location")
        logging.info("Password location = %s", password_location)
    except configparser.NoOptionError:
        password_location = "Location not found"
        logging.error("Could not find Password in password location parameter.")
        exit()

    try:
        split_on_parameter = config_execute.get("conditions", "split_on_parameter")
        logging.info("split_on_parameter = %s", split_on_parameter)
    except configparser.NoOptionError:
        split_on_parameter = "TRUE"
        logging.warning("Could not find split_on_parameter parameter, Using Default (TRUE) Value.")

    try:
        full_path_provided = config_execute.get("conditions", "full_path_provided")
        logging.info("Password location = %s", full_path_provided)
    except configparser.NoOptionError:
        full_path_provided = "FALSE"
        logging.warning("Could not find full_path_provided parameter, Using Default (TRUE) Value.")

    try:
        default_connection = config_execute.get("conditions", "default_connection")
        logging.info("Default Connection Name = %s", default_connection)
    except configparser.NoOptionError:
        default_connection = "snowflake_direct"

    try:
        default_file_list_name = config_execute.get("conditions", "default_file_list_name")
        logging.info("Default File Name = %s", default_file_list_name)
    except configparser.NoOptionError:
        default_file_list_name = "executionFileNames.txt"

    try:
        splitting_parameter = config_execute.get("conditions", "splitting_parameter")
        logging.info("Splitting parameter set to = %s", splitting_parameter)
    except configparser.NoOptionError:
        splitting_parameter = ";"
        logging.warning("Could not find splitting_parameter parameter, Using Default (;) Value.")

    logging.info("-----------------------------------------------------------------------------")
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


def substitute_variable(sql):
    with open("./properties/Mapping.csv", mode="r", encoding="utf-8-sig") as infile:
        reader = csv.reader(infile)
        # Store values in mapping dictionary
        dict_simple_functions = {rows[0]: rows[1] for rows in reader}
    for input_value, convert_value in dict_simple_functions.items():
        replace_value = "'&" + input_value.strip() + "'"
        sql = sql.replace(replace_value, convert_value.strip())
    return sql
