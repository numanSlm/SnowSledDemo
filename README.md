SnowSled
================================

> The aim of the tool is to execute DDL/DML on Snowflake using Python.

Installation
------------

*   Install Python ([Python 3.6.5](https://www.python.org/downloads/release/python-365/))
*   Install Required Dependencies  
    (logging,sqlparse,pandas,requests,snowflake-python-connector)

Steps to use the utility
------------------------

1.  Place files to be executed into input folder (Preferred format: .sql)
2.  Store password on local machine and add it’s complete location in `password_location` option in **configExecution.properties**
3.  Enter your credentials and login parameters in **configConnection.properties**
4.  Before execution make sure **configExecution.properties** fields are set to either to TRUE or FALSE (Refer Config Fields)
5.  Add filenames to be executed in text file. eg: **executionFileNames.txt**  
    If the filenames are in different path enable `full_path_provided` option
6.  If `variable_substitution` is TRUE, Update **Mapping.csv** file with values
7.  Open Command prompt and navigate to extracted folder location
8.  To execute the utility once all steps are complete.  
    Type the below command on command prompt.
```
    python snowflakeExecutor.py -f executionFileNames.txt -c connectionName
```    

9.  You can also set `default_connection` & `default_file_list_name` in **configExecution.properties** and run the utility using the below command 
```
    python snowflakeExecutor.py
``` 

10.  Once the execution is complete find result & logs code in **output** folder


Notes
-----

*   This tool will **not work** for creation of Javascript/SQL based Procedures DDL.
*   DEBUG Logging available in **Output folder** in file **LoggerDEBUG.log**
*   To fill **executionFileNames.txt** you can navigate to input folder and open command prompt and type: `dir /b` and copy paste output in file.

Config Fields
-------------

*   `sso_login = FALSE`  
    (Will make a standard connection (Non SSO) mentioned in `snowflake_direct` in **configConnection.properties**, if True will make an SSO connection similar to `snowflake_sso`)
    
*   `continue_on_failure = TRUE`  
    (The code execution will continue execution even if a failure occured in any of the executed sql queries)
    
*   `variable_substitution = TRUE`  
    (If true, values will be fetched from **Mapping.csv**, Format for variable substitution)
    

    Example:
    Input query inside SQL file:        use database '&db_name';
    Values in Mapping.csv:               db_name,testing
    Output query which will execute:     use database testing;
    

*   `show_errors_on_console = True`  
    (While running will show FileName, Query, Query Id, Error on console)
    
*   `append_master_csv_log = True`  
    (Will append **MASTER\_LOG.csv** file with output, If set to FALSE will create a new csv file with filename being the current timestamp)
    
*   `full_path_provided = TRUE`  
    (For files in different paths. Users can add full path for each in input file list)
    
*   `split_on_parameter = TRUE`  
    (Will read each sql query assuming no additional `splitting_parameter` exists in query.  
    If set to False: Will read entire file instead of splitting on `splitting_parameter`)
    
*   `splitting_parameter = |||`  
    (Will split on given value `|||` if no value found will split on semicolon `;`)
    
*   `default_connection = snowflake_sso`  
    (Will be used when `-c` “Connection Paramter” not provided while executing Script. Change the above name to any default SSO Connection.  
    Make sure `sso_login` flag is on/off according to default connection)
    
*   `default_file_list_name = executionFileNames.txt`  
    (Will use fileName set in this parameter when `-f` parameter not passed while executing the script. Change the above name to any default input file name)
    
*   `password_location = home/EntirePath/password.properties`  
    (Set password file path in password\_location)
    

#### Sample Password File

> * * *
> 
> **password.properties**  
> \[password\]=  
> password\_sf=XXXXXXXXXX  
> password\_sso=XXXXXXXXXX  
> password\_sso\_poc=XXXXXXXXXX

