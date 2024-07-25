"""

"""

from snowflake.connector.secret_detector import SecretDetector
from configFunctionsPython import *
import sys
import os.path
from os import path
import time

# Get execution configurations parameters
(
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
) = getExecuteConfig()
inputFileList = ""


fFlagUnset = False
for i in range(0, len(sys.argv)):
    if sys.argv[i] == "-f":
        inputFileList = sys.argv[i + 1]
        fFlagUnset = True
        break
if fFlagUnset == False:
    inputFileList = default_file_list_name

cFlagUnset = False
for i in range(0, len(sys.argv)):
    if sys.argv[i] == "-c":
        connectionName = sys.argv[i + 1]
        cFlagUnset = True
        break

if cFlagUnset == False:
    connectionName = default_connection

if len(inputFileList) > 0:
    print(
        "-----------------------------------------------------------------------------"
    )
    print("Reading files from", inputFileList)


# Get execution configurations parameters
# sso_login, continue_on_failure, variable_substitution, show_errors_on_console, append_master_csv_log = checkConfigCondition = getExecuteConfig()

# Check connection type
if sso_login.strip().upper() == "TRUE":
    # Create a connection
    cursor_target = makeSSOConnection(connectionName)
    print("Snowflake SSO Connection Established :", connectionName)
else:
    # Create a connection
    cursor_target = makeSnowflakeConnection(connectionName)
    print("Snowflake Connection Established (Non-SSO) :", connectionName)

# Initiate cursor
cs = cursor_target.cursor()

# Initiate Logger
for logger_name in ["snowflake.connector", "botocore", "boto3"]:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    # Change Logger File if needed
    ch = logging.FileHandler("./Output/LoggerDEBUG.log")
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(
        SecretDetector(
            "%(asctime)s - %(threadName)s %(filename)s:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s"
        )
    )
    logger.addHandler(ch)

# Check Output Logging File Name
mode = "w"
if append_master_csv_log.strip().upper() == "TRUE":
    OutputLog = "MASTER_LOG.csv"
    mode = "a"
else:
    value = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    OutputLog = str(value) + "_log.csv"
    mode = "w"

# Read all execution file names
with open("./" + inputFileList, mode="r", encoding="utf-8-sig") as infile:
    reader = infile.read()
infile.close()

# List of all Files
executionFileNames = reader.split("\n")
executedTotal = 0
executionFailure = 0

dontAddHeader = False
try:
    with open("./output/" + OutputLog, mode="r", encoding="utf-8") as fd2:
        readFile = fd2.read()
        if len(readFile.strip()) > 0:
            dontAddHeader = True
except:
    dontAddHeader = False


# Write into Output file
with open("./output/" + OutputLog, mode=mode, encoding="utf-8", newline="") as fd:
    writer = csv.writer(fd, delimiter=",")
    if dontAddHeader == False:
        writer.writerow(
            [
                "File,Name",
                "ObjectName",
                "Execution",
                "Timestamp",
                "ErrorLog",
                "Failed_SFQID",
                "ExecutionTime(mins)",
            ]
        )

    # Iteragte through each file
    for fileName in executionFileNames:
        skipFile = False
        errorsInFile = 0
        if fileName.startswith("#"):
            print("Skipped ", fileName)
            skipFile = True
        else:
            # add things here
            if full_path_provided.strip().upper() == "FALSE":
                input_path = "./input/" + fileName
            else:
                input_path = fileName

            if path.exists(input_path):
                with open(input_path, mode="r", encoding="utf-8") as readfile:
                    # Read Data inside file
                    fileContent = readfile.read()

                # Split on each semicolon
                if split_on_parameter.upper().strip() == "TRUE":
                    sqlQueries = fileContent.split(splitting_parameter)
                else:
                    sqlQueries = []
                    sqlQueries.append(fileContent)
                print("-------------------------------------------")
                print("Executing ", fileName)

                # Used to check if error is generated
                error_flag = False

                # Iterate through each query
                for sql in sqlQueries:
                    sql = sql.strip()
                    codeTerminated = False
                    # tempTime used in Logs for estimated query execution time
                    tempTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    # Remove comments
                    sql = sqlparse.format(sql, strip_comments=True).strip()

                    # Check for empty string
                    if len(sql) > 0:

                        # Increment Total SQL Executed Queries Count
                        executedTotal = executedTotal + 1

                        # Check if variables need to be substituted
                        if variable_substitution.strip().upper() == "TRUE":
                            sql = substitueVariable(sql)

                        # Execute the sql query

                        try:
                            startTime = time.time()
                            output = cs.execute(sql)
                            endTime = time.time()
                            totalExecutionTime = (endTime - startTime) / 60
                            print("Elapsed time(in min):", totalExecutionTime)
                            # Write into file
                            writer.writerow(
                                [
                                    fileName,
                                    sql,
                                    "SUCCESS",
                                    str(tempTime),
                                    "",
                                    "",
                                    totalExecutionTime,
                                ]
                            )

                        except snowflake.connector.errors.ProgrammingError as e:
                            # Capture the error (Remove extra tabs and newline from error msg)
                            error = e.msg.replace("\n", " ")
                            error = error.replace("\t", " ")

                            # Increment failure count
                            executionFailure = executionFailure + 1
                            errorsInFile = errorsInFile + 1
                            # Write into file
                            writer.writerow(
                                [
                                    fileName,
                                    sql,
                                    "FAIL",
                                    str(tempTime),
                                    str(error),
                                    str(e.sfqid),
                                ]
                            )
                            error_flag = True

                            # Display errors if show_errors_on_console is true
                            if show_errors_on_console.strip().upper() == "TRUE":
                                print("----------------------------------")
                                print("ERROR IN FILE: ", fileName)
                                print("----------------------------------")
                                print("SQL QUERY: ", sql)
                                print("----------------------------------")
                                print("QUERY ID: ", e.sfqid)
                                print("----------------------------------")
                                print("ERROR MESSAGE:", error)
                                print("----------------------------------")

                    # Check condition to break execution based on config property
                    if (
                        continue_on_failure.strip().upper() == "FALSE"
                        and error_flag == True
                    ):

                        # Unusual termination of code
                        codeTerminated = True
                        break
                if (
                    continue_on_failure.strip().upper() == "FALSE"
                    and error_flag == True
                ):
                    # Unusual termination of code
                    codeTerminated = True
                    break

                if errorsInFile == 0 and skipFile == False:
                    print(fileName, "Executed Succesfully.")
                if codeTerminated == False and errorsInFile > 0 and skipFile == False:
                    print(
                        "For File:",
                        fileName,
                        "A total of",
                        errorsInFile,
                        "queries did not run successfully. ",
                    )
                print("-------------------------------------------")
            else:
                # Can add counters here if required
                print(input_path, " PATH not found")
    if codeTerminated == True:
        print(fileName, "Execution Terminated. (Please check logs)")
    print(
        "-----------------------------------------------------------------------------"
    )

print("\tFind Logs for this execution at: ", OutputLog.strip())
print("-----------------------------------------------------------------------------")
print("\t\t\tExecution Stats: ", executedTotal - executionFailure, "/", executedTotal)
print("\t\t\tFailed Queries: ", executionFailure)
print("-----------------------------------------------------------------------------")
print("-------- Thank you for using Snowflake Python Executor --------")
print("-----------------------------------------------------------------------------")

# Close cursor
cs.close()

# Close File
fd.close()
