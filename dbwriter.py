import json
import pymysql
from botocore.exceptions import ConnectTimeoutError, ClientError
from botocore.config import Config as BotoConfig

# secret_name = os.environ.get("RDS_CONNECTION_SECRET")
runs_table = "runs_stg"
lifts_table = "lifts_stg"
ski_db = "cwp"

def main(event):
    try:
        print(f"received event: {event}")
        obj = json.loads(event)
        connection = build_connection_obj(ski_db)
        
        lifts = obj.get("lifts", [])
        runs = obj.get("runs", [])
        location = obj.get("location", "No_Location")
        date = obj.get("updatedDate", "1990-01-01")


        # handle lifts
        resp = update_lifts(connection, lifts, location, date)
        print(resp)

        # handle runs
        resp = update_runs(connection, runs, location, date)
        print(resp)

        return "Success"


    except Exception as e:
        print("generic exception received, returning 202")
        print(e)

def update_lifts(connection, lifts, location, date):
    print("Updating lifts table")
    sql = f"""
        INSERT IGNORE INTO {ski_db}.{lifts_table} (hash, location, lift_name, lift_type, lift_status, updated_date)
        VALUES """
    vals = ""
    for lift in lifts:
        txt = '("%s", "%s", "%s", "%s", %s, "%s"),' % (
            create_hash(date, location, lift['liftName']), #PKEY
            location, 
            lift.get("liftName", "N/A"), 
            lift.get("liftType", "N/A"),
            lift.get("liftStatus", False), 
            date)
        vals += txt
    vals = vals[:-1]+";"
    sql += vals
    resp = execute_sql(connection, sql)
    return resp

def update_runs(connection, runs, location, date):
    print("Updating runs table")
    run_cols = ["hash", "location", "run_name", "run_difficulty", "run_status", "run_area", "run_groomed", "updated_date"]
    cols_list = ",".join(col for col in run_cols)
    sql = f"""
        INSERT INTO {ski_db}.{runs_table} ({cols_list})
        VALUES """
    vals = ""
    for run in runs:
        txt = '("%s", "%s", "%s", "%s", %s, "%s", "%s", "%s"),' % (
            create_hash(date, location, run['runName']), #PKEY
            location, 
            run.get("runName", "N/A"), 
            run.get("runDifficulty", "N/A"),
            run.get("runStatus", False), 
            run.get("runArea", "N/A"),
            run.get("runGroomed", "N/A"),
            date)
        vals += txt
    vals = vals[:-1]+"\nON DUPLICATE KEY UPDATE"
    for col in run_cols:
        vals += f"\n{col} = VALUES({col}),"
    vals = vals[:-1]+";"

    sql += vals
    resp = execute_sql(connection, sql)
    return resp
    
def execute_sql(connection, sql_statement):
    print("EXECUTING SQL")
    cursor = connection.cursor()
    resp = cursor.execute(sql_statement)
    print(f"{resp} rows affected")
    results = cursor.fetchall()
    connection.commit()
    return results

def build_connection_obj(db):
    host_name = "10.0.0.83"
    user_name = "cwpace97"
    pwd = "tmp"
    connection_obj = pymysql.connect(
        host = host_name,
        user = user_name,
        password = pwd,
        db = db,
        port = 3306,
        charset = "utf8mb4",
        cursorclass=pymysql.cursors.DictCursor
    )
    print("Connected")
    return connection_obj

import hashlib

def create_hash(*strings, hash_algorithm='sha256'):
    """
    Creates a hash from multiple input strings using the specified hash algorithm.

    Args:
        *strings: Strings to be hashed (multiple arguments).
        hash_algorithm (str): Hashing algorithm to use (default: 'sha256').

    Returns:
        str: The resulting hash in hexadecimal format.
    """
    # Initialize the hash object with the specified algorithm
    hash_object = hashlib.new(hash_algorithm)
    
    # Combine and encode each string into the hash object
    for string in strings:
        hash_object.update(string.encode('utf-8'))
    
    # Return the hexadecimal representation of the hash
    return hash_object.hexdigest()