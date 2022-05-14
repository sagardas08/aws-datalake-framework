"""
== Create a new source system
1. Insert a new record in the dynamoDb table
2. Create an S3 bucket associated to the new source system
3. Create a new IAM user (or othe mechanism) to access the S3 bucket
4. Create a bucket policy accordingly and associate with the S3 bucket
5. Create an SNS topic associated to the S3 bucket
6. Subscribe the SNS topic to the DL source system lambda function
"""

import boto3
import json
import decimal
import time
import sys
import os
from random import random
from datetime import datetime

from connector import Connector
from utils.comUtils import getGlobalParams

tgt_sys_id = int(str(random()).split(".")[1])
global_config = getGlobalParams()


def insert_tgt_sys_info(db, tgt_json_file, region):
    # TODO: DynamoDB -> RDS: Insert Data
    with open(tgt_json_file) as json_file:
        tgt_config = json.load(json_file)

    bucket_name = (
        global_config["fm_tgt_prefix"]
        + "-"
        + tgt_config["domain"]
        + "-"
        + region
    )
    data_owner = tgt_config["data_owner"]
    support_cntct = tgt_config["support_cntct"]
    domain = tgt_config["domain"]
    subdomain = tgt_config["subdomain"]
    table = 'target_system'
    print(f"Insert source system info in {global_config['fm_prefix']}.{table} table")
    current_time = datetime.now()
    data = {
            "target_id": tgt_sys_id,
            "bucket_nm": bucket_name,
            "data_owner": data_owner,
            "support_cntct": support_cntct,
            "domain": domain,
            "subdomain": subdomain,
            "modified_ts": current_time
        }
    db.insert(table, data)


def create_tgt_sys_db(tgt_json_file, region):
    ath = boto3.client("athena", region_name=region)
    with open(tgt_json_file) as json_file:
        tgt_config = json.load(json_file)
    db_name = tgt_config["domain"]
    query = f"create database {db_name}"
    exists = False
    response = ath.list_databases(
        CatalogName="AwsDataCatalog",
    )
    for i in response["DatabaseList"]:
        if i["Name"] == db_name:
            exists = True
            print(f"Database {db_name} already exists")
    if not exists:
        ath.start_query_execution(
            QueryString=query,
            WorkGroup="dl-fmwrk",
        )


def main():
    db = Connector(global_config['db_secret'], global_config['db_region'])
    try:
        insert_tgt_sys_info(
            db, sys.argv[1], global_config["primary_region"]
        )
        insert_tgt_sys_info(
            db, sys.argv[1], global_config["secondary_region"]
        )
    except Exception as e:
        print(e)
        db.rollback()
    finally:
        db.close()
    # create_tgt_sys_db(sys.argv[1], global_config["primary_region"])
    # create_tgt_sys_db(sys.argv[1], global_config["secondary_region"])


if __name__ == "__main__":
    main()
