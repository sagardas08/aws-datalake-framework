import json
import boto3
import os
from connector.pg_connect import Connector
from datetime import datetime as date

now = date.now()
current_time = now.strftime("%H:%M:%S")
print("Loading function")


def lambda_handler(event, context):
    message = json.loads(event["Records"][0]["Sns"]["Message"])
    bucket = message["Records"][0]["s3"]["bucket"]["name"]
    key = message["Records"][0]["s3"]["object"]["key"]
    # the env vars are present in the LAMBDA function
    # and can be updated using the console
    region = os.environ["AWS_REGION"]
    fm_prefix = os.environ["fm_prefix"]
    aws_account = os.environ["aws_account"]
    key_path = key[: key.rfind("/")] + "/"
    source_path = "s3://" + bucket + "/" + key_path
    source_id = bucket.split("-")[2]
    asset_id = key.split("/")[0]
    db_secret = os.environ["db_secret"]
    db_region = os.environ["db_region"]
    table_name = "data_asset_catalogs"
    conn = Connector(db_secret, db_region)
    print(
        "Source Path: {}, Source ID: {}, Asset ID: {}".format(
            source_path, source_id, asset_id
        )
    )

    # Create timestamp to act as identifier for every execution
    ts = date.now().strftime("%Y%m%d%H%M%S")

    # generate a execution id for audit purpose
    exec_id = source_id + "_" + asset_id + "_" + str(ts)
    state_machine_name = fm_prefix + "-data-pipeline" + str(ts)

    # Create timestamp to insert in the data catalog to help tract the start time of process
    proc_start_ts = date.now().strftime("%H:%M:%S")

    # initial record is inserted in the data catalog table
    insert_data = {
        "exec_id": exec_id,
        "src_sys_id": source_id,
        "asset_id": asset_id,
        "dq_validation": "not started",
        "data_standardization": "not started",
        "data_masking": "not started",
        "src_file_path": source_path,
        "proc_start_ts": proc_start_ts,
    }
    # insert initial record
    conn.insert(table=table_name, data=insert_data)
    conn.commit()
    conn.close()

    # Step function execution is triggered
    client = boto3.client("stepfunctions")
    response = client.start_execution(
        stateMachineArn="arn:aws:states:{}:{}:stateMachine:{}-data-pipeline".format(
            region, aws_account, fm_prefix
        ),
        name=state_machine_name,
        input=json.dumps(
            {
                "source_path": source_path,
                "source_id": source_id,
                "asset_id": asset_id,
                "exec_id": exec_id,
            }
        ),
    )
    print(response)
