import boto3
import json
import time


def sfn_exists(client, arn):
    response = client.list_state_machines()
    for i in response["stateMachines"]:
        if i["stateMachineArn"] == arn:
            return True
    return False


def delete_state_machine(client, arn):
    response = client.delete_state_machine(stateMachineArn=arn)
    while sfn_exists(client, arn):
        time.sleep(5)


def create_state_machine(client, sfn_name, asl_definition):
    response = client.create_state_machine(
        name=sfn_name,
        definition=json.dumps(asl_definition),
        roleArn="arn:aws:iam::076931226898:role/service-role/StepFunctions-dl-fmwrk-data-pipeline-role-5a555edb",
        type="STANDARD",
        loggingConfiguration={
            "level": "OFF",
            "includeExecutionData": False,
        },
        tags=[
            {"key": "project", "value": "aws-datalake-framework"},
        ],
        tracingConfiguration={"enabled": False},
    )


def create_step_function(config, region=None):
    """
    Creates step function
    :param config:
    :param region:
    :return:
    """
    region = config["primary_region"] if region is None else region
    prefix = config["fm_prefix"]
    acc_no = config["aws_account"]
    sfn_name = f"{prefix}-data-pipeline"
    arn = f"arn:aws:states:{region}:{acc_no}:stateMachine:{prefix}-data-pipeline"
    asl_definition = {
        "Comment": "A description of my state machine",
        "StartAt": "Data Quality Checks",
        "States": {
            "Data Quality Checks": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": f"{prefix}-data-quality-checks",
                    "Arguments": {
                        "--source_path.$": "$.source_path",
                        "--source_id.$": "$.source_id",
                        "--asset_id.$": "$.asset_id",
                        "--exec_id.$": "$.exec_id",
                    },
                },
                "Next": "Data Masking",
                "ResultPath": None,
            },
            "Data Masking": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": f"{prefix}-data-masking",
                    "Arguments": {
                        "--source_path.$": "$.source_path",
                        "--source_id.$": "$.source_id",
                        "--asset_id.$": "$.asset_id",
                        "--exec_id.$": "$.exec_id",
                    },
                },
                "Next": "Data Standardization",
                "ResultPath": None,
            },
            "Data Standardization": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": f"{prefix}-data-standardization",
                    "Arguments": {
                        "--source_path.$": "$.source_path",
                        "--source_id.$": "$.source_id",
                        "--asset_id.$": "$.asset_id",
                        "--exec_id.$": "$.exec_id",
                    },
                },
                "End": True,
                "ResultPath": None,
            },
        },
    }
    client = boto3.client("stepfunctions", region_name=region)
    if sfn_exists(client, arn):
        update = str(input("Do you want to update the step function: Y/N  "))
        if update == "Y":
            delete_state_machine(client, arn)
            create_state_machine(client, sfn_name, asl_definition)
    else:
        create_state_machine(client, sfn_name, asl_definition)
