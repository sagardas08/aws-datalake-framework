import json
import time

import boto3


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


def create_state_machine(client, sfn_name, asl_definition, acc_number):
    response = client.create_state_machine(
        name=sfn_name,
        definition=json.dumps(asl_definition),
        roleArn=f"arn:aws:iam::{acc_number}:role/2181_SFN_ROLE",
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
    status = True
    region = config["primary_region"] if region is None else region
    prefix = config["fm_prefix"]
    acc_no = config["aws_account"]
    sfn_name = f"{prefix}-data-pipeline"
    arn = f"arn:aws:states:{region}:{acc_no}:stateMachine:{prefix}-data-pipeline"
    asl_definition = {
        "Comment": "A Hello World example of the Amazon States Language using an AWS Lambda function",
        "StartAt": "Data Quality Checks",
        "States": {
            "Data Quality Checks": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": "dl-fmwrk-data-quality-checks",
                    "Arguments": {
                        "--source_path.$": "$.source_path",
                        "--source_id.$": "$.source_id",
                        "--asset_id.$": "$.asset_id",
                        "--exec_id.$": "$.exec_id"
                    }
                },
                "Catch": [
                    {
                        "ErrorEquals": [
                            "States.TaskFailed"
                        ],
                        "ResultPath": "$.error-info.cause",
                        "Next": "dq_fallback"
                    }
                ],
                "Next": "Data Masking",
                "ResultPath": None
            },
            "dq_fallback": {
                "Type": "Pass",
                "Next": "trigger_alert"
            },
            "Data Masking": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": "dl-fmwrk-data-masking",
                    "Arguments": {
                        "--source_path.$": "$.source_path",
                        "--source_id.$": "$.source_id",
                        "--asset_id.$": "$.asset_id",
                        "--exec_id.$": "$.exec_id"
                    }
                },
                "Catch": [
                    {
                        "ErrorEquals": [
                            "States.TaskFailed"
                        ],
                        "ResultPath": "$.error-info.cause",
                        "Next": "dm_fallback"
                    }
                ],
                "Next": "Data Standardization",
                "ResultPath": None
            },
            "dm_fallback": {
                "Type": "Pass",
                "Next": "trigger_alert"
            },
            "Data Standardization": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": "dl-fmwrk-data-standardization",
                    "Arguments": {
                        "--source_path.$": "$.source_path",
                        "--source_id.$": "$.source_id",
                        "--asset_id.$": "$.asset_id",
                        "--exec_id.$": "$.exec_id"
                    }
                },
                "Catch": [
                    {
                        "ErrorEquals": [
                            "States.TaskFailed"
                        ],
                        "ResultPath": "$.error-info.cause",
                        "Next": "ds_fallback"
                    }
                ],
                "Next": "Success",
                "ResultPath": None
            },
            "ds_fallback": {
                "Type": "Pass",
                "Next": "trigger_alert"
            },
            "trigger_alert": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": "dl-fmwrk-trigger-alert",
                    "Payload": {
                        "error_info.$": "$.error-info.cause"
                    }
                },
                "End": True
            },
            "Success": {
                "Type": "Succeed"
            }
        }
    }
    client = boto3.client("stepfunctions", region_name=region)
    if sfn_exists(client, arn):
        update = str(input("Do you want to update the step function [Y/N]:  "))
        if update.lower() == "y":
            print("Step function is being updated ...")
            try:
                delete_state_machine(client, arn)
                create_state_machine(client, sfn_name, asl_definition, acc_no)
            except Exception as e:
                print(e)
                status = False
        else:
            print("Step function is not being updated ...")
    else:
        try:
            create_state_machine(client, sfn_name, asl_definition)
        except Exception as e:
            print(e)
            status = False
    return status
