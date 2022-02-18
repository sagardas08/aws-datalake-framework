import boto3
import json
import time


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
    asl_definition = {
        "Comment": "A description of my state machine",
        "StartAt": "Data Quality Checks",
        "States": {
            "Data Quality Checks": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun",
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
                "Resource": "arn:aws:states:::glue:startJobRun",
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
                "Resource": "arn:aws:states:::glue:startJobRun",
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
    try:
        response = client.delete_state_machine(
            stateMachineArn=f"arn:aws:states:{region}:{acc_no}:stateMachine:{prefix}-data-pipeline"
        )
        is_deleted = False
        while is_deleted == False:
            response = client.describe_state_machine(
                stateMachineArn=f"arn:aws:states:{region}:{acc_no}:stateMachine:{prefix}-data-pipeline"
            )
            if response["status"] == "ACTIVE" or response["status"] == "DELETING":
                time.sleep(5)
            else:
                is_deleted = True
    except:
        pass
    response = client.create_state_machine(
        name=f"{prefix}-data-pipeline",
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
