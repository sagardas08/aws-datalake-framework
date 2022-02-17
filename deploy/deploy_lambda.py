import boto3


def lambda_function_exists(lambda_client, func_name):
    """
    Check if a lambda function exists, return True if it does, return False otherwise
    A bit slower method - O(N^2)
    :param lambda_client:
    :param func_name:
    :return:
    """
    paginator = lambda_client.get_paginator("list_functions")
    response_iterator = paginator.paginate()
    for response in response_iterator:
        functions = response["Functions"]
        for function in functions:
            function_name = function["FunctionName"]
            if str(function_name) == func_name:
                return True
    return False


def create_lambda(config, region=None):
    """
    Creates the lambda function based on the config values
    :param config:
    :param region:
    :return:
    """
    fm_prefix = config["fm_prefix"]
    region = config["primary_region"] if region is None else region
    project_name = config["project_name"]
    func_name = config["lambda_function_name"]
    lambda_bucket = f"{fm_prefix}-code-{region}"
    lambda_key = f"{project_name}/lambda/lambda_function.zip"
    lambda_client = boto3.client("lambda", region_name=region)
    # TODO : Discuss if an end user wants to update the lambda function
    if lambda_function_exists(lambda_client, func_name):
        # update = str(input("A lambda function by the name {}Do you want to update the lambda function: Y/N  "))
        lambda_client.delete_function(FunctionName=func_name)
    try:
        lambda_client.create_function(
            Code={
                "S3Bucket": lambda_bucket,
                "S3Key": lambda_key,
            },
            PackageType="Zip",
            Description="Testing Lambda deployment",
            FunctionName=func_name,
            Handler="lambda_function.lambda_handler",
            Role="arn:aws:iam::076931226898:role/service-role/dlFmwrkSrcDataInfo_role",
            Runtime="python3.9",
            Environment={
                "Variables": {
                    "aws_account": config["aws_account"],
                    "fm_prefix": config["fm_prefix"],
                }
            },
        )
        return True
    except Exception as e:
        print(e)
        return False
