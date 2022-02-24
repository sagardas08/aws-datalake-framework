import boto3


def create_dq_job(config, region=None):
    """
    Creates DQ job on glue
    :param config:
    :param region:
    :return:
    """
    region = config["primary_region"] if region is None else region
    fm_prefix = config["fm_prefix"]
    client = boto3.client("glue", region_name=region)
    job_name = f"{fm_prefix}-data-quality-checks"
    client.delete_job(JobName=job_name)
    script_location = (
        f"s3://{fm_prefix}-code-{region}/aws-datalake-framework/src/genericDqChecks.py"
    )
    default_args = {
        "--extra-py-files": f"s3://{fm_prefix}-code-{region}/aws-datalake-framework/dependencies/pydeequ.zip,s3://{fm_prefix}-code-{region}/aws-datalake-framework/dependencies/utils.zip",
        "--extra-files": f"s3://{fm_prefix}-code-{region}/aws-datalake-framework/config/globalConfig.json",
        "--extra-jars": f"s3://{fm_prefix}-code-{region}/aws-datalake-framework/dependencies/deequ-1.0.3.jar",
        "--TempDir": f"s3://{fm_prefix}-code-{region}/temporary/",
    }
    response = client.create_job(
        Name=job_name,
        Description="Data Quality Job",
        Role="2482-misc-service-role",
        Command={
            "Name": "glueetl",
            "ScriptLocation": script_location,
            "PythonVersion": "3",
        },
        DefaultArguments=default_args,
        Timeout=15,
        GlueVersion="2.0",
        NumberOfWorkers=10,
        WorkerType="G.2X",
    )
    return response


def create_masking_job(config, region=None):
    """
    Creates the Data Masking job on aws glue
    :param config:
    :param region:
    :return:
    """
    region = config["primary_region"] if region is None else region
    fm_prefix = config["fm_prefix"]
    client = boto3.client("glue", region_name=region)
    job_name = f"{fm_prefix}-data-masking"
    client.delete_job(JobName=job_name)
    script_location = f"s3://{fm_prefix}-code-{region}/aws-datalake-framework/src/genericDataMasking.py"
    default_args = {
        "--extra-py-files": f"s3://{fm_prefix}-code-{region}/aws-datalake-framework/dependencies/pydeequ.zip,s3://{fm_prefix}-code-{region}/aws-datalake-framework/dependencies/utils.zip",
        "--extra-files": f"s3://{fm_prefix}-code-{region}/aws-datalake-framework/config/globalConfig.json",
        "--extra-jars": f"s3://{fm_prefix}-code-{region}/aws-datalake-framework/dependencies/deequ-1.0.3.jar",
        "--additional-python-modules": "Crypto,packaging,rfc3339,cape-privacy[spark]",
        "--TempDir": f"s3://{fm_prefix}-code-{region}/temporary/",
    }
    response = client.create_job(
        Name=job_name,
        Description="Data Masking Job",
        Role="2482-misc-service-role",
        Command={
            "Name": "glueetl",
            "ScriptLocation": script_location,
            "PythonVersion": "3",
        },
        DefaultArguments=default_args,
        Timeout=15,
        GlueVersion="3.0",
        NumberOfWorkers=10,
        WorkerType="G.2X",
    )
    return response


def create_standardization_job(config, region=None):
    """
    Creates the data standardization job on aws glue
    :param config:
    :param region:
    :return:
    """
    region = config["primary_region"] if region is None else region
    fm_prefix = config["fm_prefix"]
    client = boto3.client("glue", region_name=region)
    job_name = f"{fm_prefix}-data-standardization"
    client.delete_job(JobName=job_name)
    script_location = f"s3://{fm_prefix}-code-{region}/aws-datalake-framework/src/genericDataStandardization.py"
    default_args = {
        "--extra-py-files": f"s3://{fm_prefix}-code-{region}/aws-datalake-framework/dependencies/pydeequ.zip,s3://{fm_prefix}-code-{region}/aws-datalake-framework/dependencies/utils.zip",
        "--extra-files": f"s3://{fm_prefix}-code-{region}/aws-datalake-framework/config/globalConfig.json",
        "--extra-jars": f"s3://{fm_prefix}-code-{region}/aws-datalake-framework/dependencies/deequ-1.0.3.jar",
        "--TempDir": f"s3://{fm_prefix}-code-{region}/temporary/",
    }
    response = client.create_job(
        Name=job_name,
        Description="Data Standardization Job",
        Role="2482-misc-service-role",
        Command={
            "Name": "glueetl",
            "ScriptLocation": script_location,
            "PythonVersion": "3",
        },
        DefaultArguments=default_args,
        Timeout=15,
        GlueVersion="2.0",
        NumberOfWorkers=10,
        WorkerType="G.2X",
    )
    return response


def create_glue_jobs(config, region=None):
    """
    Main entry method
    :param config:
    :param region:
    :return:
    """
    try:
        create_dq_job(config, region)
        create_masking_job(config, region)
        create_standardization_job(config, region)
        return True
    except Exception as e:
        print(e)
        return False
