import boto3


def create_dq_job(config, region=None):
    """
    Creates DQ job on glue
    """
    region = config["primary_region"] if region is None else region
    fm_prefix = config["fm_prefix"]
    project = config["project_name"]
    client = boto3.client("glue", region_name=region)
    job_name = f"{fm_prefix}-data-quality-checks"
    # delete the previous glue job
    client.delete_job(JobName=job_name)
    code_bucket = f"s3://{fm_prefix}-code-{region}"
    # location of the glue job in the code bucket
    script_location = f"{code_bucket}/{project}/src/genericDqChecks.py"
    default_args = {
        "--extra-py-files": f"{code_bucket}/{project}/dependencies/pydeequ.zip,{code_bucket}/{project}/dependencies/utils.zip,{code_bucket}/{project}/dependencies/connector.zip",
        "--extra-files": f"{code_bucket}/{project}/config/globalConfig.json",
        "--extra-jars": f"{code_bucket}/{project}/dependencies/deequ-1.0.3.jar",
        "--TempDir": f"{code_bucket}/temporary/",
        "--additional-python-modules": "psycopg2-binary"
    }
    # create the new glue job
    response = client.create_job(
        Name=job_name,
        Description="Data Quality Job",
        Role="2189_misc_roles",
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
    """
    region = config["primary_region"] if region is None else region
    fm_prefix = config["fm_prefix"]
    project = config["project_name"]
    client = boto3.client("glue", region_name=region)
    job_name = f"{fm_prefix}-data-masking"
    # delete the previously existing glue job
    client.delete_job(JobName=job_name)
    code_bucket = f"s3://{fm_prefix}-code-{region}"
    # location of the script in the code bucket
    script_location = f"{code_bucket}/{project}/src/genericDataMasking.py"
    default_args = {
        "--extra-py-files": f"{code_bucket}/{project}/dependencies/pydeequ.zip,{code_bucket}/{project}/dependencies/utils.zip,{code_bucket}/{project}/dependencies/connector.zip",
        "--extra-files": f"{code_bucket}/{project}/config/globalConfig.json",
        "--extra-jars": f"{code_bucket}/{project}/dependencies/deequ-1.0.3.jar",
        "--TempDir": f"{code_bucket}/temporary/",
        "--additional-python-modules": "Crypto,packaging,rfc3339,cape-privacy[spark],psycopg2-binary",
    }
    # create a new job
    response = client.create_job(
        Name=job_name,
        Description="Data Masking Job",
        Role="2189_misc_roles",
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


def create_publish_job(config, region=None):
    """
    Creates the data publish job on aws glue
    """
    region = config["primary_region"] if region is None else region
    fm_prefix = config["fm_prefix"]
    project = config["project_name"]
    client = boto3.client("glue", region_name=region)
    job_name = f"{fm_prefix}-data-publish"
    # delete the previously existing glue job
    client.delete_job(JobName=job_name)
    code_bucket = f"s3://{fm_prefix}-code-{region}"
    # location of the data publish script in the code bucket
    script_location = f"{code_bucket}/{project}/src/genericDataPublish.py"
    default_args = {
        "--extra-py-files": f"{code_bucket}/{project}/dependencies/pydeequ.zip,{code_bucket}/{project}/dependencies/utils.zip,{code_bucket}/{project}/dependencies/connector.zip",
        "--extra-files": f"{code_bucket}/{project}/config/globalConfig.json",
        "--extra-jars": f"{code_bucket}/{project}/dependencies/deequ-1.0.3.jar",
        "--TempDir": f"{code_bucket}/temporary/",
        "--additional-python-modules": "psycopg2-binary"
    }
    # create a new glue job
    response = client.create_job(
        Name=job_name,
        Description="Data Publish Job",
        Role="2189_misc_roles",
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


def create_glue_jobs(config, region=None):
    """
    Main entry method to create multiple glue jobs
    """
    try:
        create_dq_job(config, region)
        create_masking_job(config, region)
        create_publish_job(config, region)
        return True
    except Exception as e:
        print(e)
        return False
