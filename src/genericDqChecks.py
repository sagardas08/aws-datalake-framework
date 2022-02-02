import sys
from datetime import datetime, timedelta
import time

import json
from boto3.dynamodb.conditions import Key
from awsglue.utils import getResolvedOptions

from utils.comUtils import *
from utils.dqUtils import *
from utils.validateSchema import validate_schema
from utils.logger import Logger


def get_global_config():
    config_file_path = "globalConfig.json"
    file = open(file=config_file_path, mode="r")
    config = json.load(file)
    file.close()
    return config


args = getResolvedOptions(sys.argv, ["source_path", "source_id", "asset_id", "exec_id"])
source_path = args["source_path"]
source_id = args["source_id"]
asset_id = args["asset_id"]
exec_id = args["exec_id"]
global_config = get_global_config()
fm_prefix = global_config["fm_prefix"]
metadata_table = f"{fm_prefix}.{asset_id}"
region = global_config["primary_region"]
log_type = global_config["log_type"]
# Initialize logger
logger = Logger(
    log_type=log_type,
    log_name=exec_id,
    src_path=source_path,
    asset_id=asset_id,
    region=region,
)
start_time = time.time()
logger.write(message=f"Start Time : {get_current_time()}")
asset_metadata_table = f"{fm_prefix}.data_asset.{asset_id}"
dynamodb = boto3.resource("dynamodb", region_name=region)
asset_info = dynamodb.Table(f"{fm_prefix}.data_asset")
asset_info_items = asset_info.query(
    KeyConditionExpression=Key("asset_id").eq(int(asset_id))
)
items = dynamodbJsonToDict(asset_info_items)
asset_file_type = items["file_type"]
asset_file_delim = items["file_delim"]
asset_file_header = items["file_header"]
# Create dataframe using the source data asset
spark = get_spark(logger)
source_file_path = source_path.replace("s3://", "s3a://")
source_df = create_spark_df(
    spark,
    source_file_path,
    asset_file_type,
    asset_file_delim,
    asset_file_header,
    logger,
)
schema_validation = validate_schema(
    asset_file_type, asset_file_header, source_df, asset_metadata_table, logger=logger
)

# Proceed to DQ checks if schema is validated
if schema_validation:
    logger.write(message="Source Schema matches the expected schema")
    metadata = get_metadata(asset_metadata_table, region, logger=logger)
    dq_code = generate_code(metadata, logger=logger)
    check = Check(spark, CheckLevel.Warning, "Deequ Data Quality Checks")
    checkOutput = None
    logger.write(message="Executing the DQ code")
    exec(dq_code, globals())
    result = VerificationResult.checkResultsAsDataFrame(spark, checkOutput)
    result.show()
    move_source_file(source_file_path=source_path, dq_result=result, logger=logger)
else:
    logger.write(message="Found schema irregularities")
    move_source_file(source_file_path=source_path, schema_validation=False, logger=logger)
# Code ends here -> Write the logs to an Output location.
stop_spark(spark)
end_time = time.time()
logger.write(message=f"End Time : {get_current_time()}")
logger.write(message=f"Time Taken = {round(end_time - start_time, 2)} seconds")
logger.write_logs_to_s3()
