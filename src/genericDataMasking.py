import sys
import json
from boto3.dynamodb.conditions import Key
from awsglue.utils import getResolvedOptions
from utils.comUtils import *
from utils.capeprivacyUtils import *
from .logger import log
from datetime import datetime, timedelta
import time


spark = get_spark_for_masking(logger)
args = getResolvedOptions(sys.argv, ["source_path", "source_id", "asset_id","exec_id"])
source_path = args["source_path"]
source_id = args["source_id"]
asset_id = args["asset_id"]
exec_id = args["exec_id"]
with open('globalConfig.json', 'r') as config_file:
    config = json.load(config_file)
    config_file.close()

fm_prefix = config["fm_prefix"]
region = config["primary_region"]
secret_name = config["secret_name"]
log_type = config["log_type"]
logger = Logger(
    log_type=log_type,
    log_name=exec_id,
    src_path=source_path,
    asset_id=asset_id,
    region=region,
)
start_time = time.time()
logger.write(message=f"Start Time : {get_current_time()}")
dynamodb = boto3.resource("dynamodb", region_name=region)
asset_info_table = f"{fm_prefix}.data_asset"
asset_info = dynamodb.Table(asset_info_table)
asset_info_items = asset_info.query(
    KeyConditionExpression=Key("asset_id").eq(int(asset_id))
)
items = dynamodbJsonToDict(asset_info_items)
asset_file_type = items["file_type"]
asset_file_delim = items["file_delim"]
asset_file_header = items["file_header"]
metadata_table = f"{fm_prefix}.data_asset.{asset_id}"
source_file_path = source_path.replace("s3://", "s3a://")
spark = get_spark_for_masking(logger)
source_df = create_spark_df(
    spark, source_file_path, asset_file_type, asset_file_delim, asset_file_header,logger
)
try:
    assert source_df != None
    print("The dataframe has been created")
except AssertionError:
    print("Unable to read the data from the source")
    sys.exit()
metadata = get_metadata(metadata_table, region,logger)
key = get_secret(secret_name,region,logger)
result = run_data_masking(source_df,metadata,key,logger)
try:
    assert result != None
    list1 = source_path.split("/")
    target_path = list1[0]+"//"+list1[2]+"/"+list1[3]+"/masked/"
    store_sparkdf_to_s3(result,target_path,asset_file_type,asset_file_delim,asset_file_header,logger)
except AssertionError:
    print("Encountered error while running data masking")
    move_file(source_file_path,logger)
end_time = time.time()
logger.write(message=f"End Time : {get_current_time()}")
logger.write(message=f"Time Taken = {round(end_time - start_time, 2)} seconds")
logger.write_logs_to_s3()
stop_spark(spark)
