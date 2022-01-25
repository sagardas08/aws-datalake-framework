import sys
from boto3.dynamodb.conditions import Key
from awsglue.utils import getResolvedOptions
from utils.comUtils import *
from utils.capeprivacyUtils import *



spark = get_spark_for_masking()

"""
Capturing source information parameters from the step function. Parameters:
  1. source_path: S3 path where the source file is created/landed
  2. source_id: Source system identifier which is a random number associated
    to the onboarded source. This identifier is also suffixed in the S3 
    bucket name.
  3. asset_id: Asset Identifier which is a random number associated to the 
    onboadred data asset.
"""
args = getResolvedOptions(sys.argv, ["source_path", "source_id", "asset_id"])
source_path = args["source_path"]
source_id = args["source_id"]
asset_id = args["asset_id"]

"""
Pulling the data asset information from dynamoDB table dl_fmwrk.data_asset 
based on the Asset ID
"""
region = "us-east-2"
dynamodb = boto3.resource("dynamodb", region_name=region)
asset_info = dynamodb.Table("dl_fmwrk.data_asset")
asset_info_items = asset_info.query(
    KeyConditionExpression=Key("asset_id").eq(int(asset_id))
)
items = dynamodbJsonToDict(asset_info_items)
asset_file_type = items["file_type"]
asset_file_delim = items["file_delim"]
asset_file_header = items["file_header"]
metadata_table = f"dl_fmwrk.data_asset.{asset_id}"
"""
Create dataframe using the source data asset
"""
source_file_path = source_path.replace("s3://", "s3a://")
source_df = create_spark_df(
    spark, source_file_path, asset_file_type, asset_file_delim, asset_file_header
)
source_df.printSchema()
metadata = get_metadata(metadata_table, region)
result=run_data_masking(spark,source_df,metadata)
result.show()
stop_spark(spark)
