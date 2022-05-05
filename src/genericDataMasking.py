import sys
import time
from awsglue.utils import getResolvedOptions
from utils.data_asset import DataAsset
from utils.comUtils import *
from utils.capeprivacyUtils import *
from utils.pg_connect import Connector


def get_global_config():
    """
    Utility method to get global config file
    :return:JSON
    """
    config_file_path = "globalConfig.json"
    file = open(file=config_file_path, mode="r")
    config = json.load(file)
    file.close()
    return config


# Get the arguments
args = getResolvedOptions(sys.argv, ["source_path", "source_id", "asset_id", "exec_id"])
global_config = get_global_config()
# Record the start time of the job
start_time = time.time()
region = boto3.session.Session().region_name
# Create connection object
conn = Connector("postgres_dev", region)
# Create object to store data asset info
asset = DataAsset(args, global_config, run_identifier="data-masking", conn=conn)
# Creating spark Session object
spark = get_spark_for_masking(asset.logger)
try:
    # Creating spark dataframe from input file.Supported:CSV,Parquet,JSON,ORC
    source_df = create_spark_df(
        spark,
        asset.source_file_path,
        asset.asset_file_type,
        asset.asset_file_delim,
        asset.asset_file_header,
        asset.logger,
    )
    # Update the data catalog dynamoDB table to "In-Progress" for easy monitoring
    asset.update_data_catalog(conn, data_masking="In-Progress")
    # Getting data asset table dedicated for a specific asset which specifies if masking is required or not
    metadata = asset.get_asset_metadata(conn)
    # Getting the masking key from AWS Secrets Manager
    key = get_secret(asset.secret_name, asset.region, asset.logger)
    # Function to mask data of sensitive columns
    result = run_data_masking(source_df, metadata, key, asset.logger)
    # Gets the target path where the masked data needs to be stored after masking
    target_path = asset.get_masking_path()
    target_path = target_path + get_timestamp(asset.source_file_path) + "/"
    # Storing the masked data in the target path
    store_sparkdf_to_s3(
        result,
        target_path,
        asset.asset_file_type,
        asset.asset_file_delim,
        asset.asset_file_header,
        asset.logger,
    )
    # Update data catalog table to "Completed" if the masking is successful
    asset.update_data_catalog(conn, data_masking="Completed")
except Exception as e:
    # Update data catalog table to "Failed" in case of exceptions
    asset.update_data_catalog(conn, data_masking="Failed")
    asset.logger.write(message=str(e))

end_time = time.time()
asset.logger.write(message=f"Time Taken = {round(end_time - start_time, 2)} seconds")
# Write logs to S3
asset.logger.write_logs_to_s3()
# Close connection
conn.close()
# Stop the spark session
stop_spark(spark)
