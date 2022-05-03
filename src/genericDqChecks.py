import sys

from awsglue.utils import getResolvedOptions

from utils.comUtils import *
from utils.dqUtils import *
from utils.data_asset import DataAsset
from connector.pg_connect import Connector


def get_global_config():
    """
    Get the globally defined config from the config file
    :return: JSON object
    """
    config_file_path = "globalConfig.json"
    file = open(file=config_file_path, mode="r")
    config = json.load(file)
    file.close()
    return config


def stop():
    """
    Method to stop the sparkSession and write logs to S3
    :return: None
    """
    stop_spark(spark)
    end_time = time.time()
    asset.logger.write(
        message=f"Time Taken = {round(end_time - start_time, 2)} seconds"
    )
    asset.logger.write_logs_to_s3()


# Record the start time of the job
start_time = time.time()
# Get the arguments
args = getResolvedOptions(sys.argv, ["source_path", "source_id", "asset_id", "exec_id"])
global_config = get_global_config()
region = boto3.session.Session().region_name
conn = Connector("postgres_dev", region)
# Creating an object to house imp info about the asset in one place
asset = DataAsset(args, global_config, run_identifier="data-masking", conn=conn)
# Creation of source dataframe using spark and asset properties
spark = get_spark(asset.logger)
source_df = create_spark_df(
    spark,
    asset.source_file_path,
    asset.asset_file_type,
    asset.asset_file_delim,
    asset.asset_file_header,
    asset.logger,
)
# validating the schema of the asset using the asset_info table
# if the schema is validated, continue with the DQ else stop the process
if asset.validate_schema(source_df):
    try:
        # update the data catalog that DQ is in progress
        asset.update_data_catalog(conn, dq_validation="In-Progress")
        # dynamically generate the DQ code using asset properties
        dq_code = asset.generate_dq_code(conn)
        # create a pydeequ check object, required while executing the DQ code
        check = Check(spark, CheckLevel.Warning, "Deequ Data Quality Checks")
        # declaring the variable that stores the DQ results
        checkOutput = None
        asset.logger.write(message="Executing the DQ code")
        # using the inbuilt exec method to execute the dynamic DQ code
        exec(dq_code, globals())
        # DQ results as a spark DF
        result = VerificationResult.checkResultsAsDataFrame(spark, checkOutput)
        # Writing the results to S3 log zone. Subject to change
        result_s3_path = asset.get_results_path()
        result.repartition(1).write.csv(result_s3_path, header=True, mode="overwrite")
        asset.update_data_catalog(conn, dq_validation="Completed")
        # check if the source file needs to be moved in case of failures
        move_source_file(
            path=asset.source_path,
            dq_result=result,
            logger=asset.logger,
        )
    except Exception as e:
        # In case of an exception update the status to failed
        asset.logger.write(message=str(e))
        asset.update_data_catalog(conn, dq_validation="Failed")
        asset.logger.write_logs_to_s3()
    stop()
else:
    # In case of invalid schema update the status to failed
    asset.update_data_catalog(conn, dq_validation="Failed")
    asset.logger.write(message="Found schema irregularities")
    move_source_file(
        path=asset.source_path,
        schema_validation=False,
        logger=asset.logger,
    )
    stop()
    raise Exception("Halting the execution due to schema irregularities in the dataset")