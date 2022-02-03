from boto3.dynamodb.conditions import Key

from utils.comUtils import *
from utils.dqUtils import *
from utils.validateSchema import validate_schema
from utils.data_asset import DataAsset


def get_global_config():
    config_file_path = "config/globalConfig.json"
    file = open(file=config_file_path, mode="r")
    config = json.load(file)
    file.close()
    return config


# Get the arguments
args = {'source_path': 's3://dl-fmwrk-10000-us-east-1/11002/init/test_folder/university_rankings (1).csv',
        'source_id': '10000',
        'asset_id': '11002',
        'exec_id': '10000_11002_20220203043407'}
global_config = get_global_config()
start_time = time.time()
asset = DataAsset(args, global_config)
spark = get_spark(asset.logger)
source_df = create_spark_df(spark, asset.source_file_path, asset.asset_file_type,
                            asset.asset_file_delim, asset.asset_file_header, asset.logger)
if asset.validate_schema(source_df):
    asset.update_data_catalog(dq_validation='In-Progress')
    dq_code = asset.generate_dq_code()
    check = Check(spark, CheckLevel.Warning, "Deequ Data Quality Checks")
    checkOutput = None
    asset.logger.write(message="Executing the DQ code")
    exec(dq_code, globals())
    result = VerificationResult.checkResultsAsDataFrame(spark, checkOutput)
    result_s3_path = asset.get_results_path()
    result.repartition(1).write.csv(result_s3_path, header=True, mode='overwrite')
    asset.update_data_catalog(dq_validation='Completed')
    move_source_file(path=asset.source_path, dq_result=result, logger=asset.logger)
else:
    asset.logger.write(message="Found schema irregularities")
    move_source_file(path=asset.source_path, schema_validation=False, logger=asset.logger)
# Code ends here: Write the logs to an Output location.
stop_spark(spark)
end_time = time.time()
asset.logger.write(message=f"Time Taken = {round(end_time - start_time, 2)} seconds")
asset.logger.write_logs_to_s3()
