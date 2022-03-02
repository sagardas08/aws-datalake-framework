import pyspark
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from .logger import log


@log
def run_data_standardization(source_df, metadata, logger=None):
    for i in metadata:
        col_name = i.get("col_nm")
        target_col_name = i.get("tgt_col_nm")
        source_df = source_df.withColumnRenamed(col_name, target_col_name)
    logger.write(message="Data standardization done successfully")
    return source_df
