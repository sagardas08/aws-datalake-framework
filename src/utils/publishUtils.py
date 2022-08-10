import pyspark
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from data_type_mapper import *
from .logger import log


@log
def run_data_publish(source_df, metadata, logger=None):
    """
    :param source_df: The spark dataframe
    :param metadata: The metadata
    :param logger:
    :return:
    """
    for i in metadata:
        # Gets the source column name from the metadata
        col_name = i.get("col_nm")
        # Gets the target column name from the metadata
        target_col_name = i.get("tgt_col_nm")
        # Gets the ui data type from the metadata
        ui_data_type = i.get("data_type")
        # Gets the target data type from the metadata
        target_ui_data_type = i.get("tgt_data_type")
        # Checking the ui data type and target data type for Datetime datatype
        if ui_data_type == "Datetime" and target_ui_data_type == "Datetime":
            datetime_format = i.get("datetime_format")
            tgt_datetime_format = i.get("tgt_datetime_format")
            # Converting to Timestamp value
            source_df = source_df.withColumn(col_name, to_timestamp(col_name, datetime_format))
            # Converting the datetime format and renaming with target column name
            source_df = source_df.withColumn(col_name, date_format(col_name, tgt_datetime_format)).withColumnRenamed(col_name, target_col_name)
        else:
            # Creating spark datatypes
            spark_data_type = UI_SPARK_MAP[target_ui_data_type]
            # Casting to spark datatypes and renaming with target column name
            source_df = source_df.withColumn(col_name, col(col_name).cast(spark_data_type)).withColumnRenamed(col_name, target_col_name)
    logger.write(message="Data Publish done successfully")
    # Returns spark dataframe
    return source_df


@log
def get_or_create_rs_schema(conn, schema, logger=None):
    # TODO: check if rs_schema exists or not before creating the schema
    conn.create_schema(schema)


@log
def get_or_create_rs_table(conn, df, db_schema, table_name, pk_column=None, logger=None):
    """
    :param conn:
    :param df:
    :param db_schema:
    :param table_name:
    :param pk_column:
    :param logger:
    :return:
    """
    # TODO: check if rs_table exists or not before creating the table
    conn.create_table(df, db_schema, table_name, pk_column=pk_column)
