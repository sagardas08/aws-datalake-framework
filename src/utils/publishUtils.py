import pyspark
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
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
        col_name = i.get("col_nm")
        # Gets the target column name from the metadata
        target_col_name = i.get("tgt_col_nm")
        # Replaces the name of the columns with user required name
        source_df = source_df.withColumnRenamed(col_name, target_col_name)
    logger.write(message="Data publish done successfully")
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
