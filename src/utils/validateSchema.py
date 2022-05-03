# imports
import boto3

from .logger import log
from connector import Connector


def get_schema_details(table, asset_id, region):
    """
    Get the details of schema from DynamoDB
    """
    # TODO: DynamoDB -> RDS: Retrieve Data
    db.retrieve_dict(rds_table, cols='*', where=('asset_id=%s', [asset_id]))
    return response


def order_columns(items):
    """
    Orders the columns according to their col_id
    :param items: iterable object containing col_id and col_name
    :return: list of columns
    """
    schema_dict = dict()
    for item in items:
        col_id = item["col_id"]["N"]
        col_name = item["col_nm"]["S"]
        schema_dict[col_name] = int(col_id)
    # sorting on the basis of numeric col_id
    columns = sorted(schema_dict, key=schema_dict.get)
    return columns


def match_in_order(actual, expected):
    """
    Method to match the order of source columns and the order in the asset info table
    :param actual: actual order present in source file
    :param expected: the expected order according to the asset_info table
    :return: Bool, list
    """
    zipped_cols = zip(actual, expected)
    difference = dict()
    match = True
    for item in zipped_cols:
        if len(item[0]) != len(item[1]) or item[0] != item[1]:
            difference[item[0]] = item[1]
            match = False
    return match, difference


def match_without_order(actual, expected):
    """
    method to match 2 lists irrespective of their order
    """
    matching = [i for i in actual if i in expected]
    if len(matching) == len(actual) == len(expected):
        return True
    else:
        return False


def match_length(actual, expected):
    """
    method to match the length of 2 lists
    """
    if len(actual) == len(expected):
        return True
    return False


@log
def validate_schema(
    asset_file_type,
    asset_file_header,
    df,
    metadata_table,
    region,
    asset_id,
    logger=None,
):
    """8
    Target Function to enforce schema validation
    :return:
    """
    # get the list of cols
    df_cols = df.columns
    # get the details of the schema from the metadata
    schema_details = get_schema_details(metadata_table, asset_id, region)
    # order the columns as per their col_id
    columns = order_columns(schema_details)
    actual_cols = [i.lower() for i in df_cols]
    expected_cols = [i.lower() for i in columns]
    result = None
    # If the length of the 2 lists : actual and expected do not match from the start
    if not match_length(actual_cols, expected_cols):
        result = False
    else:
        # For json and parquet files: match the column names of the actual and expected cols
        if asset_file_type == "json" or asset_file_type == "parquet":
            result = match_without_order(actual_cols, expected_cols)
        # For CSV files with file header: Match the column name and column order
        elif asset_file_type == "csv" and asset_file_header:
            result, diff = match_in_order(actual_cols, expected_cols)
            if len(diff) > 0:
                print("The following columns are not matching: ")
                for k, v in diff.items():
                    print(f"Source Col Name: {k} --- Expected: {v}")
    return result
